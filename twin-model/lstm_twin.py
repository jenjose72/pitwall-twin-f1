"""
PyTorch LSTM-based digital twin.
Reproduces Hundman et al. (KDD 2018) telemanom approach.
Predicts next telemetry values from prior history.
"""

import torch
import torch.nn as nn
from torch.utils.data import DataLoader, TensorDataset
import numpy as np
from pathlib import Path
from typing import Tuple, List
import json


class LSTMTelemetryTwin(nn.Module):
    """Shared LSTM-based telemetry predictor with car-ID embedding.

    v2: Single model predicts all channels for all cars, using learned car-ID embedding.
    This replaces per-channel models (would be 5 channels × 20 cars = 100 separate models).
    """

    def __init__(
        self,
        input_size: int,
        num_cars: int = 20,
        car_embedding_dim: int = 16,
        hidden_size: int = 128,
        num_layers: int = 2,
        dropout: float = 0.2,
        output_channels: int = 5,  # speed, RPM, throttle, brake, gear
    ):
        super().__init__()
        self.input_size = input_size
        self.num_cars = num_cars
        self.car_embedding_dim = car_embedding_dim
        self.hidden_size = hidden_size
        self.num_layers = num_layers
        self.output_channels = output_channels

        # Learn embeddings for each car
        self.car_embedding = nn.Embedding(num_cars, car_embedding_dim)

        # LSTM takes telemetry + car embedding
        lstm_input_size = input_size + car_embedding_dim
        self.lstm = nn.LSTM(
            lstm_input_size,
            hidden_size,
            num_layers,
            dropout=dropout if num_layers > 1 else 0,
            batch_first=True,
        )

        # Predict all output channels at once
        self.fc = nn.Sequential(
            nn.Linear(hidden_size, 128),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(128, 64),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(64, output_channels),
        )

    def forward(self, x: torch.Tensor, car_ids: torch.Tensor = None) -> torch.Tensor:
        """
        Args:
            x: (batch_size, seq_len, input_size) historical telemetry
            car_ids: (batch_size,) car indices, optional (default: 0)
        Returns:
            (batch_size, output_channels) predicted next values for all channels
        """
        batch_size = x.shape[0]
        seq_len = x.shape[1]

        if car_ids is None:
            car_ids = torch.zeros(batch_size, dtype=torch.long, device=x.device)

        # Get car embeddings
        car_emb = self.car_embedding(car_ids)  # (batch_size, car_embedding_dim)
        car_emb = car_emb.unsqueeze(1).expand(-1, seq_len, -1)  # (batch_size, seq_len, car_embedding_dim)

        # Concatenate telemetry with car embedding
        x_with_embedding = torch.cat([x, car_emb], dim=-1)  # (batch_size, seq_len, input_size + car_embedding_dim)

        # Run through LSTM
        lstm_out, _ = self.lstm(x_with_embedding)
        last_hidden = lstm_out[:, -1, :]

        # Predict all channels
        output = self.fc(last_hidden)  # (batch_size, output_channels)
        return output


class TelemetryTwin:
    """Digital twin service for F1 telemetry anomaly detection (v2: shared model).

    v2 uses a single LSTM with car-ID embedding instead of per-channel models.
    This predicts all 5 channels (speed, RPM, throttle, brake, gear) for any car.
    """

    def __init__(
        self,
        channels: List[str] = None,
        num_cars: int = 20,
        seq_len: int = 30,
        hidden_size: int = 128,
        learning_rate: float = 0.001,
        device: str = "cpu",
    ):
        self.channels = channels or ["speed", "rpm", "throttle", "brake", "gear"]
        self.num_cars = num_cars
        self.seq_len = seq_len
        self.hidden_size = hidden_size
        self.learning_rate = learning_rate
        self.device = torch.device(device)
        self.channel_to_idx = {ch: i for i, ch in enumerate(self.channels)}

        # Single shared LSTM for all cars and all channels
        self.model = LSTMTelemetryTwin(
            input_size=len(self.channels),
            num_cars=num_cars,
            hidden_size=hidden_size,
            output_channels=len(self.channels),
        ).to(self.device)

        self.optimizer = torch.optim.Adam(self.model.parameters(), lr=learning_rate)
        self.criterion = nn.MSELoss()
        self.scaler_stats = {channel: {"mean": 0, "std": 1} for channel in self.channels}

    def normalize(self, values: np.ndarray, channel: str) -> np.ndarray:
        """Normalize values using stored statistics."""
        mean = self.scaler_stats[channel]["mean"]
        std = self.scaler_stats[channel]["std"]
        return (values - mean) / (std + 1e-8)

    def denormalize(self, values: np.ndarray, channel: str) -> np.ndarray:
        """Denormalize values."""
        mean = self.scaler_stats[channel]["mean"]
        std = self.scaler_stats[channel]["std"]
        return values * std + mean

    def fit(self, train_data: dict, car_id: int = 0, epochs: int = 10, batch_size: int = 32):
        """
        Train shared LSTM model on historical data (all channels, one car).
        train_data: {channel: [values]}
        car_id: which car this data is from (0-19)
        """
        # Update scaler stats for all channels
        for channel in self.channels:
            if channel not in train_data:
                continue
            values = np.array(train_data[channel], dtype=np.float32)
            values = values[~np.isnan(values)]
            self.scaler_stats[channel]["mean"] = float(np.mean(values))
            self.scaler_stats[channel]["std"] = float(np.std(values))

        # Normalize all channels
        norm_data = {}
        for channel in self.channels:
            if channel in train_data:
                values = np.array(train_data[channel], dtype=np.float32)
                values = values[~np.isnan(values)]
                norm_data[channel] = self.normalize(values, channel)
            else:
                norm_data[channel] = np.array([])

        # Verify all channels have same length
        min_len = min(len(v) for v in norm_data.values() if len(v) > 0)
        if min_len < self.seq_len + 1:
            print(f"  Not enough data: {min_len} < {self.seq_len + 1}")
            return

        # Create multi-channel sequences
        X, y, car_ids = [], [], []
        for i in range(min_len - self.seq_len):
            # Sequence: (seq_len, num_channels)
            seq = np.stack([norm_data[ch][i : i + self.seq_len] for ch in self.channels], axis=-1)
            # Target: (num_channels,)
            tgt = np.array([norm_data[ch][i + self.seq_len] for ch in self.channels])
            X.append(seq)
            y.append(tgt)
            car_ids.append(car_id)

        X = torch.FloatTensor(np.array(X)).to(self.device)  # (N, seq_len, num_channels)
        y = torch.FloatTensor(np.array(y)).to(self.device)  # (N, num_channels)
        car_ids_t = torch.LongTensor(np.array(car_ids)).to(self.device)

        dataset = TensorDataset(X, y, car_ids_t)
        loader = DataLoader(dataset, batch_size=batch_size, shuffle=True)

        self.model.train()

        for epoch in range(epochs):
            total_loss = 0
            for batch_X, batch_y, batch_car_ids in loader:
                self.optimizer.zero_grad()
                pred = self.model(batch_X, batch_car_ids)
                loss = self.criterion(pred, batch_y)
                loss.backward()
                self.optimizer.step()
                total_loss += loss.item()

            if (epoch + 1) % max(1, epochs // 3) == 0:
                print(f"  epoch {epoch+1}/{epochs}: loss={total_loss/len(loader):.6f}")

    def predict(self, history: dict, car_id: int = 0) -> dict:
        """
        Predict next telemetry values using shared LSTM.
        history: {channel: [recent_values]}
        car_id: which car (0-19)
        Returns: {channel: predicted_value}
        """
        predictions = {}

        # Check if we have enough history for all channels
        if not all(channel in history and len(history[channel]) >= self.seq_len for channel in self.channels):
            return {ch: None for ch in self.channels}

        # Build multi-channel sequence
        try:
            seq_data = []
            for channel in self.channels:
                values = np.array(history[channel][-self.seq_len :], dtype=np.float32)
                values = values[~np.isnan(values)]
                if len(values) < self.seq_len:
                    return {ch: None for ch in self.channels}
                norm_values = self.normalize(values, channel)
                seq_data.append(norm_values)

            # Shape: (seq_len, num_channels)
            X = np.stack(seq_data, axis=-1)
            X = torch.FloatTensor(X).unsqueeze(0).to(self.device)  # (1, seq_len, num_channels)

            car_ids = torch.LongTensor([car_id]).to(self.device)

            self.model.eval()
            with torch.no_grad():
                pred_norm = self.model(X, car_ids).cpu().numpy()[0]  # (num_channels,)

            # Denormalize each channel
            for i, channel in enumerate(self.channels):
                predictions[channel] = float(self.denormalize(pred_norm[i], channel))

            return predictions

        except Exception as e:
            print(f"Prediction error: {e}")
            return {ch: None for ch in self.channels}

    def get_residuals(self, actual: dict, predicted: dict) -> dict:
        """Compute prediction residuals (actual - predicted)."""
        residuals = {}
        for channel in self.channels:
            if (
                channel in actual
                and channel in predicted
                and actual[channel] is not None
                and predicted[channel] is not None
            ):
                residuals[channel] = float(actual[channel] - predicted[channel])
            else:
                residuals[channel] = None
        return residuals

    def save(self, path: Path):
        """Save shared model and scaler stats."""
        path = Path(path)
        path.mkdir(exist_ok=True)

        # Save shared model
        torch.save(self.model.state_dict(), path / "shared_lstm_model.pt")

        # Save scaler stats
        with open(path / "scaler_stats.json", "w") as f:
            json.dump(self.scaler_stats, f)

    def load(self, path: Path):
        """Load shared model and scaler stats."""
        path = Path(path)

        # Load shared model
        state_dict = torch.load(path / "shared_lstm_model.pt", map_location=self.device)
        self.model.load_state_dict(state_dict)
        self.model.to(self.device)

        # Load scaler stats
        with open(path / "scaler_stats.json", "r") as f:
            self.scaler_stats = json.load(f)


# Example: train on sample data
if __name__ == "__main__":
    twin = TelemetryTwin(device="cpu")

    # Dummy training data
    np.random.seed(42)
    train_data = {
        "speed": np.sin(np.linspace(0, 20, 500)) * 100 + 200 + np.random.normal(0, 10, 500),
        "rpm": np.random.normal(10000, 1000, 500),
        "throttle": np.random.uniform(0, 1, 500),
        "brake": np.random.uniform(0, 1, 500),
        "gear": np.random.randint(1, 8, 500),
    }

    print("Training digital twin on sample data...")
    twin.fit(train_data, epochs=5)

    # Test prediction
    history = {channel: list(train_data[channel][-30:]) for channel in twin.channels}
    predictions = twin.predict(history)
    print(f"\nPredictions: {predictions}")

    # Test residuals
    actual = {channel: train_data[channel][-1] for channel in twin.channels}
    residuals = twin.get_residuals(actual, predictions)
    print(f"Residuals: {residuals}")
