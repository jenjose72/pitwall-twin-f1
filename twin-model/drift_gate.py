"""
Drift detection and gating layer.
Uses River's ADWIN and DDM for detecting data drift in residuals.
Decides when to adapt the digital twin.
"""

from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import json
from pathlib import Path


@dataclass
class DriftEvent:
    """Represents a detected drift event."""
    timestamp: float
    channel: str
    detector: str  # "ADWIN" or "DDM"
    severity: float  # 0-1
    description: str


class DriftGate:
    """
    Drift detection and adaptation gating (v2: ADWIN + Page-Hinkley).
    - ADWIN: detects general change in residual distribution
    - Page-Hinkley: fast reacting to sustained shifts
    - Confirmation: drift confirmed only when both detect change
    - Freeze: blocks retraining during active anomalies
    """

    def __init__(
        self,
        channels: List[str],
        adwin_threshold: float = 0.05,
        page_hinkley_threshold: float = 5.0,
        consecutive_drifts: int = 5,
        min_samples: int = 100,
    ):
        self.channels = channels
        self.adwin_threshold = adwin_threshold
        self.page_hinkley_threshold = page_hinkley_threshold
        self.consecutive_drifts = consecutive_drifts
        self.min_samples = min_samples

        # Drift detectors per channel
        self.residual_history = {ch: [] for ch in channels}
        self.adwin_counters = {ch: 0 for ch in channels}
        self.page_hinkley_scores = {ch: 0 for ch in channels}  # Cumulative sum for Page-Hinkley
        self.drift_confirmations = {ch: 0 for ch in channels}  # Require both ADWIN + PH
        self.drift_events = []
        self.sample_count = {ch: 0 for ch in channels}

        # Adaptation state
        self.is_adapting = False
        self.anomaly_active = False

    def update(self, residuals: Dict[str, float], timestamp: float) -> Tuple[bool, List[DriftEvent]]:
        """
        Update drift detectors with new residuals (ADWIN + Page-Hinkley).
        Returns: (should_adapt, drift_events)
        """
        new_events = []

        for channel in self.channels:
            if channel not in residuals or residuals[channel] is None:
                continue

            residual = abs(residuals[channel])
            self.residual_history[channel].append(residual)
            self.sample_count[channel] += 1

            # Skip if insufficient data
            if self.sample_count[channel] < self.min_samples:
                continue

            # ADWIN-like detector: recent vs. baseline distribution
            recent_window = self.residual_history[channel][-50:]
            baseline_window = self.residual_history[channel][:100]

            recent_mean = sum(recent_window) / len(recent_window)
            baseline_mean = sum(baseline_window) / len(baseline_window)
            relative_change = abs(recent_mean - baseline_mean) / (baseline_mean + 1e-8)

            adwin_detects = relative_change > self.adwin_threshold

            # Page-Hinkley detector: cumulative sum of (x - mean)
            baseline_std = np.std(baseline_window)
            ph_increment = (residual - baseline_mean) / (baseline_std + 1e-8)
            self.page_hinkley_scores[channel] += ph_increment
            page_hinkley_detects = abs(self.page_hinkley_scores[channel]) > self.page_hinkley_threshold

            # Drift confirmed only if BOTH detectors agree
            if adwin_detects and page_hinkley_detects:
                self.drift_confirmations[channel] += 1

                if self.drift_confirmations[channel] >= self.consecutive_drifts:
                    # Confirmed drift
                    event = DriftEvent(
                        timestamp=timestamp,
                        channel=channel,
                        detector="ADWIN+PageHinkley",
                        severity=min(max(relative_change, abs(self.page_hinkley_scores[channel]) / 10), 1.0),
                        description=f"Drift confirmed in {channel}: {relative_change:.2%} ADWIN, "
                                  f"PH={self.page_hinkley_scores[channel]:.1f}",
                    )
                    new_events.append(event)
                    self.drift_events.append(event)
                    self.drift_confirmations[channel] = 0  # Reset counter
                    self.page_hinkley_scores[channel] = 0  # Reset Page-Hinkley
            else:
                self.drift_confirmations[channel] = max(0, self.drift_confirmations[channel] - 1)

        # Decision logic for adaptation
        should_adapt = self._decide_adaptation(new_events)

        return should_adapt, new_events

    def _decide_adaptation(self, current_events: List[DriftEvent]) -> bool:
        """
        Decide whether to trigger model adaptation.
        Logic:
        - Adapt if drift confirmed AND no active anomaly
        - Block adaptation if we're actively alarming
        """
        if not current_events:
            return False

        # Check if anomaly is active (recent high residuals)
        # For simplicity: block if any channel had high residual recently
        self.anomaly_active = any(
            max(self.residual_history.get(ch, [0])[-10:] or [0]) > 2.0
            for ch in self.channels
        )

        return bool(current_events) and not self.anomaly_active

    def set_anomaly_active(self, active: bool):
        """Manually signal whether an anomaly is active."""
        self.anomaly_active = active

    def get_drift_summary(self) -> Dict:
        """Return summary of drift detection state."""
        return {
            "total_events": len(self.drift_events),
            "anomaly_active": self.anomaly_active,
            "is_adapting": self.is_adapting,
            "recent_events": [
                {
                    "timestamp": e.timestamp,
                    "channel": e.channel,
                    "severity": e.severity,
                    "description": e.description,
                }
                for e in self.drift_events[-10:]
            ],
        }

    def save_state(self, path: Path):
        """Save drift detection state."""
        path = Path(path)
        path.mkdir(exist_ok=True)

        state = {
            "drift_events": [
                {
                    "timestamp": e.timestamp,
                    "channel": e.channel,
                    "detector": e.detector,
                    "severity": e.severity,
                    "description": e.description,
                }
                for e in self.drift_events
            ],
            "residual_history_lengths": {ch: len(h) for ch, h in self.residual_history.items()},
            "anomaly_active": self.anomaly_active,
        }

        with open(path / "drift_state.json", "w") as f:
            json.dump(state, f, indent=2)


class FleetConsensus:
    """
    Fleet-wide consensus layer (v2: with masking).
    Distinguishes individual anomalies from fleet-wide regime changes.
    Masks out benign car-specific events (pit stops, traffic, lapped, tyre switches).
    """

    def __init__(self, num_drivers: int = 20, consensus_threshold: float = 0.7):
        self.num_drivers = num_drivers
        self.consensus_threshold = consensus_threshold
        self.residual_shifts = {d: [] for d in range(num_drivers)}
        self.fleet_residual_mean = 0

    def _is_masked_event(
        self,
        driver: int,
        lap_info: dict = None,
        gap_to_car_ahead: float = None,
    ) -> bool:
        """
        Check if this car's residual shift should be masked (ignored).
        Benign car-specific events:
        - Pit stop in/out laps
        - Being lapped (blue flag)
        - Running close to car ahead (gap < 1s)
        - Tyre compound switch (monitored via lap_info)
        """
        if lap_info is None:
            lap_info = {}

        # Mask pit stop laps (in-lap before pit, out-lap after pit)
        pit_in_time = lap_info.get("pit_in_time")
        pit_out_time = lap_info.get("pit_out_time")
        if pit_in_time is not None or pit_out_time is not None:
            return True

        # Mask if being lapped
        if lap_info.get("is_lapped", False):
            return True

        # Mask if running close to car ahead (traffic)
        if gap_to_car_ahead is not None and gap_to_car_ahead < 1.0:  # 1 second gap
            return True

        # Mask if tyre compound changed recently (in last 5 laps)
        current_compound = lap_info.get("tyre_compound")
        prev_compound = lap_info.get("prev_tyre_compound")
        if current_compound != prev_compound:
            return True

        return False

    def update(
        self,
        driver: int,
        residuals: Dict[str, float],
        lap_info: dict = None,
        gap_to_car_ahead: float = None,
    ) -> Dict:
        """
        Update consensus with residuals from one driver.
        lap_info: {"pit_in_time", "pit_out_time", "is_lapped", "tyre_compound", "prev_tyre_compound"}
        gap_to_car_ahead: seconds to the car ahead
        Returns: {"type": "benign_regime_change" | "individual_anomaly" | "masked_event"}
        """
        # Compute average residual magnitude
        magnitude = sum(
            abs(r) for r in residuals.values() if r is not None
        ) / len([r for r in residuals.values() if r is not None] or [1])

        # Check if this is a masked event
        is_masked = self._is_masked_event(driver, lap_info, gap_to_car_ahead)

        if is_masked:
            self.residual_shifts[driver].append(0)  # Zero out masked residuals
        else:
            self.residual_shifts[driver].append(magnitude)

        # Keep only recent samples
        if len(self.residual_shifts[driver]) > 100:
            self.residual_shifts[driver].pop(0)

        # Compute fleet statistics (excluding masked events)
        all_residuals = [
            r for residuals_list in self.residual_shifts.values()
            for r in residuals_list[-10:]  # Recent samples
            if r > 0  # Skip masked (zero) residuals
        ]

        if all_residuals:
            fleet_mean = sum(all_residuals) / len(all_residuals)
        else:
            fleet_mean = 0

        # Compare this driver's residual to fleet
        driver_recent = self.residual_shifts[driver][-10:] if self.residual_shifts[driver] else []
        driver_recent_mean = (
            sum(driver_recent) / len(driver_recent)
            if driver_recent
            else 0
        )

        # Count how many drivers have similar elevated residuals (excluding masked)
        drivers_elevated = sum(
            1 for d in range(self.num_drivers)
            if self.residual_shifts[d]
            and (sum(self.residual_shifts[d][-10:]) / max(len(self.residual_shifts[d][-10:]), 1) > fleet_mean * 1.2)
        )

        consensus_ratio = drivers_elevated / max(self.num_drivers - 1, 1)  # Exclude self

        classification = {
            "driver": driver,
            "driver_residual": driver_recent_mean,
            "fleet_mean_residual": fleet_mean,
            "consensus_ratio": consensus_ratio,
            "is_masked": is_masked,
            "type": (
                "masked_event" if is_masked
                else "benign_regime_change" if consensus_ratio > self.consensus_threshold
                else "individual_anomaly"
            ),
        }

        return classification


# Example usage
if __name__ == "__main__":
    import random

    drift_gate = DriftGate(channels=["speed", "rpm", "throttle", "brake"])
    fleet = FleetConsensus(num_drivers=20)

    print("Simulating residual stream...")
    for t in range(200):
        # Simulate residuals
        residuals = {
            "speed": random.gauss(0, 5),
            "rpm": random.gauss(0, 500),
            "throttle": random.gauss(0, 0.1),
            "brake": random.gauss(0, 0.1),
        }

        should_adapt, events = drift_gate.update(residuals, timestamp=float(t))

        if events:
            print(f"[t={t}] Drift detected: {events[0].description}")

        if t % 50 == 0:
            classification = fleet.update(driver=1, residuals=residuals)
            print(f"[t={t}] Fleet consensus: {classification['type']}")

    print("\nDrift summary:")
    print(json.dumps(drift_gate.get_drift_summary(), indent=2))
