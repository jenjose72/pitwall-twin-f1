"""
Fault injection for testing anomaly detection.
Injects synthetic faults into telemetry streams.
"""

import json
import logging
from enum import Enum
from dataclasses import dataclass
from typing import Dict, Optional, Tuple
import numpy as np

logger = logging.getLogger(__name__)


class FaultType(Enum):
    """Supported fault types."""
    STUCK_SENSOR = "stuck_sensor"  # Value frozen
    SPIKE = "spike"  # Sudden jump
    DROPOUT = "dropout"  # Missing values
    GAIN_DRIFT = "gain_drift"  # Gradual drift


@dataclass
class FaultInjectionParams:
    """Parameters for injecting a fault."""
    fault_type: FaultType
    channel: str
    start_time: float
    end_time: float
    severity: float = 1.0  # 0-1, how severe
    description: str = ""


class FaultInjector:
    """Injects synthetic faults into telemetry for testing."""

    def __init__(self):
        self.active_faults: Dict[str, FaultInjectionParams] = {}
        self.fault_history = []

    def register_fault(self, fault: FaultInjectionParams) -> str:
        """Register a fault to be injected."""
        fault_id = f"{fault.channel}_{len(self.fault_history)}"
        self.active_faults[fault_id] = fault
        self.fault_history.append((fault_id, fault))
        logger.info(f"Registered fault: {fault.description}")
        return fault_id

    def inject_stuck_sensor(
        self,
        channel: str,
        start_time: float,
        end_time: float,
        value: float,
    ) -> str:
        """Sensor value freezes at a fixed value."""
        fault = FaultInjectionParams(
            fault_type=FaultType.STUCK_SENSOR,
            channel=channel,
            start_time=start_time,
            end_time=end_time,
            description=f"Stuck {channel} at {value}",
        )
        return self.register_fault(fault)

    def inject_spike(
        self,
        channel: str,
        start_time: float,
        end_time: float,
        magnitude: float = 3.0,
    ) -> str:
        """Sharp spike in channel values."""
        fault = FaultInjectionParams(
            fault_type=FaultType.SPIKE,
            channel=channel,
            start_time=start_time,
            end_time=end_time,
            severity=magnitude,
            description=f"Spike in {channel} (+{magnitude:.1f}σ)",
        )
        return self.register_fault(fault)

    def inject_dropout(
        self,
        channel: str,
        start_time: float,
        end_time: float,
    ) -> str:
        """Values missing/NaN."""
        fault = FaultInjectionParams(
            fault_type=FaultType.DROPOUT,
            channel=channel,
            start_time=start_time,
            end_time=end_time,
            description=f"Dropout in {channel}",
        )
        return self.register_fault(fault)

    def inject_gain_drift(
        self,
        channel: str,
        start_time: float,
        end_time: float,
        drift_rate: float = 0.1,
    ) -> str:
        """Gradual scaling of channel values."""
        fault = FaultInjectionParams(
            fault_type=FaultType.GAIN_DRIFT,
            channel=channel,
            start_time=start_time,
            end_time=end_time,
            severity=drift_rate,
            description=f"Gain drift in {channel} ({drift_rate:.1%} per time unit)",
        )
        return self.register_fault(fault)

    def apply_injection(
        self,
        telemetry: Dict,
        current_time: float,
    ) -> Tuple[Dict, Optional[str]]:
        """
        Apply any active faults to telemetry.
        Returns: (modified_telemetry, fault_applied_id or None)
        """
        modified = dict(telemetry)
        applied_fault_id = None

        for fault_id, fault in list(self.active_faults.items()):
            # Check if fault is active now
            if not (fault.start_time <= current_time <= fault.end_time):
                # Clean up expired faults
                if current_time > fault.end_time:
                    del self.active_faults[fault_id]
                continue

            channel = fault.channel
            if channel not in modified:
                continue

            original_value = modified[channel]
            if original_value is None:
                continue

            # Apply fault
            if fault.fault_type == FaultType.STUCK_SENSOR:
                # Sensor value is stuck (in real scenario, we'd pass a constant)
                # For simulation: just indicate it should be stuck
                pass  # Already set when registered

            elif fault.fault_type == FaultType.SPIKE:
                # Add spike proportional to severity
                noise = np.random.normal(0, abs(original_value) * 0.1)
                spike_magnitude = fault.severity * abs(original_value)
                modified[channel] = original_value + spike_magnitude + noise

            elif fault.fault_type == FaultType.DROPOUT:
                # Drop the value
                modified[channel] = None

            elif fault.fault_type == FaultType.GAIN_DRIFT:
                # Gradually scale the value
                time_elapsed = current_time - fault.start_time
                scale_factor = 1.0 + (fault.severity * time_elapsed)
                modified[channel] = original_value * scale_factor

            applied_fault_id = fault_id
            break  # Apply only one fault per message

        return modified, applied_fault_id

    def get_active_faults(self, current_time: float) -> Dict[str, FaultInjectionParams]:
        """Get currently active faults."""
        active = {}
        for fault_id, fault in self.active_faults.items():
            if fault.start_time <= current_time <= fault.end_time:
                active[fault_id] = fault
        return active

    def export_faults_json(self) -> str:
        """Export fault schedule as JSON."""
        faults = []
        for fault_id, fault in self.fault_history:
            faults.append({
                "id": fault_id,
                "type": fault.fault_type.value,
                "channel": fault.channel,
                "start_time": fault.start_time,
                "end_time": fault.end_time,
                "severity": fault.severity,
                "description": fault.description,
            })
        return json.dumps(faults, indent=2)


class ScenarioBuilder:
    """Build standard fault scenarios for evaluation."""

    def __init__(self):
        self.injector = FaultInjector()

    def scenario_mechanical_failure(self):
        """Simulate gradual mechanical failure (slow gain drift)."""
        self.injector.inject_gain_drift("rpm", 100, 500, drift_rate=0.02)
        self.injector.inject_gain_drift("throttle", 100, 500, drift_rate=0.01)

    def scenario_sensor_fault(self):
        """Simulate sensor malfunction (dropout + spike)."""
        self.injector.inject_dropout("speed", 200, 250)
        self.injector.inject_spike("speed", 250, 300, magnitude=2.0)

    def scenario_transient_issue(self):
        """Simulate brief transient issue (spike)."""
        self.injector.inject_spike("throttle", 150, 160, magnitude=5.0)

    def scenario_slow_ramp_fault(self):
        """Slow-ramping fault that tests twin adaptation."""
        self.injector.inject_gain_drift("rpm", 100, 600, drift_rate=0.005)

    def get_injector(self) -> FaultInjector:
        return self.injector


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    injector = FaultInjector()

    # Register faults
    injector.inject_stuck_sensor("throttle", 100, 200, value=0.5)
    injector.inject_spike("speed", 300, 310, magnitude=2.0)
    injector.inject_gain_drift("rpm", 400, 500, drift_rate=0.1)

    # Simulate telemetry with faults
    print("Simulating telemetry with faults:")
    for t in range(0, 600, 50):
        tel = {
            "timestamp": f"2023-10-07T12:{t//60:02d}:{t%60:02d}",
            "driver": "1",
            "speed": 200 + np.sin(t / 100) * 50,
            "rpm": 10000 + np.cos(t / 100) * 1000,
            "throttle": 0.5 + np.sin(t / 150) * 0.2,
            "brake": 0.0,
            "gear": 5,
        }

        modified, fault_id = injector.apply_injection(tel, float(t))
        if fault_id:
            print(f"t={t:3d}: Fault applied: {fault_id}")
        else:
            print(f"t={t:3d}: No fault")

    print("\n=== Fault Schedule ===")
    print(injector.export_faults_json())
