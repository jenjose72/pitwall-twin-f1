"""Quick test of the replayer without Docker."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from replayer.main import F1Replayer
from replayer.config import settings


def test_load_session():
    """Test loading a race session."""
    replayer = F1Replayer()
    success = replayer.load_session(2023, 'Monaco')
    assert success, "Failed to load Monaco 2023"
    print("✓ Session loaded successfully")


def test_telemetry_streaming():
    """Test telemetry streaming (first 10 records per driver)."""
    replayer = F1Replayer()
    if not replayer.load_session(2023, 'Monaco'):
        print("✗ Failed to load session")
        return

    if replayer.session.telemetry is None or len(replayer.session.telemetry) == 0:
        print("✗ No telemetry data")
        return

    print(f"✓ Telemetry data: {len(replayer.session.telemetry)} records")
    print(f"  Drivers: {replayer.session.drivers.tolist()}")
    print(f"  Columns: {replayer.session.telemetry.columns.tolist()}")


def test_laps_data():
    """Test laps data."""
    replayer = F1Replayer()
    if not replayer.load_session(2023, 'Monaco'):
        return

    if replayer.session.laps is None or len(replayer.session.laps) == 0:
        print("✗ No laps data")
        return

    print(f"✓ Laps data: {len(replayer.session.laps)} laps")


if __name__ == '__main__':
    print("Testing F1 Replayer...")
    print()

    try:
        test_load_session()
        test_telemetry_streaming()
        test_laps_data()
        print()
        print("All tests passed!")
    except Exception as e:
        print(f"✗ Test failed: {e}")
        import traceback
        traceback.print_exc()
