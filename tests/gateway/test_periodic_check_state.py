import json
import time

from gateway.periodic_check_state import get_ambient_message, suppress_channel


def test_suppress_channel_preserves_existing_periodic_state(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    state_path = data_dir / "periodic_check_state.json"
    state_path.write_text(
        json.dumps(
            {
                "cursors": {"123": 456},
                "ambient_messages": {"99": {"channel_id": 123, "timestamp": time.time()}},
            }
        )
    )

    suppress_channel(123, reactor="42", triggered_message_id="99")

    raw = json.loads(state_path.read_text())
    assert raw["cursors"] == {"123": 456}
    assert raw["ambient_messages"]["99"]["channel_id"] == 123
    assert raw["suppressed"]["123"]["reactor"] == "42"


def test_get_ambient_message_reads_state_then_legacy(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "periodic_check_state.json").write_text(
        json.dumps({"ambient_messages": {"1": {"trigger_type": "state"}}})
    )
    (data_dir / "ambient_messages.json").write_text(
        json.dumps({"2": {"trigger_type": "legacy"}})
    )

    assert get_ambient_message(1)["trigger_type"] == "state"
    assert get_ambient_message(2)["trigger_type"] == "legacy"
