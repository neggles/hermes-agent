"""Gateway-side helpers for periodic-check runtime state.

The periodic-check hook lives in the user hooks directory, while Discord
feedback handling lives inside the gateway package.  This module gives gateway
code a stable integration point for the hook's JSON state without importing
hook-private modules by fragile path names.
"""

import json
import logging
import time
from pathlib import Path
from typing import Any

from filelock import FileLock

from hermes_cli.config import get_hermes_home
from utils import atomic_json_write

logger = logging.getLogger(__name__)


def _state_path() -> Path:
    return get_hermes_home() / "data" / "periodic_check_state.json"


def _ambient_legacy_path() -> Path:
    return get_hermes_home() / "data" / "ambient_messages.json"


def _lock_path(path: Path) -> Path:
    return path.with_suffix(path.suffix + ".lock")


def _read_raw(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        logger.warning("Failed to read periodic-check state", exc_info=True)
        return {}


def suppress_channel(
    channel_id: int | str,
    *,
    reactor: str = "",
    triggered_message_id: str = "",
    until_quiet_minutes: int = 10,
    reason: str = "x_reaction",
) -> None:
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with FileLock(str(_lock_path(path))):
        raw = _read_raw(path)
        suppressed = raw.setdefault("suppressed", {})
        if not isinstance(suppressed, dict):
            suppressed = {}
            raw["suppressed"] = suppressed
        key = str(channel_id)
        existing = suppressed.get(key) if isinstance(suppressed.get(key), dict) else {}
        suppressed[key] = {
            "until_quiet_minutes": until_quiet_minutes,
            "last_activity": time.time(),
            "reason": reason,
            "reactor": reactor,
            "triggered_message_id": triggered_message_id,
            "reaction_count": int(existing.get("reaction_count", 0) or 0) + 1,
        }
        atomic_json_write(path, raw)


def get_ambient_message(message_id: int | str) -> dict[str, Any] | None:
    key = str(message_id)
    path = _state_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    with FileLock(str(_lock_path(path))):
        raw = _read_raw(path)
        ambient = raw.get("ambient_messages", {})
        if isinstance(ambient, dict) and isinstance(ambient.get(key), dict):
            return ambient[key]

    legacy_path = _ambient_legacy_path()
    if legacy_path.exists():
        try:
            legacy = json.loads(legacy_path.read_text(encoding="utf-8"))
            entry = legacy.get(key) if isinstance(legacy, dict) else None
            return entry if isinstance(entry, dict) else None
        except Exception:
            logger.debug("Failed to read legacy ambient message state", exc_info=True)
    return None
