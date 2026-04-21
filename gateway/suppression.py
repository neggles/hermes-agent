"""Channel suppression manager for ❌ reaction feedback.

Single source of truth for suppression state, used by:
  - Discord adapter reaction handler (writes suppression on ❌)
  - Watcher polling loop (checks suppression before triggering)
  - Session reset handler (clears suppression for a channel)

Thread/async safety: all disk operations use fcntl file locking so the
reaction handler (running on the Discord event loop) and the watcher
(running on the same event loop, possibly yielding across awaits) cannot
clobber each other's writes.
"""
import fcntl
import json
import logging
import time
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger("gateway.hooks.periodic_check.suppression")


class SuppressionManager:
    """Manages per-channel ❌ suppression state with atomic file-backed storage.

    All mutations go through locked read-modify-write cycles. The in-memory
    cache is always refreshed from disk before reads, so both the adapter
    (reaction handler) and the watcher (polling loop) see consistent state.
    """

    def __init__(self, state_path: Path):
        self._state_path = state_path
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_path = state_path.with_suffix(".suppression.lock")

    @contextmanager
    def _file_lock(self):
        """Acquire exclusive file lock for read-modify-write safety."""
        fd = open(self._lock_path, "w")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            fd.close()

    def _read_disk(self) -> dict[str, dict]:
        """Read suppression entries from the state file."""
        if not self._state_path.exists():
            return {}
        try:
            data = json.loads(self._state_path.read_text())
            return data.get("suppressed", {})
        except Exception:
            logger.warning("Failed to read suppression state", exc_info=True)
            return {}

    def _write_disk(self, suppressed: dict[str, dict]) -> None:
        """Write suppression entries to the state file, preserving other keys."""
        try:
            if self._state_path.exists():
                data = json.loads(self._state_path.read_text())
            else:
                data = {}
            data["suppressed"] = suppressed
            self._state_path.write_text(json.dumps(data, indent=2) + "\n")
        except Exception:
            logger.warning("Failed to write suppression state", exc_info=True)

    def suppress(
        self,
        channel_id: int | str,
        *,
        reactor: str = "",
        triggered_message_id: str = "",
        until_quiet_minutes: int = 10,
        reason: str = "x_reaction",
    ) -> None:
        """Add or update a suppression entry for a channel."""
        ch_key = str(channel_id)
        with self._file_lock():
            suppressed = self._read_disk()
            existing = suppressed.get(ch_key)
            reaction_count = (existing.get("reaction_count", 0) + 1) if existing else 1
            suppressed[ch_key] = {
                "until_quiet_minutes": until_quiet_minutes,
                "last_activity": time.time(),
                "reason": reason,
                "reactor": reactor,
                "triggered_message_id": triggered_message_id,
                "reaction_count": reaction_count,
            }
            self._write_disk(suppressed)
            logger.info(
                "Channel %s suppressed (reason=%s, reactor=%s, count=%d)",
                channel_id, reason, reactor, reaction_count,
            )

    def check(
        self,
        channel_id: int | str,
        has_new_activity: bool = False,
    ) -> str | None:
        """Check if a channel is suppressed.

        Returns reason string if suppressed, None if clear.
        Removes stale suppressions (quiet for >= until_quiet_minutes).
        Resets the quiet timer if there's new activity.
        """
        ch_key = str(channel_id)
        with self._file_lock():
            suppressed = self._read_disk()
            entry = suppressed.get(ch_key)
            if not entry:
                return None

            quiet_minutes = entry.get("until_quiet_minutes", 10)
            last_activity = entry.get("last_activity", 0)

            if has_new_activity:
                entry["last_activity"] = time.time()
                last_activity = entry["last_activity"]
                suppressed[ch_key] = entry
                self._write_disk(suppressed)

            elapsed = (time.time() - last_activity) / 60

            if elapsed >= quiet_minutes:
                del suppressed[ch_key]
                self._write_disk(suppressed)
                logger.info(
                    "Suppression lifted for channel %s (quiet for %.0f min)",
                    channel_id, elapsed,
                )
                return None

            return f"❌ feedback — {quiet_minutes - elapsed:.0f}min remaining"

    def clear_channel(self, channel_id: int | str) -> bool:
        """Remove suppression for a specific channel. Returns True if it was suppressed."""
        ch_key = str(channel_id)
        with self._file_lock():
            suppressed = self._read_disk()
            if ch_key in suppressed:
                del suppressed[ch_key]
                self._write_disk(suppressed)
                logger.info("Suppression cleared for channel %s", channel_id)
                return True
        return False

    def clear_all(self) -> int:
        """Clear all suppressions. Returns count cleared."""
        with self._file_lock():
            suppressed = self._read_disk()
            count = len(suppressed)
            if count:
                self._write_disk({})
                logger.info("Cleared %d suppression entries", count)
            return count

    def get_all(self) -> dict[str, dict]:
        """Get a snapshot of all current suppressions (read from disk)."""
        with self._file_lock():
            return self._read_disk()

    def is_suppressed(self, channel_id: int | str) -> bool:
        """Quick check without side effects (no timer reset, no expiry)."""
        ch_key = str(channel_id)
        with self._file_lock():
            suppressed = self._read_disk()
            entry = suppressed.get(ch_key)
            if not entry:
                return False
            elapsed = (time.time() - entry.get("last_activity", 0)) / 60
            return elapsed < entry.get("until_quiet_minutes", 10)


# Module-level singleton — shared by adapter and watcher hook
_instance: SuppressionManager | None = None


def get_suppression_manager() -> SuppressionManager:
    """Get or create the module-level SuppressionManager singleton."""
    global _instance
    if _instance is None:
        from hermes_constants import get_hermes_home
        state_path = get_hermes_home() / "data" / "periodic_check_state.json"
        _instance = SuppressionManager(state_path)
    return _instance
