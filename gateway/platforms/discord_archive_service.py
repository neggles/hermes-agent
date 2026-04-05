import asyncio
import logging
from datetime import UTC, datetime

import discord
from pydantic import BaseModel, ConfigDict, Field, field_validator

from gateway.config import PlatformConfig
from gateway.platforms.discord_archive import DiscordArchiveDB, DiscordMessage

logger = logging.getLogger(__name__)

type DiscordChannel = discord.abc.GuildChannel | discord.abc.PrivateChannel | discord.Thread


def _is_dm_channel(channel: DiscordChannel | None) -> bool:
    return channel is not None and (
        isinstance(channel, discord.DMChannel) or channel.__class__.__name__.endswith("DMChannel")
    )


def _channel_allowlist_ids(channel: DiscordChannel | None) -> set[int]:
    """Gather all IDs relevant to a channel for allowlist filtering."""
    ids: set[int] = set()

    if channel is None:
        return ids
    if getattr(channel, "id", None) is not None:
        ids.add(channel.id)
    if getattr(channel, "parent_id", None) is not None:
        ids.add(channel.parent_id)
    if getattr(channel, "category_id", None) is not None:
        ids.add(channel.category_id)
    if getattr(channel, "parent", None) is not None:
        if getattr(channel.parent, "id", None) is not None:
            ids.add(channel.parent.id)
        if getattr(channel.parent, "category_id", None) is not None:
            ids.add(channel.parent.category_id)
    for recipient in list(getattr(channel, "recipients", []) or []):
        if getattr(recipient, "id", None) is not None:
            ids.add(recipient.id)

    return ids


def _normalize_message_text(text: object | None) -> str:
    normalized = str(text or "")
    normalized = normalized.replace("\r\n", "\n").replace("\r", "\n")
    normalized = normalized.replace("\x00", "")
    return normalized.strip()


def _discord_id(value: object | None) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _attachment_payload(att: discord.Attachment) -> dict[str, object]:
    return {
        "id": _discord_id(getattr(att, "id", None)),
        "filename": getattr(att, "filename", "") or "",
        "size": int(getattr(att, "size", 0) or 0),
        "url": getattr(att, "url", "") or "",
        "content_type": getattr(att, "content_type", None),
    }


def _preferred_user_name(user: discord.abc.User | discord.Member | None) -> str:
    return (
        str(getattr(user, "name", "") or "").strip() or str(getattr(user, "display_name", "") or "").strip()
    )


def _reference_ids(
    reference: discord.MessageReference | None,
) -> tuple[int | None, int | None, int | None]:
    return (
        _discord_id(getattr(reference, "message_id", None)),
        _discord_id(getattr(reference, "channel_id", None)),
        _discord_id(getattr(reference, "guild_id", None)),
    )


def _pick_round_robin_batch(
    channels: list[DiscordChannel],
    start_index: int,
    max_items: int,
) -> tuple[list[DiscordChannel], int]:
    if not channels or max_items <= 0:
        return [], 0
    n = len(channels)
    start = start_index % n
    take = min(max_items, n)
    batch = [channels[(start + idx) % n] for idx in range(take)]
    return batch, (start + take) % n


def _discord_object(message_id: int) -> discord.Object:
    return discord.Object(id=message_id)


class FrontfillConfig(BaseModel):
    model_config: ConfigDict = ConfigDict(extra="allow")

    enabled: bool = Field(
        True,
        description="Whether to enable frontfill (historical back-population on first sight)",
    )
    interval_sec: int = Field(30, description="Interval between frontfill runs (seconds)")
    max_channels_per_tick: int = Field(3, description="Max channels to frontfill per tick.")
    seed_limit: int = Field(100, description="Number of recent messages to seed when no cursor exists.")
    max_pages_per_channel: int = Field(10, description="Max number of pages to frontfill per tick")


class BackfillConfig(BaseModel):
    model_config: ConfigDict = ConfigDict(extra="allow")

    enabled: bool = Field(True, description="Whether to enable backfill of historical messages")
    interval_sec: int = Field(120, description="Interval between backfill runs (seconds)")
    max_channels_per_tick: int = Field(1, description="Max channels to backfill per tick.")
    max_pages_per_channel: int = Field(1, description="Max number of pages to backfill per tick")
    page_pause_sec: float = Field(0.5, description="Delay between page fetches (seconds)")
    channel_pause_sec: float = Field(1.0, description="Delay between channel backfills (seconds)")


class DiscordArchiveConfig(BaseModel):
    model_config: ConfigDict = ConfigDict(extra="allow")

    enabled: bool = Field(False, description="Whether the Discord archive is enabled.")
    include_dms: bool = Field(True, description="Whether to include DMs in the archive")
    frontfill: FrontfillConfig = Field(default_factory=FrontfillConfig)
    backfill: BackfillConfig = Field(default_factory=BackfillConfig, description="Backfill configuration")
    allowed_guild_ids: set[int] = Field(
        default_factory=set,
        description="Optional allowlist of guild IDs to archive. If empty, all guilds are included.",
    )
    allowed_channel_ids: set[int] = Field(
        default_factory=set,
        description="Optional allowlist of channel IDs to archive. If empty, all channels are included.",
    )

    @field_validator("allowed_guild_ids", "allowed_channel_ids", mode="before")
    @classmethod
    def _coerce_id_sets(cls, value: object) -> set[int]:
        if value in (None, "", []):
            return set()
        if isinstance(value, set):
            raw_items = value
        elif isinstance(value, (list, tuple)):
            raw_items = set(value)
        else:
            raw_items = {value}
        return {int(item) for item in raw_items if item not in (None, "")}

    @field_validator("frontfill", mode="before")
    @classmethod
    def _coerce_frontfill(cls, value: object) -> object:
        if isinstance(value, bool):
            return {"enabled": value}
        return value

    @field_validator("backfill", mode="before")
    @classmethod
    def _coerce_backfill(cls, value: object) -> object:
        if isinstance(value, bool):
            return {"enabled": value}
        return value


class DiscordArchiveService:
    """Best-effort Discord archive population sidecar."""

    def __init__(
        self,
        config: PlatformConfig,
        *,
        logger_name: str = __name__,
    ) -> None:
        self._platform_config = config
        self.config = DiscordArchiveConfig.model_validate(config.extra.get("archive", {}))
        self._logger = logging.getLogger(logger_name)

        self.client: discord.Client | None = None
        self.db: DiscordArchiveDB | None = None
        self._stop_event = asyncio.Event()
        self._frontfill_task: asyncio.Task | None = None
        self._backfill_task: asyncio.Task | None = None
        self._bootstrapped_channels: set[int] = set()
        self._active_history_channels: set[int] = set()
        self._frontfill_rr_index = 0
        self._backfill_rr_index = 0

    async def start(self, client: discord.Client) -> None:
        """Initialize archive state and optional workers."""
        self.client = client
        if not self.config.enabled:
            return

        if self.db is None:
            self.db = DiscordArchiveDB()

        if self.config.frontfill.enabled and (self._frontfill_task is None or self._frontfill_task.done()):
            self._stop_event.clear()
            self._frontfill_task = asyncio.create_task(
                self._frontfill_loop(),
                name="discord-archive-frontfill",
            )
        if self.config.backfill.enabled and (self._backfill_task is None or self._backfill_task.done()):
            self._stop_event.clear()
            self._backfill_task = asyncio.create_task(
                self._backfill_loop(),
                name="discord-archive-backfill",
            )

    async def stop(self) -> None:
        """Stop workers and close archive resources."""
        self._stop_event.set()
        if self._frontfill_task is not None:
            self._frontfill_task.cancel()
            try:
                await self._frontfill_task
            except asyncio.CancelledError:
                pass
            except Exception as e:  # pragma: no cover - defensive logging
                logger.debug("Discord archive frontfill shutdown failed: %s", e)
            self._frontfill_task = None

        if self._backfill_task is not None:
            self._backfill_task.cancel()
            try:
                await self._backfill_task
            except asyncio.CancelledError:
                pass
            except Exception as e:  # pragma: no cover - defensive logging
                logger.debug("Discord archive backfill shutdown failed: %s", e)
            self._backfill_task = None

        if self.db is not None:
            self.db.close()
            self.db = None

        self._bootstrapped_channels.clear()
        self._active_history_channels.clear()
        self._frontfill_rr_index = 0
        self._backfill_rr_index = 0
        self.client = None
        self._stop_event.clear()

    async def archive_message(self, message: discord.Message) -> None:
        """Persist one live Discord message."""
        if not self.db or not self.config.enabled:
            return
        if not self.is_channel_allowed(message.channel):
            return

        try:
            await self.bootstrap_channel(message.channel)
        except Exception as e:  # pragma: no cover - best effort
            self._logger.debug("Discord archive bootstrap failed while archiving message: %s", e)

        try:
            self.db.upsert_message(self.message_to_archive_model(message))
        except Exception as e:  # pragma: no cover - best effort
            self._logger.debug("Discord archive upsert failed: %s", e)

    async def archive_message_edit(self, before: discord.Message, after: discord.Message) -> None:
        """Persist one Discord message edit."""
        if not self.db or not self.config.enabled:
            return
        if not self.is_channel_allowed(before.channel):
            return

        try:
            await self.bootstrap_channel(before.channel)
        except Exception as e:  # pragma: no cover - best effort
            self._logger.debug("Discord archive bootstrap failed while archiving edit: %s", e)

        try:
            before_model = self.message_to_archive_model(before)
            after_model = self.message_to_archive_model(after)
        except Exception as e:  # pragma: no cover - best effort
            self._logger.debug("Discord archive edit row shaping failed: %s", e)
            return

        try:
            self.db.record_message_edit(before=before_model, after=after_model)
        except Exception as e:  # pragma: no cover - best effort
            self._logger.debug("Discord archive edit change-log insert failed: %s", e)

        try:
            self.db.upsert_message(after_model)
        except Exception as e:  # pragma: no cover - best effort
            self._logger.debug("Discord archive edit latest-row upsert failed: %s", e)

    def mark_deleted(
        self,
        *,
        message_id: int,
        channel_id: int | None = None,
        guild_id: int | None = None,
    ) -> None:
        """Mark one message deleted in the archive."""
        if not self.db or not self.config.enabled:
            return
        try:
            self.db.mark_deleted(message_id=message_id, channel_id=channel_id, guild_id=guild_id)
        except Exception as e:  # pragma: no cover - best effort
            self._logger.debug("Discord archive delete mark failed: %s", e)

    async def bootstrap_channel(self, channel: DiscordChannel) -> None:
        """Seed/archive one channel on first sight."""
        if (
            not self.db
            or not self.config.enabled
            or not self.config.frontfill.enabled
            or not self.is_channel_allowed(channel)
        ):
            return

        channel_id = channel.id
        if channel_id in self._bootstrapped_channels:
            return

        self._bootstrapped_channels.add(channel_id)
        try:
            await self.sync_channel_forward(
                channel,
                max_pages=max(1, min(self.config.frontfill.max_pages_per_channel, 100)),
                seed_limit=max(1, min(self.config.frontfill.seed_limit, 1000)),
                drain_all_pages=True,
            )
        except Exception as e:  # pragma: no cover - best effort
            self._bootstrapped_channels.discard(channel_id)
            self._logger.debug("Discord channel bootstrap failed (%s): %s", channel_id, e)

    def can_view_channel(self, channel: DiscordChannel) -> bool:
        if _is_dm_channel(channel) or getattr(channel, "guild", None) is None:
            return True

        try:
            guild = channel.guild
            self_member = guild.me
            permissions = channel.permissions_for(self_member)
            if not permissions.read_messages:
                return False
            if not permissions.read_message_history:
                return False
        except Exception:
            self._logger.exception("Error checking channel permissions for %s", channel.id)
            return False

        return True

    def is_channel_allowed(self, channel: DiscordChannel | None) -> bool:
        if not channel:
            return False

        if not self.can_view_channel(channel):
            return False

        if _is_dm_channel(channel):
            dms_enabled = bool(self._platform_config.extra.get("enable_dms", True))
            return dms_enabled and self.config.include_dms

        if self.config.allowed_channel_ids:
            candidate_ids = _channel_allowlist_ids(channel)
            if not candidate_ids or not (candidate_ids & self.config.allowed_channel_ids):
                return False

        if self.config.allowed_guild_ids:
            if channel.guild.id not in self.config.allowed_guild_ids:
                return False

        return True

    def message_to_archive_model(self, message: discord.Message) -> DiscordMessage:
        channel = getattr(message, "channel", None)
        guild = getattr(channel, "guild", None)
        author = getattr(message, "author", None)
        reference = getattr(message, "reference", None)

        created_at = getattr(message, "created_at", None) or datetime.now(tz=UTC)
        edited_at = getattr(message, "edited_at", None)
        author_id = _discord_id(getattr(author, "id", None))
        guild_id = _discord_id(getattr(guild, "id", None))

        if channel is None or getattr(channel, "id", None) is None:
            raise ValueError("message channel is missing a numeric ID")
        channel_id = channel.id

        reply_to_message_id, reply_to_channel_id, reply_to_guild_id = _reference_ids(reference)
        preferred_name = _preferred_user_name(author) or None

        return DiscordMessage(
            message_id=int(message.id),
            guild_id=guild_id,
            guild_name=getattr(guild, "name", None),
            channel_id=channel_id,
            channel_name=getattr(channel, "name", str(channel_id)),
            thread_id=channel_id
            if isinstance(channel, discord.Thread) or getattr(channel, "parent_id", None) is not None
            else None,
            author_id=author_id,
            author_name=preferred_name,
            author_display=preferred_name or (str(author_id) if author_id is not None else None),
            author_is_bot=bool(getattr(author, "bot", False)),
            content=_normalize_message_text(getattr(message, "content", "")),
            attachments_json=[_attachment_payload(att) for att in getattr(message, "attachments", [])],
            reactions_json=[],
            reply_to_message_id=reply_to_message_id,
            reply_to_channel_id=reply_to_channel_id,
            reply_to_guild_id=reply_to_guild_id,
            created_at=created_at,
            edited_at=edited_at,
            deleted=False,
        )

    async def _frontfill_loop(self) -> None:
        self._logger.info(
            "Discord archive frontfill started (interval=%ss, channels/tick=%s, pages/channel=%s)",
            self.config.frontfill.interval_sec,
            self.config.frontfill.max_channels_per_tick,
            self.config.frontfill.max_pages_per_channel,
        )
        try:
            while not self._stop_event.is_set():
                try:
                    await self.run_frontfill_tick()
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # pragma: no cover - defensive logging
                    self._logger.debug("Discord archive frontfill tick failed: %s", e)

                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=max(5, min(self.config.frontfill.interval_sec, 3600)),
                    )
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass
        finally:
            self._logger.info("Discord archive frontfill stopped")

    async def _backfill_loop(self) -> None:
        self._logger.info(
            "Discord archive backfill started (interval=%ss, channels/tick=%s, pages/channel=%s)",
            self.config.backfill.interval_sec,
            self.config.backfill.max_channels_per_tick,
            self.config.backfill.max_pages_per_channel,
        )
        try:
            while not self._stop_event.is_set():
                try:
                    await self.run_backfill_tick()
                except asyncio.CancelledError:
                    raise
                except Exception as e:  # pragma: no cover - defensive logging
                    self._logger.debug("Discord archive backfill tick failed: %s", e)

                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=max(10, min(self.config.backfill.interval_sec, 3600)),
                    )
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            pass
        finally:
            self._logger.info("Discord archive backfill stopped")

    async def collect_history_targets(self) -> list[DiscordChannel]:
        if self.client is None:
            return []

        rows: list[DiscordChannel] = []
        for channel in list(getattr(self.client, "private_channels", []) or []):
            if self.is_channel_allowed(channel):
                rows.append(channel)

        for guild in list(getattr(self.client, "guilds", []) or []):
            for channel in list(getattr(guild, "text_channels", []) or []):
                if self.is_channel_allowed(channel):
                    rows.append(channel)

            for thread in list(getattr(guild, "threads", []) or []):
                if self.is_channel_allowed(thread):
                    rows.append(thread)

            active_threads_getter = getattr(guild, "active_threads", None)
            if callable(active_threads_getter):
                try:
                    active_threads = await active_threads_getter()
                except Exception as e:
                    self._logger.debug(
                        "Discord active-thread discovery failed (guild=%s): %s",
                        getattr(guild, "id", "unknown"),
                        e,
                    )
                else:
                    for thread in list(active_threads or []):
                        if self.is_channel_allowed(thread):
                            rows.append(thread)

        rows.sort(
            key=lambda ch: (
                str(getattr(getattr(ch, "guild", None), "id", "")),
                str(getattr(ch, "id", "")),
            )
        )
        deduped: list[DiscordChannel] = []
        seen_ids: set[int] = set()
        for channel in rows:
            if channel.id in seen_ids:
                continue
            if not callable(getattr(channel, "history", None)):
                continue
            seen_ids.add(channel.id)
            deduped.append(channel)
        return deduped

    async def run_frontfill_tick(self) -> None:
        if not self.db or not self.config.frontfill.enabled:
            return
        targets = await self.collect_history_targets()
        if not targets:
            return

        batch, self._frontfill_rr_index = _pick_round_robin_batch(
            targets,
            self._frontfill_rr_index,
            max(1, min(self.config.frontfill.max_channels_per_tick, 100)),
        )
        for channel in batch:
            try:
                await self.sync_channel_forward(
                    channel,
                    max_pages=max(1, min(self.config.frontfill.max_pages_per_channel, 100)),
                    seed_limit=max(1, min(self.config.frontfill.seed_limit, 1000)),
                    drain_all_pages=False,
                )
            except Exception as e:  # pragma: no cover - defensive logging
                self._logger.debug(
                    "Discord archive frontfill channel sync failed (%s): %s",
                    getattr(channel, "id", "unknown"),
                    e,
                )

    async def run_backfill_tick(self) -> None:
        if not self.db or not self.config.backfill.enabled:
            return
        targets = await self.collect_history_targets()
        if not targets:
            return

        batch, self._backfill_rr_index = _pick_round_robin_batch(
            targets,
            self._backfill_rr_index,
            max(1, min(self.config.backfill.max_channels_per_tick, 25)),
        )
        for index, channel in enumerate(batch):
            try:
                await self.sync_channel_backfill(
                    channel,
                    max_pages=max(1, min(self.config.backfill.max_pages_per_channel, 25)),
                )
            except Exception as e:  # pragma: no cover - defensive logging
                self._logger.debug(
                    "Discord archive backfill channel sync failed (%s): %s",
                    getattr(channel, "id", "unknown"),
                    e,
                )
            if index < len(batch) - 1:
                await self._sleep_or_stop(max(0.0, min(self.config.backfill.channel_pause_sec, 30.0)))

    async def _sleep_or_stop(self, seconds: float) -> None:
        if seconds <= 0:
            return
        try:
            await asyncio.wait_for(self._stop_event.wait(), timeout=seconds)
        except asyncio.TimeoutError:
            pass

    async def sync_channel_forward(
        self,
        channel: DiscordChannel,
        *,
        max_pages: int,
        seed_limit: int,
        drain_all_pages: bool,
    ) -> int:
        if not self.db or not self.is_channel_allowed(channel):
            return 0

        if channel.id in self._active_history_channels:
            return 0
        self._active_history_channels.add(channel.id)

        try:
            cursor = self.db.get_channel_cursor(channel.id)
            if cursor is None:
                seeded = [m async for m in channel.history(limit=max(1, seed_limit), oldest_first=True)]
                for hist_msg in seeded:
                    self.db.upsert_message(self.message_to_archive_model(hist_msg))
                return len(seeded)

            after_obj = _discord_object(cursor)
            pages_left = None if drain_all_pages else max(1, int(max_pages))
            total = 0
            while True:
                batch = [m async for m in channel.history(limit=100, oldest_first=True, after=after_obj)]
                if not batch:
                    break
                for hist_msg in batch:
                    self.db.upsert_message(self.message_to_archive_model(hist_msg))
                total += len(batch)

                latest_id = getattr(batch[-1], "id", None)
                try:
                    latest_id = int(latest_id) if latest_id is not None else None
                except (TypeError, ValueError):
                    latest_id = None
                if latest_id is None:
                    break
                after_obj = _discord_object(latest_id)

                if len(batch) < 100:
                    break
                if pages_left is not None:
                    pages_left -= 1
                    if pages_left <= 0:
                        break
            return total
        finally:
            self._active_history_channels.discard(channel.id)

    async def sync_channel_backfill(
        self,
        channel: DiscordChannel,
        *,
        max_pages: int,
    ) -> int:
        if not self.db or not self.is_channel_allowed(channel):
            return 0

        if channel.id in self._active_history_channels:
            return 0
        self._active_history_channels.add(channel.id)

        try:
            state = self.db.get_backfill_state(channel.id)
            if bool(state.get("complete")):
                return 0

            oldest_message_id = state.get("oldest_message_id")
            oldest_created_at = state.get("oldest_created_at")
            if oldest_message_id is None:
                oldest_row = self.db.get_oldest_message(channel.id)
                if not oldest_row:
                    return 0
                oldest_message_id = oldest_row.get("message_id")
                oldest_created_at = oldest_row.get("created_at")
                if oldest_message_id is None:
                    return 0
                self.db.upsert_backfill_state(
                    channel.id,
                    oldest_message_id=oldest_message_id,
                    oldest_created_at=oldest_created_at,
                    complete=False,
                )

            before_obj = _discord_object(oldest_message_id)
            pages_left = max(1, int(max_pages))
            total = 0
            reached_start = False

            while pages_left > 0:
                batch = [m async for m in channel.history(limit=100, oldest_first=False, before=before_obj)]
                if not batch:
                    reached_start = True
                    break

                for hist_msg in batch:
                    self.db.upsert_message(self.message_to_archive_model(hist_msg))
                total += len(batch)

                oldest_msg = batch[-1]
                next_oldest_id = getattr(oldest_msg, "id", None)
                try:
                    next_oldest_id = int(next_oldest_id) if next_oldest_id is not None else None
                except (TypeError, ValueError):
                    next_oldest_id = None
                next_oldest_created = getattr(oldest_msg, "created_at", None)

                if next_oldest_id is not None:
                    self.db.upsert_backfill_state(
                        channel.id,
                        oldest_message_id=next_oldest_id,
                        oldest_created_at=next_oldest_created,
                        complete=False,
                    )

                if next_oldest_id is None or next_oldest_id == oldest_message_id:
                    reached_start = True
                    break

                oldest_message_id = next_oldest_id
                before_obj = _discord_object(oldest_message_id)

                if len(batch) < 100:
                    reached_start = True
                    break

                pages_left -= 1
                if pages_left > 0:
                    await self._sleep_or_stop(max(0.0, min(self.config.backfill.page_pause_sec, 30.0)))

            if reached_start:
                self.db.mark_backfill_complete(channel.id, complete=True)
            return total
        finally:
            self._active_history_channels.discard(channel.id)
