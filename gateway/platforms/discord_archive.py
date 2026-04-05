# ruff: noqa: E712
"""
Discord archive storage (SQLite + FTS5).

Stores Discord channel messages for local search/retrieval and keeps enough
metadata for channel-scoped filtering.
"""

import json
import re
from datetime import UTC, datetime, timedelta
from typing import Any

import sqlalchemy as sa
from discord.abc import ChannelType
from sqlalchemy.orm import Mapped, mapped_column, sessionmaker

from .discord_db import (
    AsyncSession,
    Base,
    BigIntPK,
    CreateTimestamp,
    DiscordID,
    OptionalDiscordID,
    RequiredTimestamp,
    Snowflake,
    SyncSession,
    SyncSessionType,
    Timestamp,
    UpdateTimestamp,
    get_async_engine,
    get_sync_engine,
)


class DiscordMessage(Base):
    __tablename__ = "messages"
    __table_args__ = (
        sa.Index("idx_messages_channel_created", "channel_id", "created_at", unique=True),
        sa.Index("idx_messages_guild_channel_created", "guild_id", "channel_id", "created_at"),
    )

    message_id: Mapped[Snowflake] = mapped_column(sa.BigInteger, primary_key=True)
    guild_id: Mapped[OptionalDiscordID]
    guild_name: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    channel_id: Mapped[OptionalDiscordID]
    channel_name: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    thread_id: Mapped[OptionalDiscordID]
    author_id: Mapped[OptionalDiscordID] = mapped_column(sa.BigInteger, nullable=True, index=True)
    author_name: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    author_display: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    author_is_bot: Mapped[bool] = mapped_column(sa.Boolean, nullable=False, default=False)

    content: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    attachments_json: Mapped[dict | list | None] = mapped_column(sa.JSON, nullable=True)
    reactions_json: Mapped[dict | list | None] = mapped_column(sa.JSON, nullable=True)

    reply_to_message_id: Mapped[OptionalDiscordID]
    reply_to_channel_id: Mapped[OptionalDiscordID]
    reply_to_guild_id: Mapped[OptionalDiscordID]

    created_at: Mapped[RequiredTimestamp]
    edited_at: Mapped[Timestamp]
    deleted: Mapped[bool] = mapped_column(sa.Boolean, nullable=False, default=False)


class DiscordChannel(Base):
    __tablename__ = "channels"
    __table_args__ = (
        sa.Index("idx_channels_guild_id", "guild_id"),
        sa.Index("idx_channels_name", "name"),
    )

    id: Mapped[DiscordID] = mapped_column(sa.BigInteger, primary_key=True)
    guild_id: Mapped[OptionalDiscordID]
    channel_type: Mapped[ChannelType] = mapped_column(sa.Integer, nullable=True)
    name: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    topic: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    updated_at: Mapped[UpdateTimestamp]


class MessageChange(Base):
    __tablename__ = "message_changes"
    __table_args__ = (
        sa.Index("idx_message_changes_message_changed", "message_id", "changed_at"),
        sa.Index("idx_message_changes_message", "message_id", "changed_at"),
    )

    id: Mapped[BigIntPK]
    message_id: Mapped[DiscordID]
    guild_id: Mapped[OptionalDiscordID]
    channel_id: Mapped[OptionalDiscordID]
    author_id: Mapped[OptionalDiscordID]
    author_name: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    author_display: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    author_is_bot: Mapped[bool] = mapped_column(sa.Boolean, nullable=False, default=False)
    original_created_at: Mapped[Timestamp]
    changed_at: Mapped[RequiredTimestamp]
    change_type: Mapped[str] = mapped_column(sa.Text, nullable=False)
    before_content: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    after_content: Mapped[str | None] = mapped_column(sa.Text, nullable=True)


class ReactionChange(Base):
    __tablename__ = "reaction_changes"
    __table_args__ = (
        sa.Index("idx_reaction_changes_message_changed", "message_id", "changed_at"),
        sa.Index("idx_reaction_changes_message", "message_id", "changed_at"),
    )

    id: Mapped[BigIntPK]
    message_id: Mapped[DiscordID]
    guild_id: Mapped[OptionalDiscordID]
    channel_id: Mapped[OptionalDiscordID]
    author_id: Mapped[OptionalDiscordID]
    author_name: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    author_display: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    author_is_bot: Mapped[bool] = mapped_column(sa.Boolean, nullable=False, default=False)
    original_created_at: Mapped[Timestamp]
    changed_at: Mapped[RequiredTimestamp]
    change_type: Mapped[str] = mapped_column(sa.Text, nullable=False)
    emoji_key: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    emoji_name: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    emoji_display: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    emoji_id: Mapped[OptionalDiscordID]
    message_author_id: Mapped[OptionalDiscordID]
    message_author_name: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    message_author_display: Mapped[str | None] = mapped_column(sa.Text, nullable=True)


class ChannelState(Base):
    __tablename__ = "channel_state"

    channel_id: Mapped[DiscordID] = mapped_column(sa.BigInteger, primary_key=True)
    last_message_id: Mapped[OptionalDiscordID]
    updated_at: Mapped[UpdateTimestamp]


class ChannelTurnState(Base):
    __tablename__ = "channel_turn_state"

    channel_id: Mapped[DiscordID] = mapped_column(sa.BigInteger, primary_key=True)
    last_context_message_id: Mapped[OptionalDiscordID]
    updated_at: Mapped[UpdateTimestamp]


class ChannelBackfillState(Base):
    __tablename__ = "channel_backfill_state"
    __table_args__ = (sa.Index("idx_channel_backfill_complete", "complete", "updated_at"),)

    channel_id: Mapped[DiscordID] = mapped_column(sa.BigInteger, primary_key=True)
    oldest_message_id: Mapped[OptionalDiscordID]
    oldest_created_at: Mapped[Timestamp] = mapped_column(nullable=True)
    complete: Mapped[bool] = mapped_column(sa.Boolean, nullable=False, default=False)
    updated_at: Mapped[UpdateTimestamp]


class ThreadContextSeed(Base):
    __tablename__ = "thread_context_seed"
    __table_args__ = (sa.Index("idx_thread_seed_parent", "parent_channel_id", "updated_at"),)

    thread_id: Mapped[DiscordID] = mapped_column(sa.BigInteger, primary_key=True)
    guild_id: Mapped[OptionalDiscordID]
    parent_channel_id: Mapped[OptionalDiscordID]
    anchor_message_id: Mapped[OptionalDiscordID]
    seed_text: Mapped[str | None] = mapped_column(sa.Text, nullable=True)
    seed_kind: Mapped[str] = mapped_column(sa.Text, nullable=False)
    created_at: Mapped[CreateTimestamp]
    updated_at: Mapped[UpdateTimestamp]


class ThreadScrapeState(Base):
    __tablename__ = "thread_scrape_state"

    parent_channel_id: Mapped[DiscordID] = mapped_column(sa.BigInteger, primary_key=True)
    public_before_ts: Mapped[Timestamp] = mapped_column(nullable=True)
    private_before_ts: Mapped[Timestamp] = mapped_column(nullable=True)

    updated_at: Mapped[UpdateTimestamp]


class DiscordArchiveDB:
    """SQLAlchemy-backed Discord archive with FTS5 search."""

    def __init__(self):
        self._sync_engine = get_sync_engine()
        self._async_engine = get_async_engine()
        self.sync_session: SyncSessionType = SyncSession

        create_all_tables()

    def close(self) -> None:
        if self._sync_engine:
            self._sync_engine.dispose()
        if self._async_engine:
            self._async_engine.dispose()

    @staticmethod
    def _coerce_datetime(value: Any) -> datetime | None:
        if value is None or value == "":
            return None
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=UTC)
        return datetime.fromtimestamp(float(value), tz=UTC)

    @staticmethod
    def _row_has(row: Any, key: str) -> bool:
        mapping = getattr(row, "_mapping", None)
        if mapping is not None:
            return key in mapping
        if isinstance(row, dict):
            return key in row
        keys = getattr(row, "keys", None)
        if callable(keys):
            try:
                return key in keys()
            except Exception:
                return False
        return hasattr(row, key)

    @staticmethod
    def _row_get(row: Any, key: str, default: Any = None) -> Any:
        mapping = getattr(row, "_mapping", None)
        if mapping is not None:
            return mapping.get(key, default)
        if isinstance(row, dict):
            return row.get(key, default)
        keys = getattr(row, "keys", None)
        if callable(keys):
            try:
                if key in keys():
                    return row[key]
            except Exception:
                pass
        return getattr(row, key, default)

    def get_channel_cursor(self, channel_id: int) -> int | None:
        with self.sync_session() as session:
            stmt = sa.select(ChannelState).where(ChannelState.channel_id == channel_id)
            channel_state = session.scalars(stmt).one_or_none()
            return channel_state.last_message_id if channel_state else None

    def get_channel(self, channel_id: int) -> dict[str, Any] | None:
        """Return one archived channel metadata row."""
        with self.sync_session() as session:
            channel = session.get(DiscordChannel, channel_id)
            if not channel:
                return None
            return {
                "channel_id": channel.id,
                "guild_id": channel.guild_id,
                "channel_type": channel.channel_type,
                "channel_name": channel.name,
                "topic": channel.topic,
                "updated_at": channel.updated_at,
            }

    def get_turn_anchor(self, channel_id: int) -> int | None:
        with self.sync_session() as session:
            stmt = sa.select(ChannelTurnState).where(ChannelTurnState.channel_id == channel_id)
            turn_state = session.scalars(stmt).one_or_none()
            return turn_state.last_context_message_id if turn_state else None

    def set_turn_anchor(self, channel_id: int, anchor_id: int) -> None:
        # Keep anchor monotonic for numeric Discord IDs so concurrent/out-of-order
        # handlers cannot move the channel delta cursor backwards.
        with self.sync_session() as session:
            # get current anchor if it exists
            stmt = sa.select(ChannelTurnState).where(ChannelTurnState.channel_id == channel_id)
            turn_state = session.scalars(stmt).one_or_none()
            current_anchor = turn_state.last_context_message_id if turn_state else None
            if current_anchor:
                turn_state.last_context_message_id = max(anchor_id, current_anchor)
            else:
                turn_state = ChannelTurnState(
                    channel_id=channel_id,
                    last_context_message_id=anchor_id,
                )
                session.add(turn_state)
            session.commit()

    def clear_turn_anchor(self, channel_id: int) -> None:
        """Clear the turn anchor for a channel (used by /new and /reset)."""
        with self.sync_session() as session:
            stmt = sa.delete(ChannelTurnState).where(ChannelTurnState.channel_id == channel_id)
            session.execute(stmt)
            session.commit()

    def get_message(self, channel_id: int, message_id: int) -> dict[str, Any] | None:
        """Return one archived message row for a channel/message pair."""
        with self.sync_session() as session:
            stmt = sa.select(DiscordMessage).where(
                DiscordMessage.channel_id == channel_id, DiscordMessage.message_id == message_id
            )
            message = session.scalars(stmt).one_or_none()
            return self._message_to_dict(message) if message else None

    def get_reply_context(
        self,
        *,
        reply_id: int,
        channel_id: int | None = None,
    ) -> dict[str, Any] | None:
        """Return author + preview fields for one reply target message."""
        if not reply_id:
            return None
        with self.sync_session() as session:
            if channel_id:
                stmt = sa.select(DiscordMessage).where(
                    DiscordMessage.channel_id == channel_id,
                    DiscordMessage.message_id == reply_id,
                )
                message: DiscordMessage | None = session.scalars(stmt).one_or_none()
            else:
                stmt = sa.select(DiscordMessage).where(DiscordMessage.message_id == reply_id)
                message: DiscordMessage | None = session.scalars(stmt).one_or_none()

            if not message:
                return None

            author_display = message.author_display or message.author_name or message.author_id or "Unknown"

            return {
                "message_id": message.message_id,
                "author_id": message.author_id,
                "author_name": message.author_name,
                "author_display": author_display,
                "author_is_bot": message.author_is_bot,
                "deleted": message.deleted,
                "preview": self._reply_preview(message.content, message.deleted),
            }

    def get_thread_seed(self, thread_id: int) -> dict[str, Any] | None:
        """Return persisted seed context for one thread."""
        if not thread_id:
            return None
        with self.sync_session() as session:
            seed = session.get(ThreadContextSeed, thread_id)
            if not seed:
                return None
            return {
                "thread_id": seed.thread_id,
                "guild_id": seed.guild_id,
                "parent_channel_id": seed.parent_channel_id,
                "anchor_message_id": seed.anchor_message_id,
                "seed_text": seed.seed_text or "",
                "seed_kind": seed.seed_kind or "",
                "created_at": seed.created_at,
                "updated_at": seed.updated_at,
            }

    def upsert_thread_seed(
        self,
        *,
        thread_id: int,
        seed_text: str,
        seed_kind: str,
        guild_id: int | None = None,
        parent_channel_id: int | None = None,
        anchor_message_id: int | None = None,
    ) -> None:
        """Persist one thread seed payload."""
        now = datetime.now(tz=UTC)
        with self.sync_session() as session:
            context_seed = session.get(ThreadContextSeed, thread_id)
            if not context_seed:
                context_seed = ThreadContextSeed(
                    thread_id=thread_id,
                    guild_id=guild_id,
                    parent_channel_id=parent_channel_id,
                    anchor_message_id=anchor_message_id,
                    seed_text=seed_text,
                    seed_kind=seed_kind or "unanchored",
                    created_at=now,
                    updated_at=now,
                )
                session.add(context_seed)
            else:
                context_seed.guild_id = guild_id
                context_seed.parent_channel_id = parent_channel_id
                context_seed.anchor_message_id = anchor_message_id
                context_seed.seed_text = seed_text
                context_seed.seed_kind = seed_kind or "unanchored"
                context_seed.updated_at = now

            session.commit()

    def get_thread_scrape_state(self, parent_channel_id: int) -> dict[str, Any]:
        """Return archived-thread scan cursors for one parent channel."""
        if not parent_channel_id:
            return {
                "parent_channel_id": None,
                "public_before_ts": None,
                "private_before_ts": None,
                "updated_at": None,
            }

        with self.sync_session() as session:
            stmt = sa.select(ThreadScrapeState).where(
                ThreadScrapeState.parent_channel_id == parent_channel_id
            )
            scrape_state = session.scalars(stmt).one_or_none()
            if not scrape_state:
                return {
                    "parent_channel_id": parent_channel_id,
                    "public_before_ts": None,
                    "private_before_ts": None,
                    "updated_at": None,
                }

            return {
                "parent_channel_id": scrape_state.parent_channel_id,
                "public_before_ts": scrape_state.public_before_ts,
                "private_before_ts": scrape_state.private_before_ts,
                "updated_at": scrape_state.updated_at,
            }

    def upsert_thread_scrape_state(
        self,
        parent_channel_id: int,
        *,
        public_before_ts: Any = ...,
        private_before_ts: Any = ...,
    ) -> None:
        """
        Upsert archived-thread scan cursors.

        Pass `...` to keep a field unchanged, or `None` to clear/reset a field.
        """
        with self.sync_session() as session:
            stmt = sa.select(ThreadScrapeState).where(
                ThreadScrapeState.parent_channel_id == parent_channel_id
            )
            scrape_state = session.scalars(stmt).one_or_none()
            if not scrape_state:
                scrape_state = ThreadScrapeState(parent_channel_id=parent_channel_id)
                session.add(scrape_state)

            if public_before_ts is not ...:
                scrape_state.public_before_ts = self._coerce_datetime(public_before_ts)
            if private_before_ts is not ...:
                scrape_state.private_before_ts = self._coerce_datetime(private_before_ts)

            scrape_state.updated_at = datetime.now(tz=UTC)
            session.commit()

    def get_oldest_message(self, channel_id: int) -> dict[str, Any] | None:
        """Return the oldest archived message metadata for one channel."""
        with self.sync_session() as session:
            stmt = (
                sa.select(DiscordMessage.message_id, DiscordMessage.created_at)
                .where(DiscordMessage.channel_id == channel_id)
                .order_by(DiscordMessage.created_at.asc())
                .limit(1)
            )
            row = session.execute(stmt).one_or_none()
            if not row:
                return None

            return {
                "message_id": row.message_id,
                "created_at": row.created_at,
            }

    def get_backfill_state(self, channel_id: int) -> dict[str, Any]:
        """Return persisted backward-fill state for one channel."""
        with self.sync_session() as session:
            stmt = sa.select(ChannelBackfillState).where(ChannelBackfillState.channel_id == channel_id)
            backfill_state = session.scalars(stmt).one_or_none()
            if not backfill_state:
                return {
                    "channel_id": channel_id,
                    "oldest_message_id": None,
                    "oldest_created_at": None,
                    "complete": False,
                    "updated_at": None,
                }

            return {
                "channel_id": backfill_state.channel_id,
                "oldest_message_id": backfill_state.oldest_message_id,
                "oldest_created_at": backfill_state.oldest_created_at,
                "complete": backfill_state.complete,
                "updated_at": backfill_state.updated_at,
            }

    def get_backfill_summary(self) -> dict[str, int]:
        """Return aggregate counts for archived channel backfill progress."""
        with self.sync_session() as session:
            total_channels = int(
                session.execute(
                    sa.select(sa.func.count()).select_from(ChannelBackfillState)
                ).scalar_one_or_none()
                or 0
            )
            complete_channels = int(
                session.execute(
                    sa.select(sa.func.count())
                    .select_from(ChannelBackfillState)
                    .where(ChannelBackfillState.complete == True)
                ).scalar_one_or_none()
                or 0
            )
            archived_messages = int(
                session.execute(sa.select(sa.func.count()).select_from(DiscordMessage)).scalar_one_or_none()
                or 0
            )
            enabled_channels = int(
                session.execute(
                    sa.select(sa.func.count(sa.distinct(DiscordMessage.channel_id))).select_from(
                        DiscordMessage
                    )
                ).scalar_one_or_none()
                or 0
            )

        return {
            "tracked_channels": total_channels,
            "complete_channels": complete_channels,
            "incomplete_channels": max(0, total_channels - complete_channels),
            "archived_messages": archived_messages,
            "enabled_channels": enabled_channels,
        }

    def list_backfill_states(
        self,
        *,
        limit: int = 20,
        incomplete_only: bool = False,
    ) -> list[dict[str, Any]]:
        """Return per-channel persisted backfill state, newest-updated first."""
        limit = max(1, min(int(limit), 500))

        message_stats = (
            sa.select(
                DiscordMessage.channel_id.label("channel_id"),
                sa.func.count(DiscordMessage.message_id).label("archived_message_count"),
                sa.func.min(DiscordMessage.created_at).label("earliest_archived_at"),
                sa.func.max(DiscordMessage.created_at).label("latest_archived_at"),
            )
            .group_by(DiscordMessage.channel_id)
            .subquery()
        )

        with self.sync_session() as session:
            stmt = (
                sa.select(
                    ChannelBackfillState,
                    DiscordChannel.name.label("channel_name"),
                    message_stats.c.archived_message_count,
                    message_stats.c.earliest_archived_at,
                    message_stats.c.latest_archived_at,
                )
                .outerjoin(
                    message_stats,
                    ChannelBackfillState.channel_id == message_stats.c.channel_id,
                )
                .outerjoin(
                    DiscordChannel,
                    ChannelBackfillState.channel_id == DiscordChannel.id,
                )
            )
            if incomplete_only:
                stmt = stmt.where(ChannelBackfillState.complete == False)
            stmt = stmt.order_by(
                ChannelBackfillState.complete.asc(),
                ChannelBackfillState.updated_at.desc(),
            ).limit(limit)

            rows = session.execute(stmt).all()

        results: list[dict[str, Any]] = []
        for row in rows:
            state: ChannelBackfillState = row[0]
            results.append(
                {
                    "channel_id": state.channel_id,
                    "channel_name": row.channel_name,
                    "oldest_message_id": state.oldest_message_id,
                    "oldest_created_at": state.oldest_created_at,
                    "complete": state.complete,
                    "updated_at": state.updated_at,
                    "archived_message_count": int(row.archived_message_count or 0),
                    "earliest_archived_at": row.earliest_archived_at,
                    "latest_archived_at": row.latest_archived_at,
                }
            )
        return results

    @staticmethod
    def _is_older_id(candidate: int | None, current: int | None) -> bool:
        """Discord snowflake ordering: lower numeric IDs are older."""
        if candidate is None:
            return False
        if current is None:
            return True
        return candidate < current

    def upsert_backfill_state(
        self,
        channel_id: int,
        *,
        oldest_message_id: int | None = None,
        oldest_created_at: datetime | float | int | None = None,
        complete: bool | None = None,
    ) -> None:
        """
        Upsert backward-fill cursor for a channel.

        - oldest_message_id is kept monotonic (only moves to older IDs).
        - complete can be toggled explicitly when provided.
        """
        with self.sync_session() as session:
            stmt = sa.select(ChannelBackfillState).where(ChannelBackfillState.channel_id == channel_id)
            backfill_state = session.scalars(stmt).one_or_none()
            if not backfill_state:
                backfill_state = ChannelBackfillState(channel_id=channel_id)
                session.add(backfill_state)

            current_oldest = backfill_state.oldest_message_id
            if self._is_older_id(oldest_message_id, current_oldest):
                backfill_state.oldest_message_id = oldest_message_id
                backfill_state.oldest_created_at = self._coerce_datetime(oldest_created_at)

            if complete is not None:
                backfill_state.complete = bool(complete)

            backfill_state.updated_at = datetime.now(tz=UTC)
            session.commit()

    def mark_backfill_complete(self, channel_id: int, complete: bool = True) -> None:
        """Mark backward-fill status for one channel."""
        self.upsert_backfill_state(channel_id, complete=complete)

    def _advance_channel_cursor(
        self,
        session: SyncSessionType,
        channel_id: int,
        message_id: int | None,
        *,
        updated_at: datetime | None = None,
    ) -> None:
        channel_state = session.get(ChannelState, channel_id)
        updated = False
        if not channel_state:
            channel_state = ChannelState(channel_id=channel_id, last_message_id=message_id)
            session.add(channel_state)
            updated = True
        if message_id is not None:
            current_last = channel_state.last_message_id
            channel_state.last_message_id = (
                message_id if current_last is None else max(message_id, current_last)
            )
            updated = True

        if updated:
            channel_state.updated_at = updated_at or datetime.now(tz=UTC)
        session.flush()

    def _upsert_channel_record(
        self,
        session: SyncSessionType,
        *,
        channel_id: int,
        guild_id: int | None,
        channel_name: str | None,
        updated_at: datetime | None = None,
    ) -> None:
        channel = session.get(DiscordChannel, channel_id)
        updated = False
        if not channel:
            channel = DiscordChannel(
                id=channel_id,
                guild_id=guild_id,
                name=channel_name,
            )
            session.add(channel)
            updated = True

        if channel.guild_id != guild_id:
            channel.guild_id = guild_id
            updated = True
        if channel.name != channel_name:
            channel.name = channel_name
            updated = True

        if updated:
            channel.updated_at = updated_at or datetime.now(tz=UTC)
        session.flush()

    def upsert_message(self, incoming: DiscordMessage) -> None:
        """
        Upsert one message row.

        The incoming model is treated as a detached payload object.
        """
        message_id = incoming.message_id
        channel_id = incoming.channel_id
        if message_id is None or channel_id is None:
            return

        created_at = incoming.created_at or datetime.now(tz=UTC)

        with self.sync_session() as session:
            message = session.get(DiscordMessage, message_id)
            if not message:
                message = DiscordMessage(message_id=message_id, created_at=created_at)
                session.add(message)
            elif message.created_at is None:
                message.created_at = created_at

            message.guild_id = incoming.guild_id
            message.guild_name = incoming.guild_name
            message.channel_id = channel_id
            message.channel_name = incoming.channel_name
            message.thread_id = incoming.thread_id
            message.author_id = incoming.author_id
            message.author_name = incoming.author_name
            message.author_display = incoming.author_display
            message.author_is_bot = incoming.author_is_bot
            message.content = incoming.content
            message.attachments_json = incoming.attachments_json
            if incoming.reactions_json is not None:
                message.reactions_json = incoming.reactions_json
            message.reply_to_message_id = incoming.reply_to_message_id
            message.reply_to_channel_id = incoming.reply_to_channel_id
            message.reply_to_guild_id = incoming.reply_to_guild_id
            message.edited_at = incoming.edited_at
            message.deleted = incoming.deleted
            self._upsert_channel_record(
                session,
                channel_id=channel_id,
                guild_id=incoming.guild_id,
                channel_name=incoming.channel_name,
            )
            self._advance_channel_cursor(session, channel_id, message_id)

            session.commit()

    @staticmethod
    def _normalize_content(content: str | None) -> str:
        return " ".join((content or "").split()).strip()

    @staticmethod
    def _reply_preview(content: str | None, deleted: bool) -> str:
        if deleted:
            return "[Deleted]"
        normalized = " ".join((content or "").split()).strip()
        if not normalized:
            return "[non-text message]"
        return normalized[:50]

    def record_message_edit(self, *, before: DiscordMessage, after: DiscordMessage) -> None:
        """
        Record one edit event as `before -> after`.

        No-op content edits are ignored to avoid noisy context payloads.
        """
        message_id = after.message_id
        channel_id = after.channel_id
        if message_id is None or channel_id is None:
            return

        before_norm = self._normalize_content(before.content)
        after_norm = self._normalize_content(after.content)
        if before_norm == after_norm:
            return

        change = MessageChange(
            message_id=message_id,
            guild_id=after.guild_id,
            channel_id=channel_id,
            author_id=after.author_id,
            author_name=after.author_name,
            author_display=after.author_display,
            author_is_bot=bool(after.author_is_bot),
            original_created_at=before.created_at or after.created_at,
            changed_at=after.edited_at or datetime.now(tz=UTC),
            change_type="edit",
            before_content=before.content,
            after_content=after.content,
        )
        with self.sync_session() as session:
            session.add(change)
            session.commit()

    def record_reaction_change(
        self,
        *,
        message_id: int,
        channel_id: int,
        change_type: str,
        changed_at: datetime | float | int | None = None,
        guild_id: int | None = None,
        author_id: int | None = None,
        author_name: str | None = None,
        author_display: str | None = None,
        author_is_bot: bool = False,
        original_created_at: datetime | float | int | None = None,
        emoji_key: str | None = None,
        emoji_name: str | None = None,
        emoji_display: str | None = None,
        emoji_id: int | None = None,
        message_author_id: int | None = None,
        message_author_name: str | None = None,
        message_author_display: str | None = None,
    ) -> None:
        """Record one reaction add/remove/clear event."""
        change_type = str(change_type or "").strip().lower()
        if not change_type:
            return

        change = ReactionChange(
            message_id=message_id,
            guild_id=guild_id,
            channel_id=channel_id,
            author_id=author_id,
            author_name=author_name,
            author_display=author_display,
            author_is_bot=author_is_bot,
            original_created_at=self._coerce_datetime(original_created_at),
            changed_at=self._coerce_datetime(changed_at) or datetime.now(tz=UTC),
            change_type=change_type,
            emoji_key=emoji_key,
            emoji_name=emoji_name,
            emoji_display=emoji_display,
            emoji_id=emoji_id,
            message_author_id=message_author_id,
            message_author_name=message_author_name,
            message_author_display=message_author_display,
        )
        with self.sync_session() as session:
            session.add(change)
            session.commit()

    def mark_deleted(
        self,
        message_id: int,
        channel_id: int | None = None,
        guild_id: int | None = None,
    ) -> None:
        """Mark a message as deleted, inserting a tombstone if needed."""
        now = datetime.now(tz=UTC)
        resolved_channel = channel_id
        resolved_guild = guild_id
        with self.sync_session() as session:
            message = session.get(DiscordMessage, message_id)
            if message:
                resolved_channel = resolved_channel or message.channel_id
                resolved_guild = resolved_guild or message.guild_id
                if not message.deleted:
                    session.add(
                        MessageChange(
                            message_id=message_id,
                            guild_id=resolved_guild,
                            channel_id=resolved_channel,
                            author_id=message.author_id,
                            author_name=message.author_name,
                            author_display=message.author_display,
                            author_is_bot=message.author_is_bot,
                            original_created_at=message.created_at,
                            changed_at=now,
                            change_type="delete",
                            before_content=message.content,
                            after_content="[Deleted]",
                        )
                    )
                message.deleted = True
                if message.edited_at is None:
                    message.edited_at = now
            else:
                if resolved_channel is None:
                    return
                session.add(
                    MessageChange(
                        message_id=message_id,
                        guild_id=resolved_guild,
                        channel_id=resolved_channel,
                        original_created_at=None,
                        changed_at=now,
                        change_type="delete",
                        before_content=None,
                        after_content="[Deleted]",
                    )
                )
                session.add(
                    DiscordMessage(
                        message_id=message_id,
                        guild_id=resolved_guild,
                        channel_id=resolved_channel,
                        content=None,
                        created_at=now,
                        edited_at=now,
                        deleted=True,
                    )
                )

            if resolved_channel is not None:
                self._advance_channel_cursor(session, resolved_channel, message_id, updated_at=now)
            session.commit()

    def _message_to_dict(self, row: Any) -> dict[str, Any]:
        attachments: list[dict[str, Any]] = []
        raw_attachments = self._row_get(row, "attachments_json")
        if raw_attachments:
            try:
                if isinstance(raw_attachments, str):
                    decoded = json.loads(raw_attachments)
                else:
                    decoded = raw_attachments
                if isinstance(decoded, list):
                    attachments = [att for att in decoded if isinstance(att, dict)]
            except Exception:
                attachments = []

        reactions: list[dict[str, Any]] = []
        raw_reactions = self._row_get(row, "reactions_json") if self._row_has(row, "reactions_json") else None
        if raw_reactions:
            try:
                if isinstance(raw_reactions, str):
                    decoded = json.loads(raw_reactions)
                else:
                    decoded = raw_reactions
                if isinstance(decoded, list):
                    reactions = [entry for entry in decoded if isinstance(entry, dict)]
            except Exception:
                reactions = []

        return {
            "message_id": self._row_get(row, "message_id"),
            "guild_id": self._row_get(row, "guild_id"),
            "guild_name": self._row_get(row, "guild_name"),
            "channel_id": self._row_get(row, "channel_id"),
            "channel_name": self._row_get(row, "channel_name"),
            "thread_id": self._row_get(row, "thread_id"),
            "author_id": self._row_get(row, "author_id"),
            "author_name": self._row_get(row, "author_name"),
            "author_display": self._row_get(row, "author_display"),
            "author_is_bot": bool(self._row_get(row, "author_is_bot")),
            "content": self._row_get(row, "content") or "",
            "attachments_json": raw_attachments,
            "attachments": attachments,
            "reactions_json": raw_reactions,
            "reactions": reactions,
            "reply_to_message_id": (
                self._row_get(row, "reply_to_message_id")
                if self._row_has(row, "reply_to_message_id")
                else None
            ),
            "reply_to_channel_id": (
                self._row_get(row, "reply_to_channel_id")
                if self._row_has(row, "reply_to_channel_id")
                else None
            ),
            "reply_to_guild_id": (
                self._row_get(row, "reply_to_guild_id") if self._row_has(row, "reply_to_guild_id") else None
            ),
            "created_at": self._row_get(row, "created_at"),
            "edited_at": self._row_get(row, "edited_at"),
            "deleted": bool(self._row_get(row, "deleted")),
        }

    def enrich_reply_context_rows(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Attach reply target author/preview fields for context rendering."""
        if not rows:
            return rows

        ordered_ids: list[int] = []
        seen_ids: set[int] = set()
        for row in rows:
            reply_id = row.get("reply_to_message_id")
            if reply_id is None or reply_id in seen_ids:
                continue
            seen_ids.add(reply_id)
            ordered_ids.append(reply_id)

        if not ordered_ids:
            return rows

        with self.sync_session() as session:
            stmt = sa.select(
                DiscordMessage.message_id,
                DiscordMessage.author_id,
                DiscordMessage.author_name,
                DiscordMessage.author_display,
                DiscordMessage.content,
                DiscordMessage.deleted,
            ).where(DiscordMessage.message_id.in_(ordered_ids))
            parent_rows = {
                row.message_id: {
                    "author_display": row.author_display,
                    "author_name": row.author_name,
                    "author_id": row.author_id,
                    "content": row.content,
                    "deleted": row.deleted,
                }
                for row in session.execute(stmt)
            }

        enriched_rows: list[dict[str, Any]] = []
        for row in rows:
            enriched = dict(row)
            reply_id = row.get("reply_to_message_id")
            if reply_id is not None:
                parent = parent_rows.get(reply_id)
                if parent:
                    enriched["reply_author_display"] = (
                        parent.get("author_display")
                        or parent.get("author_name")
                        or parent.get("author_id")
                        or "unknown"
                    )
                    enriched["reply_preview"] = self._reply_preview(
                        parent.get("content"),
                        bool(parent.get("deleted")),
                    )
            enriched_rows.append(enriched)
        return enriched_rows

    def _change_row_to_dict(self, row: Any) -> dict[str, Any]:
        payload = {
            "id": self._row_get(row, "id"),
            "message_id": self._row_get(row, "message_id"),
            "guild_id": self._row_get(row, "guild_id"),
            "channel_id": self._row_get(row, "channel_id"),
            "author_id": self._row_get(row, "author_id"),
            "author_name": self._row_get(row, "author_name"),
            "author_display": self._row_get(row, "author_display"),
            "author_is_bot": bool(self._row_get(row, "author_is_bot")),
            "original_created_at": self._row_get(row, "original_created_at"),
            "changed_at": self._row_get(row, "changed_at"),
            "change_type": self._row_get(row, "change_type"),
            "before_content": self._row_get(row, "before_content") or "",
            "after_content": self._row_get(row, "after_content") or "",
        }
        if self._row_has(row, "emoji_key"):
            payload["emoji_key"] = self._row_get(row, "emoji_key") or ""
            payload["emoji_name"] = self._row_get(row, "emoji_name") or ""
            payload["emoji_display"] = self._row_get(row, "emoji_display") or ""
            payload["emoji_id"] = self._row_get(row, "emoji_id") or ""
        if self._row_has(row, "message_author_id"):
            payload["message_author_id"] = self._row_get(row, "message_author_id") or ""
            payload["message_author_name"] = self._row_get(row, "message_author_name") or ""
            payload["message_author_display"] = self._row_get(row, "message_author_display") or ""
        return payload

    @staticmethod
    def _normalize_channel_ids(
        channel_id: int | list[int] | None,
    ) -> tuple[list[int] | None, bool]:
        if channel_id is None:
            return None, False
        if isinstance(channel_id, list):
            seen_ids: set[int] = set()
            normalized: list[int] = []
            for entry in channel_id:
                if entry in seen_ids:
                    continue
                seen_ids.add(entry)
                normalized.append(entry)
            return normalized, len(normalized) == 0
        return [channel_id], False

    def _build_recent_messages_stmt(
        self,
        *,
        channel_id: int | list[int] | None,
        limit: int,
        include_bots: bool,
        max_age: timedelta | None,
    ) -> tuple[sa.Select[Any], bool]:
        limit = max(1, min(int(limit), 500))
        normalized_channel_ids, explicit_empty = self._normalize_channel_ids(channel_id)

        stmt = sa.select(DiscordMessage).where(DiscordMessage.deleted == False)
        if normalized_channel_ids is not None:
            stmt = stmt.where(DiscordMessage.channel_id.in_(normalized_channel_ids))
        if include_bots is False:
            stmt = stmt.where(DiscordMessage.author_is_bot == False)
        if max_age is not None:
            cutoff = datetime.now(tz=UTC) - max_age
            stmt = stmt.where(DiscordMessage.created_at >= cutoff)
        stmt = stmt.order_by(DiscordMessage.created_at.desc()).limit(limit)
        return stmt, explicit_empty

    def list_recent_messages(
        self,
        channel_id: int | list[int] | None = None,
        limit: int = 20,
        include_bots: bool = True,
        max_age: timedelta | None = None,
    ) -> list[dict[str, Any]]:
        """Return recent messages in chronological order."""
        stmt, explicit_empty = self._build_recent_messages_stmt(
            channel_id=channel_id,
            limit=limit,
            include_bots=include_bots,
            max_age=max_age,
        )
        if explicit_empty:
            return []

        with self.sync_session() as session:
            rows = list(session.scalars(stmt))

        out = [self._message_to_dict(row) for row in rows]
        out.reverse()
        return out

    async def list_recent_messages_async(
        self,
        channel_id: int | list[int] | None = None,
        limit: int = 20,
        include_bots: bool = True,
        max_age: timedelta | None = None,
    ) -> list[dict[str, Any]]:
        """Async version of list_recent_messages."""
        stmt, explicit_empty = self._build_recent_messages_stmt(
            channel_id=channel_id,
            limit=limit,
            include_bots=include_bots,
            max_age=max_age,
        )
        if explicit_empty:
            return []

        async with AsyncSession() as session:
            rows = list(await session.scalars(stmt))

        out = [self._message_to_dict(row) for row in rows]
        out.reverse()
        return out

    def list_messages_after(
        self,
        channel_id: int,
        after_message_id: int | None,
        limit: int = 200,
        include_bots: bool = False,
    ) -> list[dict[str, Any]]:
        """Return messages in a channel after a message ID, oldest first."""
        limit = max(1, min(limit, 1000))
        if after_message_id is None:
            return self.list_recent_messages(
                channel_id=channel_id,
                limit=limit,
                include_bots=include_bots,
            )

        with self.sync_session() as session:
            stmt = sa.select(DiscordMessage).where(
                DiscordMessage.channel_id == channel_id,
                DiscordMessage.deleted == False,
                DiscordMessage.message_id > after_message_id,
            )
            if include_bots is False:
                stmt = stmt.where(DiscordMessage.author_is_bot == False)
            stmt = stmt.order_by(DiscordMessage.created_at.asc()).limit(limit)
            rows = list(session.scalars(stmt))

        return [self._message_to_dict(row) for row in rows]

    def list_changes_since_anchor(
        self,
        channel_id: int,
        anchor_message_id: int,
        limit: int = 200,
        include_bots: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Return message edit/delete events that happened after the anchor message time.

        This catches edits/deletes to older messages that would otherwise be invisible
        when filtering strictly by message_id.
        """
        limit = max(1, min(int(limit), 1000))
        with self.sync_session() as session:
            anchor_created_at = session.scalar(
                sa.select(DiscordMessage.created_at).where(
                    DiscordMessage.channel_id == channel_id,
                    DiscordMessage.message_id == anchor_message_id,
                )
            )
            if anchor_created_at is None:
                return []

            stmt = sa.select(MessageChange).where(
                MessageChange.channel_id == channel_id,
                MessageChange.changed_at > anchor_created_at,
            )
            if include_bots is False:
                stmt = stmt.where(MessageChange.author_is_bot == False)
            stmt = stmt.order_by(MessageChange.changed_at.asc()).limit(limit)
            rows = list(session.scalars(stmt))

        return [self._change_row_to_dict(row) for row in rows]

    def list_reaction_changes_since_anchor(
        self,
        channel_id: int,
        anchor_message_id: int,
        limit: int = 200,
        include_bots: bool = False,
    ) -> list[dict[str, Any]]:
        """
        Return reaction events that happened after the anchor message time.

        This catches reactions on older messages that would otherwise be invisible
        in delta-only context windows.
        """
        limit = max(1, min(int(limit), 1000))
        with self.sync_session() as session:
            anchor_created_at = session.scalar(
                sa.select(DiscordMessage.created_at).where(
                    DiscordMessage.channel_id == channel_id,
                    DiscordMessage.message_id == anchor_message_id,
                )
            )
            if anchor_created_at is None:
                return []

            stmt = sa.select(ReactionChange).where(
                ReactionChange.channel_id == channel_id,
                ReactionChange.changed_at > anchor_created_at,
            )
            if include_bots is False:
                stmt = stmt.where(ReactionChange.author_is_bot == False)
            stmt = stmt.order_by(ReactionChange.changed_at.asc()).limit(limit)
            rows = list(session.scalars(stmt))

        reaction_rows = [self._change_row_to_dict(row) for row in rows]
        for row in reaction_rows:
            row["before_content"] = ""
            row["after_content"] = ""
        return reaction_rows

    def list_all_changes_since_anchor(
        self,
        channel_id: int,
        anchor_message_id: int,
        limit: int = 200,
        include_bots: bool = False,
    ) -> list[dict[str, Any]]:
        """Return message and reaction changes ordered chronologically."""
        limit = max(1, min(int(limit), 1000))
        changes = self.list_changes_since_anchor(
            channel_id=channel_id,
            anchor_message_id=anchor_message_id,
            limit=limit,
            include_bots=include_bots,
        )
        reaction_changes = self.list_reaction_changes_since_anchor(
            channel_id=channel_id,
            anchor_message_id=anchor_message_id,
            limit=limit,
            include_bots=include_bots,
        )
        merged = changes + reaction_changes
        merged.sort(
            key=lambda row: (
                self._coerce_datetime(row.get("changed_at")) or datetime.min.replace(tzinfo=UTC),
                int(row.get("id") or 0),
            )
        )
        return merged[:limit]

    def count_new_non_bot_messages(
        self,
        channel_id: int,
        after_message_id: int | None,
    ) -> int:
        """Count non-bot, non-deleted messages after the anchor message ID."""
        if after_message_id is None:
            return 0

        with self.sync_session() as session:
            stmt = sa.select(sa.func.count()).where(
                DiscordMessage.channel_id == channel_id,
                DiscordMessage.deleted == False,
                DiscordMessage.author_is_bot == False,
                DiscordMessage.message_id > after_message_id,
            )
            return int(session.scalar(stmt) or 0)

    def _neighbors(
        self,
        channel_id: int,
        created_at: datetime,
        around: int,
    ) -> dict[str, list[dict[str, Any]]]:
        if around <= 0:
            return {"before": [], "after": []}

        with self.sync_session() as session:
            before_stmt = (
                sa.select(DiscordMessage)
                .where(
                    DiscordMessage.channel_id == channel_id,
                    DiscordMessage.deleted == False,
                    DiscordMessage.created_at < created_at,
                )
                .order_by(DiscordMessage.created_at.desc())
                .limit(around)
            )
            after_stmt = (
                sa.select(DiscordMessage)
                .where(
                    DiscordMessage.channel_id == channel_id,
                    DiscordMessage.deleted == False,
                    DiscordMessage.created_at > created_at,
                )
                .order_by(DiscordMessage.created_at.asc())
                .limit(around)
            )
            before_rows = list(session.scalars(before_stmt))
            after_rows = list(session.scalars(after_stmt))

        before = [self._message_to_dict(row) for row in before_rows]
        before.reverse()
        after = [self._message_to_dict(row) for row in after_rows]
        return {"before": before, "after": after}

    @staticmethod
    def _search_snippet(content: str | None, query: str) -> str:
        text = " ".join((content or "").split()).strip()
        if not text:
            return "[non-text message]"

        terms = [term for term in re.findall(r"\w+", query) if term]
        if not terms:
            return text[:120]

        lowered = text.casefold()
        positions = [lowered.find(term.casefold()) for term in terms]
        positions = [pos for pos in positions if pos >= 0]
        if not positions:
            return text[:120]

        start = max(0, min(positions) - 30)
        end = min(len(text), start + 120)
        snippet = text[start:end].strip()
        if start > 0:
            snippet = "..." + snippet
        if end < len(text):
            snippet = snippet + "..."
        return snippet

    def _search_messages_fallback(
        self,
        *,
        query: str,
        guild_id: int | None,
        channel_id: int | None,
        since_ts: datetime | None,
        until_ts: datetime | None,
        limit: int,
        around: int,
    ) -> list[dict[str, Any]]:
        with self.sync_session() as session:
            stmt = sa.select(DiscordMessage).where(
                DiscordMessage.deleted == False,
                DiscordMessage.content.is_not(None),
                DiscordMessage.content.ilike(f"%{query}%"),
            )
            if guild_id is not None:
                stmt = stmt.where(DiscordMessage.guild_id == guild_id)
            if channel_id is not None:
                stmt = stmt.where(DiscordMessage.channel_id == channel_id)
            if since_ts is not None:
                stmt = stmt.where(DiscordMessage.created_at >= since_ts)
            if until_ts is not None:
                stmt = stmt.where(DiscordMessage.created_at <= until_ts)
            stmt = stmt.order_by(DiscordMessage.created_at.desc()).limit(limit)
            rows = list(session.scalars(stmt))

        rows.reverse()
        results = []
        for row in rows:
            hit = self._message_to_dict(row)
            hit["snippet"] = self._search_snippet(hit.get("content"), query)
            created_at = self._coerce_datetime(hit["created_at"])
            ctx = (
                self._neighbors(hit["channel_id"], created_at, around)
                if created_at
                else {"before": [], "after": []}
            )
            results.append(
                {
                    "hit": hit,
                    "context_before": ctx["before"],
                    "context_after": ctx["after"],
                }
            )
        return results

    def search_messages(
        self,
        query: str,
        guild_id: int | None = None,
        channel_id: int | None = None,
        since_ts: datetime | float | int | None = None,
        until_ts: datetime | float | int | None = None,
        limit: int = 5,
        around: int = 1,
    ) -> list[dict[str, Any]]:
        """Search archived Discord messages with backend-specific FTS where available."""
        query = (query or "").strip()
        if not query:
            return []

        limit = max(1, min(int(limit), 200))
        around = max(0, min(int(around), 10))
        since_dt = self._coerce_datetime(since_ts)
        until_dt = self._coerce_datetime(until_ts)
        dialect_name = self._sync_engine.dialect.name

        if dialect_name == "postgresql":
            with self.sync_session() as session:
                ts_vector = sa.func.to_tsvector("english", sa.func.coalesce(DiscordMessage.content, ""))
                ts_query = sa.func.websearch_to_tsquery("english", query)
                rank = sa.func.ts_rank_cd(ts_vector, ts_query)
                stmt = sa.select(
                    DiscordMessage,
                    sa.func.ts_headline(
                        "english",
                        sa.func.coalesce(DiscordMessage.content, ""),
                        ts_query,
                    ).label("snippet"),
                    rank.label("rank"),
                ).where(
                    DiscordMessage.deleted == False,
                    ts_vector.op("@@")(ts_query),
                )
                if guild_id:
                    stmt = stmt.where(DiscordMessage.guild_id == guild_id)
                if channel_id:
                    stmt = stmt.where(DiscordMessage.channel_id == channel_id)
                if since_dt is not None:
                    stmt = stmt.where(DiscordMessage.created_at >= since_dt)
                if until_dt is not None:
                    stmt = stmt.where(DiscordMessage.created_at <= until_dt)
                stmt = stmt.order_by(rank.desc(), DiscordMessage.created_at.desc()).limit(limit)
                rows = session.execute(stmt).all()

            results = []
            for row in rows:
                hit = self._message_to_dict(row[0])
                hit["snippet"] = row.snippet or self._search_snippet(hit.get("content"), query)
                created_at = self._coerce_datetime(hit["created_at"])
                ctx = (
                    self._neighbors(hit["channel_id"], created_at, around)
                    if created_at
                    else {"before": [], "after": []}
                )
                results.append(
                    {
                        "hit": hit,
                        "context_before": ctx["before"],
                        "context_after": ctx["after"],
                    }
                )
            return results

        where = ["discord_messages_fts MATCH ?", "m.deleted = 0"]
        params: list[Any] = [query]

        if guild_id:
            where.append("m.guild_id = ?")
            params.append(guild_id)
        if channel_id:
            where.append("m.channel_id = ?")
            params.append(channel_id)
        if since_dt is not None:
            where.append("m.created_at >= ?")
            params.append(since_dt)
        if until_dt is not None:
            where.append("m.created_at <= ?")
            params.append(until_dt)

        params.append(limit)
        sql = f"""
            SELECT
                m.*,
                snippet(discord_messages_fts, 0, '>>>', '<<<', '...', 28) AS snippet
            FROM discord_messages_fts
            JOIN messages m ON m.rowid = discord_messages_fts.rowid
            WHERE {" AND ".join(where)}
            ORDER BY rank
            LIMIT ?
        """
        try:
            with self.sync_session() as session:
                rows = session.connection().exec_driver_sql(sql, tuple(params)).fetchall()

            results = []
            for row in rows:
                hit = self._message_to_dict(row)
                hit["snippet"] = self._row_get(row, "snippet")
                created_at = self._coerce_datetime(hit["created_at"])
                ctx = (
                    self._neighbors(hit["channel_id"], created_at, around)
                    if created_at
                    else {"before": [], "after": []}
                )
                results.append(
                    {
                        "hit": hit,
                        "context_before": ctx["before"],
                        "context_after": ctx["after"],
                    }
                )
            return results
        except Exception as exc:
            if "discord_messages_fts" not in str(exc):
                raise
            return self._search_messages_fallback(
                query=query,
                guild_id=guild_id,
                channel_id=channel_id,
                since_ts=since_dt,
                until_ts=until_dt,
                limit=limit,
                around=around,
            )


def create_all_tables():
    """Utility function to create all tables in the database."""
    engine = get_sync_engine()
    Base.metadata.create_all(engine)
    if engine.dialect.name == "postgresql":
        with engine.begin() as conn:
            conn.exec_driver_sql(
                """
                CREATE INDEX IF NOT EXISTS idx_messages_content_fts
                ON messages
                USING gin (to_tsvector('english', coalesce(content, '')))
                """
            )


if __name__ == "__main__":
    create_all_tables()
