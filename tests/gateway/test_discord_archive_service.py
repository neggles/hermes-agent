import asyncio
import sys
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import Session as OrmSession
from sqlalchemy.orm import sessionmaker

import gateway.platforms.discord_db as discord_db_mod
from gateway.config import PlatformConfig
from gateway.platforms.discord_archive import DiscordArchiveDB, DiscordMessage, MessageChange
from gateway.platforms.discord_archive_service import DiscordArchiveService


def _clear_discord_db_caches() -> None:
    for getter_name in ("get_async_session_factory", "get_async_engine", "get_sync_engine"):
        getter = getattr(discord_db_mod, getter_name, None)
        if getter is None:
            continue
        try:
            engine = getter()
        except Exception:
            engine = None
        if engine is not None and hasattr(engine, "dispose"):
            try:
                engine.dispose()
            except Exception:
                pass
        cache_clear = getattr(getter, "cache_clear", None)
        if callable(cache_clear):
            cache_clear()


@pytest.fixture
def isolated_discord_db(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.delenv("HERMES_DISCORD_DB_URL", raising=False)
    for env_name in (
        "DISCORD_ARCHIVE_ENABLED",
        "DISCORD_ARCHIVE_INCLUDE_DMS",
        "DISCORD_ARCHIVE_ALLOWED_GUILDS",
        "DISCORD_ARCHIVE_ALLOWED_CHANNELS",
        "DISCORD_ARCHIVE_FRONTFILL_ENABLED",
        "DISCORD_ARCHIVE_FRONTFILL_INTERVAL_SEC",
        "DISCORD_ARCHIVE_FRONTFILL_MAX_CHANNELS_PER_TICK",
        "DISCORD_ARCHIVE_FRONTFILL_SEED_LIMIT",
        "DISCORD_ARCHIVE_FRONTFILL_MAX_PAGES_PER_CHANNEL",
        "DISCORD_ARCHIVE_BACKFILL_ENABLED",
        "DISCORD_ARCHIVE_BACKFILL_INTERVAL_SEC",
        "DISCORD_ARCHIVE_BACKFILL_MAX_CHANNELS_PER_TICK",
        "DISCORD_ARCHIVE_BACKFILL_MAX_PAGES_PER_CHANNEL",
        "DISCORD_ENABLE_DMS",
    ):
        monkeypatch.delenv(env_name, raising=False)
    _clear_discord_db_caches()
    yield
    _clear_discord_db_caches()


class FakePermissions:
    def __init__(self, *, view_channel: bool = True, read_messages: bool = True, read_message_history: bool = True):
        self.view_channel = view_channel
        self.read_messages = read_messages
        self.read_message_history = read_message_history


class FakeGuild:
    def __init__(self, guild_id: int, name: str = "Guild", *, text_channels=None, threads=None):
        self.id = guild_id
        self.name = name
        self.me = SimpleNamespace(id=999)
        self.text_channels = list(text_channels or [])
        self.threads = list(threads or [])


class FakeTextChannel:
    def __init__(
        self,
        channel_id: int,
        *,
        guild: FakeGuild | None = None,
        name: str = "general",
        messages: list[Any] | None = None,
        category_id: int | None = None,
    ):
        self.id = channel_id
        self.guild = guild
        self.name = name
        self.category_id = category_id
        self.parent_id = None
        self.parent = None
        self.category = SimpleNamespace(id=category_id) if category_id is not None else None
        self._messages = list(messages or [])

    def permissions_for(self, _member):
        return FakePermissions()

    async def history(self, *, limit: int = 100, oldest_first: bool = False, after=None, before=None):
        rows = list(self._messages)
        if after is not None and getattr(after, "id", None) is not None:
            rows = [row for row in rows if row.id > after.id]
        if before is not None and getattr(before, "id", None) is not None:
            rows = [row for row in rows if row.id < before.id]
        rows.sort(key=lambda row: row.id, reverse=not oldest_first)
        for row in rows[:limit]:
            yield row


class FakeDMChannel:
    def __init__(self, channel_id: int, *, messages: list[Any] | None = None):
        self.id = channel_id
        self.name = "dm"
        self.guild = None
        self.parent = None
        self.parent_id = None
        self.category = None
        self.category_id = None
        self._messages = list(messages or [])

    async def history(self, *, limit: int = 100, oldest_first: bool = False, after=None, before=None):
        rows = list(self._messages)
        if after is not None and getattr(after, "id", None) is not None:
            rows = [row for row in rows if row.id > after.id]
        if before is not None and getattr(before, "id", None) is not None:
            rows = [row for row in rows if row.id < before.id]
        rows.sort(key=lambda row: row.id, reverse=not oldest_first)
        for row in rows[:limit]:
            yield row


class FakeThread:
    def __init__(
        self,
        channel_id: int,
        *,
        guild: FakeGuild,
        parent: FakeTextChannel,
        name: str = "thread",
        messages: list[Any] | None = None,
    ):
        self.id = channel_id
        self.guild = guild
        self.parent = parent
        self.parent_id = parent.id
        self.name = name
        self.category = getattr(parent, "category", None)
        self.category_id = getattr(parent, "category_id", None)
        self._messages = list(messages or [])

    def permissions_for(self, _member):
        return FakePermissions()

    async def history(self, *, limit: int = 100, oldest_first: bool = False, after=None, before=None):
        rows = list(self._messages)
        if after is not None and getattr(after, "id", None) is not None:
            rows = [row for row in rows if row.id > after.id]
        if before is not None and getattr(before, "id", None) is not None:
            rows = [row for row in rows if row.id < before.id]
        rows.sort(key=lambda row: row.id, reverse=not oldest_first)
        for row in rows[:limit]:
            yield row


class FakeAttachment:
    def __init__(self, attachment_id: int, filename: str, *, url: str = "https://example.test/file", content_type: str = "text/plain", size: int = 12):
        self.id = attachment_id
        self.filename = filename
        self.url = url
        self.content_type = content_type
        self.size = size


class FakeAuthor:
    def __init__(self, user_id: int, *, name: str = "user", display_name: str | None = None, bot: bool = False):
        self.id = user_id
        self.name = name
        self.display_name = display_name or name
        self.bot = bot


class FakeReference:
    def __init__(self, message_id: int, *, channel_id: int | None = None, guild_id: int | None = None):
        self.message_id = message_id
        self.channel_id = channel_id
        self.guild_id = guild_id


class FakeMessage:
    def __init__(
        self,
        message_id: int,
        *,
        channel: Any,
        author: Any,
        content: str = "hello",
        created_at: datetime | None = None,
        edited_at: datetime | None = None,
        attachments: list[Any] | None = None,
        reference: Any = None,
        message_type: str = "default",
        mentions: list[Any] | None = None,
    ):
        self.id = message_id
        self.channel = channel
        self.author = author
        self.content = content
        self.created_at = created_at or datetime.now(tz=UTC)
        self.edited_at = edited_at
        self.attachments = list(attachments or [])
        self.reference = reference
        self.type = message_type
        self.mentions = list(mentions or [])


def archived_message(
    *,
    message_id: int,
    channel_id: int,
    created_at: datetime,
    guild_id: int | None = None,
    guild_name: str | None = None,
    channel_name: str | None = None,
    thread_id: int | None = None,
    author_id: int | None = None,
    author_name: str | None = None,
    author_display: str | None = None,
    author_is_bot: bool = False,
    content: str | None = None,
    attachments_json: list[dict[str, Any]] | None = None,
    reactions_json: list[dict[str, Any]] | None = None,
    reply_to_message_id: int | None = None,
    reply_to_channel_id: int | None = None,
    reply_to_guild_id: int | None = None,
    edited_at: datetime | None = None,
    deleted: bool = False,
) -> DiscordMessage:
    return DiscordMessage(
        message_id=message_id,
        guild_id=guild_id,
        guild_name=guild_name,
        channel_id=channel_id,
        channel_name=channel_name,
        thread_id=thread_id,
        author_id=author_id,
        author_name=author_name,
        author_display=author_display,
        author_is_bot=author_is_bot,
        content=content,
        attachments_json=attachments_json or [],
        reactions_json=reactions_json,
        reply_to_message_id=reply_to_message_id,
        reply_to_channel_id=reply_to_channel_id,
        reply_to_guild_id=reply_to_guild_id,
        created_at=created_at,
        edited_at=edited_at,
        deleted=deleted,
    )


@pytest.mark.asyncio
async def test_archive_service_channel_eligibility_respects_dm_and_allowlists(isolated_discord_db):
    service = DiscordArchiveService(
        PlatformConfig(
            enabled=True,
            token="fake",
            extra={
                "archive": {
                    "enabled": True,
                    "include_dms": False,
                    "allowed_guild_ids": [10],
                    "allowed_channel_ids": [123],
                }
            },
        )
    )

    dm = FakeDMChannel(1)
    guild = FakeGuild(10)
    allowed = FakeTextChannel(123, guild=guild)
    blocked_channel = FakeTextChannel(999, guild=guild)
    blocked_guild = FakeTextChannel(123, guild=FakeGuild(77))

    assert service.is_channel_allowed(dm) is False
    assert service.is_channel_allowed(allowed) is True
    assert service.is_channel_allowed(blocked_channel) is False
    assert service.is_channel_allowed(blocked_guild) is False


def test_message_to_archive_model_shapes_thread_reply_attachment_and_bot(isolated_discord_db):
    service = DiscordArchiveService(
        PlatformConfig(enabled=True, token="fake", extra={"archive": {"enabled": True}})
    )
    guild = FakeGuild(10, name="Hermes Guild")
    parent = FakeTextChannel(123, guild=guild, name="support")
    thread = FakeThread(456, guild=guild, parent=parent, name="topic")
    author = FakeAuthor(42, name="bot-user", bot=True)
    reference = FakeReference(7, channel_id=123, guild_id=10)
    attachment = FakeAttachment(9, "report.txt")
    message = FakeMessage(
        99,
        channel=thread,
        author=author,
        content="  hello\r\nworld  ",
        edited_at=datetime(2026, 4, 1, tzinfo=UTC),
        attachments=[attachment],
        reference=reference,
    )

    model = service.message_to_archive_model(message)

    assert model.message_id == 99
    assert model.guild_id == 10
    assert model.channel_id == 456
    assert model.thread_id == 456
    assert model.author_id == 42
    assert model.author_is_bot is True
    assert model.content == "hello\nworld"
    assert model.reply_to_message_id == 7
    assert model.reply_to_channel_id == 123
    assert model.reply_to_guild_id == 10
    assert model.attachments_json[0]["id"] == 9
    assert model.attachments_json[0]["filename"] == "report.txt"


@pytest.mark.asyncio
async def test_archive_message_edit_and_delete_round_trip(isolated_discord_db):
    service = DiscordArchiveService(
        PlatformConfig(
            enabled=True,
            token="fake",
            extra={"archive": {"enabled": True, "frontfill": {"enabled": False}}},
        )
    )
    await service.start(SimpleNamespace(guilds=[], private_channels=[]))

    guild = FakeGuild(10)
    channel = FakeTextChannel(123, guild=guild)
    author = FakeAuthor(42, name="Jezza")

    before = FakeMessage(1001, channel=channel, author=author, content="before")
    await service.archive_message(before)
    stored = service.db.get_message(channel.id, before.id)
    assert stored is not None
    assert stored["content"] == "before"

    after = FakeMessage(
        1001,
        channel=channel,
        author=author,
        content="after",
        created_at=before.created_at,
        edited_at=datetime(2026, 4, 1, 1, 0, tzinfo=UTC),
    )
    await service.archive_message_edit(before, after)
    stored = service.db.get_message(channel.id, after.id)
    assert stored is not None
    assert stored["content"] == "after"

    with service.db.sync_session() as session:
        stmt = sa.select(sa.func.count()).select_from(MessageChange).where(
            MessageChange.message_id == after.id
        )
        assert int(session.scalar(stmt) or 0) == 1

    service.mark_deleted(message_id=after.id, channel_id=channel.id, guild_id=guild.id)
    stored = service.db.get_message(channel.id, after.id)
    assert stored is not None
    assert stored["deleted"] is True

    await service.stop()


@pytest.mark.asyncio
async def test_sync_channel_forward_seeds_and_then_fetches_after_cursor(isolated_discord_db):
    service = DiscordArchiveService(
        PlatformConfig(
            enabled=True,
            token="fake",
            extra={"archive": {"enabled": True, "frontfill": {"enabled": True}}},
        )
    )
    await service.start(SimpleNamespace(guilds=[], private_channels=[]))

    guild = FakeGuild(10)
    channel = FakeTextChannel(123, guild=guild)
    author = FakeAuthor(42, name="Jezza")
    channel._messages = [
        FakeMessage(1, channel=channel, author=author, content="one", created_at=datetime(2026, 4, 1, 0, 0, tzinfo=UTC)),
        FakeMessage(2, channel=channel, author=author, content="two", created_at=datetime(2026, 4, 1, 0, 1, tzinfo=UTC)),
    ]

    seeded = await service.sync_channel_forward(
        channel,
        max_pages=5,
        seed_limit=10,
        drain_all_pages=True,
    )
    assert seeded == 2
    assert service.db.get_channel_cursor(channel.id) == 2

    channel._messages.extend(
        [
            FakeMessage(3, channel=channel, author=author, content="three"),
            FakeMessage(4, channel=channel, author=author, content="four", created_at=datetime(2026, 4, 1, 0, 3, tzinfo=UTC)),
        ]
    )
    fetched = await service.sync_channel_forward(
        channel,
        max_pages=5,
        seed_limit=10,
        drain_all_pages=False,
    )
    assert fetched == 2
    assert service.db.get_channel_cursor(channel.id) == 4
    assert service.db.get_message(channel.id, 4)["content"] == "four"

    await service.stop()


@pytest.mark.asyncio
async def test_run_frontfill_tick_advances_round_robin_index(isolated_discord_db):
    service = DiscordArchiveService(
        PlatformConfig(
            enabled=True,
            token="fake",
            extra={
                "archive": {
                    "enabled": True,
                    "frontfill": {
                        "enabled": True,
                        "max_channels_per_tick": 1,
                    },
                }
            },
        )
    )
    guild = FakeGuild(10)
    ch1 = FakeTextChannel(101, guild=guild)
    ch2 = FakeTextChannel(202, guild=guild)
    guild.text_channels = [ch1, ch2]
    client = SimpleNamespace(guilds=[guild], private_channels=[])
    await service.start(client)

    seen: list[int] = []

    async def fake_sync(channel, **_kwargs):
        seen.append(channel.id)
        return 0

    service.sync_channel_forward = fake_sync  # type: ignore[method-assign]

    await service.run_frontfill_tick()
    await service.run_frontfill_tick()

    assert seen == [101, 202]
    await service.stop()


@pytest.mark.asyncio
async def test_sync_channel_backfill_uses_persisted_state_and_marks_complete(isolated_discord_db):
    service = DiscordArchiveService(
        PlatformConfig(
            enabled=True,
            token="fake",
            extra={
                "archive": {
                    "enabled": True,
                    "frontfill": {"enabled": False},
                    "backfill": {"enabled": True, "max_pages_per_channel": 2},
                }
            },
        )
    )
    await service.start(SimpleNamespace(guilds=[], private_channels=[]))

    guild = FakeGuild(10)
    channel = FakeTextChannel(123, guild=guild)
    author = FakeAuthor(42, name="Jezza")

    # Seed recent history first so backfill has an oldest known point.
    service.db.upsert_message(
            service.message_to_archive_model(
            FakeMessage(3, channel=channel, author=author, content="three", created_at=datetime(2026, 4, 1, 0, 2, tzinfo=UTC))
        )
    )
    service.db.upsert_message(
            service.message_to_archive_model(
            FakeMessage(4, channel=channel, author=author, content="four", created_at=datetime(2026, 4, 1, 0, 3, tzinfo=UTC))
        )
    )

    channel._messages = [
        FakeMessage(1, channel=channel, author=author, content="one", created_at=datetime(2026, 4, 1, 0, 0, tzinfo=UTC)),
        FakeMessage(2, channel=channel, author=author, content="two", created_at=datetime(2026, 4, 1, 0, 1, tzinfo=UTC)),
        FakeMessage(3, channel=channel, author=author, content="three", created_at=datetime(2026, 4, 1, 0, 2, tzinfo=UTC)),
        FakeMessage(4, channel=channel, author=author, content="four", created_at=datetime(2026, 4, 1, 0, 3, tzinfo=UTC)),
    ]

    count = await service.sync_channel_backfill(channel, max_pages=2)
    assert count == 2
    assert service.db.get_message(channel.id, 1)["content"] == "one"
    assert service.db.get_message(channel.id, 2)["content"] == "two"

    state = service.db.get_backfill_state(channel.id)
    assert state["complete"] is True
    assert state["oldest_message_id"] == 1

    count = await service.sync_channel_backfill(channel, max_pages=2)
    assert count == 0
    await service.stop()


@pytest.mark.asyncio
async def test_run_backfill_tick_advances_round_robin_index(isolated_discord_db):
    service = DiscordArchiveService(
        PlatformConfig(
            enabled=True,
            token="fake",
            extra={
                "archive": {
                    "enabled": True,
                    "frontfill": {"enabled": False},
                    "backfill": {
                        "enabled": True,
                        "max_channels_per_tick": 1,
                    },
                }
            },
        )
    )
    guild = FakeGuild(10)
    ch1 = FakeTextChannel(101, guild=guild)
    ch2 = FakeTextChannel(202, guild=guild)
    guild.text_channels = [ch1, ch2]
    client = SimpleNamespace(guilds=[guild], private_channels=[])
    await service.start(client)

    seen: list[int] = []

    async def fake_sync(channel, **_kwargs):
        seen.append(channel.id)
        return 0

    service.sync_channel_backfill = fake_sync  # type: ignore[method-assign]

    await service.run_backfill_tick()
    await service.run_backfill_tick()

    assert seen == [101, 202]
    await service.stop()


@pytest.mark.asyncio
async def test_list_recent_messages_filters_by_age_and_channels(isolated_discord_db):
    db = DiscordArchiveDB()
    now = datetime.now(tz=UTC)

    db.upsert_message(
        archived_message(
            message_id=1,
            guild_id=10,
            channel_id=100,
            channel_name="alpha",
            author_id=1,
            author_name="one",
            author_display="one",
            content="old",
            created_at=now - timedelta(minutes=20),
            deleted=False,
        )
    )
    db.upsert_message(
        archived_message(
            message_id=2,
            guild_id=10,
            channel_id=100,
            channel_name="alpha",
            author_id=2,
            author_name="two",
            author_display="two",
            content="recent-a",
            created_at=now - timedelta(minutes=4),
            deleted=False,
        )
    )
    db.upsert_message(
        archived_message(
            message_id=3,
            guild_id=10,
            channel_id=200,
            channel_name="beta",
            author_id=3,
            author_name="three",
            author_display="three",
            content="recent-b",
            created_at=now - timedelta(minutes=2),
            deleted=False,
        )
    )
    db.upsert_message(
        archived_message(
            message_id=4,
            guild_id=10,
            channel_id=200,
            channel_name="beta",
            author_id=4,
            author_name="four",
            author_display="four",
            content="deleted",
            created_at=now - timedelta(minutes=1),
            deleted=True,
        )
    )

    rows = db.list_recent_messages(max_age=timedelta(minutes=10), limit=5)
    assert [row["message_id"] for row in rows] == [2, 3]

    rows = db.list_recent_messages(
        channel_id=[200],
        max_age=timedelta(minutes=10),
        limit=5,
    )
    assert [row["message_id"] for row in rows] == [3]

    rows = db.list_recent_messages(
        channel_id=[100, 200],
        max_age=timedelta(minutes=10),
        limit=1,
    )
    assert [row["message_id"] for row in rows] == [3]

    rows = db.list_recent_messages(channel_id=None, limit=2)
    assert [row["message_id"] for row in rows] == [2, 3]

    rows = db.list_recent_messages(channel_id=[], limit=5)
    assert rows == []

    rows_async = await db.list_recent_messages_async(
        channel_id=[100, 200],
        max_age=timedelta(minutes=10),
        limit=5,
    )
    assert [row["message_id"] for row in rows_async] == [2, 3]


def test_search_messages_falls_back_when_fts_table_missing(isolated_discord_db):
    db = DiscordArchiveDB()
    now = datetime.now(tz=UTC)

    db.upsert_message(
        archived_message(
            message_id=10,
            guild_id=10,
            channel_id=100,
            channel_name="alpha",
            author_id=1,
            author_name="one",
            author_display="one",
            content="deployment failed on staging cluster",
            created_at=now - timedelta(minutes=2),
        )
    )
    db.upsert_message(
        archived_message(
            message_id=11,
            guild_id=10,
            channel_id=100,
            channel_name="alpha",
            author_id=2,
            author_name="two",
            author_display="two",
            content="rolled back deployment and recovered",
            created_at=now - timedelta(minutes=1),
        )
    )

    results = db.search_messages("deployment", channel_id=100, limit=5, around=1)

    assert [row["hit"]["message_id"] for row in results] == [10, 11]
    assert all(row["hit"]["snippet"] for row in results)


def test_upsert_message_commits_message_and_cursor_once(isolated_discord_db):
    db = DiscordArchiveDB()
    commit_count = 0

    class TrackingSession(OrmSession):
        def commit(self):
            nonlocal commit_count
            commit_count += 1
            return super().commit()

    db.sync_session = sessionmaker(db._sync_engine, expire_on_commit=False, class_=TrackingSession)
    db.upsert_message(
        archived_message(
            message_id=55,
            guild_id=10,
            channel_id=100,
            channel_name="alpha",
            author_id=7,
            author_name="seven",
            author_display="seven",
            content="hello",
            created_at=datetime(2026, 4, 1, tzinfo=UTC),
        )
    )

    assert commit_count == 1
    assert db.get_channel_cursor(100) == 55
    assert db.get_message(100, 55)["content"] == "hello"


def test_upsert_message_creates_channel_metadata_row(isolated_discord_db):
    db = DiscordArchiveDB()

    db.upsert_message(
        archived_message(
            message_id=88,
            guild_id=10,
            channel_id=100,
            channel_name="alpha",
            author_id=7,
            author_name="seven",
            author_display="seven",
            content="hello",
            created_at=datetime(2026, 4, 1, tzinfo=UTC),
        )
    )

    channel = db.get_channel(100)
    assert channel is not None
    assert channel["channel_id"] == 100
    assert channel["guild_id"] == 10
    assert channel["channel_name"] == "alpha"


def test_mark_deleted_commits_change_log_tombstone_and_cursor_once(isolated_discord_db):
    db = DiscordArchiveDB()
    commit_count = 0

    class TrackingSession(OrmSession):
        def commit(self):
            nonlocal commit_count
            commit_count += 1
            return super().commit()

    db.sync_session = sessionmaker(db._sync_engine, expire_on_commit=False, class_=TrackingSession)
    db.mark_deleted(message_id=77, channel_id=100, guild_id=10)

    assert commit_count == 1
    stored = db.get_message(100, 77)
    assert stored is not None
    assert stored["deleted"] is True
    assert db.get_channel_cursor(100) == 77

    with db.sync_session() as session:
        stmt = sa.select(sa.func.count()).select_from(MessageChange).where(
            MessageChange.message_id == 77
        )
        assert int(session.scalar(stmt) or 0) == 1


def _ensure_discord_mock():
    if "discord" in sys.modules and hasattr(sys.modules["discord"], "__file__"):
        return

    discord_mod = MagicMock()
    discord_mod.Intents.default.return_value = MagicMock()
    discord_mod.Client = MagicMock
    discord_mod.File = MagicMock
    discord_mod.DMChannel = FakeDMChannel
    discord_mod.Thread = FakeThread
    discord_mod.ForumChannel = type("ForumChannel", (), {})
    discord_mod.MessageType = SimpleNamespace(default="default", reply="reply")
    discord_mod.opus = SimpleNamespace(is_loaded=lambda: True)
    discord_mod.ui = SimpleNamespace(
        View=object,
        button=lambda *a, **k: (lambda fn: fn),
        Button=object,
    )
    discord_mod.ButtonStyle = SimpleNamespace(success=1, primary=2, danger=3, green=1, blurple=2, red=3)
    discord_mod.Color = SimpleNamespace(orange=lambda: 1, green=lambda: 2, blue=lambda: 3, red=lambda: 4)
    discord_mod.Interaction = object
    discord_mod.Embed = MagicMock
    discord_mod.app_commands = SimpleNamespace(
        describe=lambda **kwargs: (lambda fn: fn),
        choices=lambda **kwargs: (lambda fn: fn),
        Choice=lambda **kwargs: SimpleNamespace(**kwargs),
    )

    ext_mod = MagicMock()
    commands_mod = MagicMock()
    ext_mod.commands = commands_mod

    sys.modules.setdefault("discord", discord_mod)
    sys.modules.setdefault("discord.ext", ext_mod)
    sys.modules.setdefault("discord.ext.commands", commands_mod)


@pytest.mark.asyncio
async def test_adapter_on_message_archives_before_response_filters(monkeypatch):
    _ensure_discord_mock()

    import gateway.platforms.discord as discord_platform
    from gateway.platforms.discord import DiscordAdapter

    class FakeBot:
        def __init__(self, *args, **kwargs):
            self._events = {}
            self.user = SimpleNamespace(id=999, name="Hermes")
            self.tree = SimpleNamespace(sync=AsyncMock(return_value=[]))

        def event(self, fn):
            self._events[fn.__name__] = fn
            return fn

        async def start(self, _token):
            await self._events["on_ready"]()

        async def close(self):
            return None

    monkeypatch.setattr(discord_platform.commands, "Bot", FakeBot, raising=False)
    monkeypatch.setattr(discord_platform.discord, "DMChannel", FakeDMChannel, raising=False)
    monkeypatch.setattr(discord_platform.discord, "Thread", FakeThread, raising=False)
    monkeypatch.setattr(discord_platform.discord, "MessageType", SimpleNamespace(default="default", reply="reply"), raising=False)
    monkeypatch.setattr(discord_platform.discord, "opus", SimpleNamespace(is_loaded=lambda: True), raising=False)

    import gateway.status as status_mod

    monkeypatch.setattr(status_mod, "acquire_scoped_lock", lambda *args, **kwargs: (True, None))
    monkeypatch.setattr(status_mod, "release_scoped_lock", lambda *args, **kwargs: None)

    adapter = DiscordAdapter(PlatformConfig(enabled=True, token="fake-token"))
    adapter._resolve_allowed_usernames = AsyncMock()
    adapter._register_slash_commands = MagicMock()
    adapter.handle_message = AsyncMock()
    adapter._archive_service = SimpleNamespace(
        start=AsyncMock(),
        stop=AsyncMock(),
        archive_message=AsyncMock(),
        archive_message_edit=AsyncMock(),
        mark_deleted=MagicMock(),
        bootstrap_channel=AsyncMock(),
    )

    monkeypatch.setenv("DISCORD_ALLOW_BOTS", "none")
    monkeypatch.setenv("DISCORD_IGNORE_NO_MENTION", "true")

    assert await adapter.connect() is True

    on_message = adapter._client._events["on_message"]
    channel = FakeTextChannel(123, guild=FakeGuild(10))
    other_user = SimpleNamespace(id=1234)
    message = FakeMessage(
        55,
        channel=channel,
        author=FakeAuthor(77, name="OtherBot", bot=True),
        content="hello",
        mentions=[other_user],
        message_type="default",
    )

    await on_message(message)

    adapter._archive_service.archive_message.assert_awaited_once_with(message)
    adapter.handle_message.assert_not_awaited()

    own_message = FakeMessage(
        56,
        channel=channel,
        author=adapter._client.user,
        content="ignored",
        message_type="default",
    )
    await on_message(own_message)

    adapter._archive_service.archive_message.assert_awaited_once()
    await adapter.disconnect()
