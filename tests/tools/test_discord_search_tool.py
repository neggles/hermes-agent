from __future__ import annotations

import csv
import io
from datetime import UTC, datetime

import pytest

import gateway.platforms.discord_db as discord_db_mod
from gateway.platforms.discord_archive import DiscordArchiveDB, DiscordMessage
from gateway.session import Platform, SessionSource
from tools.discord_search_tool import discord_search_tool


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
    monkeypatch.delenv("DISCORD_ALLOWED_USERS", raising=False)
    _clear_discord_db_caches()
    yield
    _clear_discord_db_caches()


def _message(
    *,
    message_id: int,
    guild_id: int,
    channel_id: int,
    channel_name: str,
    content: str,
) -> DiscordMessage:
    return DiscordMessage(
        message_id=message_id,
        guild_id=guild_id,
        guild_name=f"Guild {guild_id}",
        channel_id=channel_id,
        channel_name=channel_name,
        thread_id=None,
        author_id=1,
        author_name="user",
        author_display="user",
        author_is_bot=False,
        content=content,
        attachments_json=[],
        reactions_json=[],
        reply_to_message_id=None,
        reply_to_channel_id=None,
        reply_to_guild_id=None,
        created_at=datetime(2026, 4, 4, tzinfo=UTC),
        edited_at=None,
        deleted=False,
    )


def _csv_rows(raw: str) -> list[dict[str, str]]:
    return list(csv.DictReader(io.StringIO(raw)))


def test_discord_search_is_scoped_to_current_guild_in_guild_context(isolated_discord_db):
    db = DiscordArchiveDB()
    db.upsert_message(_message(message_id=1, guild_id=10, channel_id=100, channel_name="alpha", content="deployment issue"))
    db.upsert_message(_message(message_id=2, guild_id=20, channel_id=200, channel_name="beta", content="deployment issue"))

    result = discord_search_tool(
        mode="search",
        query="deployment",
        source=SessionSource(
            platform=Platform.DISCORD,
            chat_id="100",
            chat_type="group",
            user_id="999",
            guild_id="10",
        ),
    )

    assert "hit_message_id" in result
    rows = _csv_rows(result)
    assert [row["hit_message_id"] for row in rows] == ["1"]


def test_discord_search_dm_allowed_user_can_search_anything(isolated_discord_db, monkeypatch):
    monkeypatch.setenv("DISCORD_ALLOWED_USERS", "999")
    db = DiscordArchiveDB()
    db.upsert_message(_message(message_id=1, guild_id=10, channel_id=100, channel_name="alpha", content="deployment issue"))
    db.upsert_message(_message(message_id=2, guild_id=20, channel_id=200, channel_name="beta", content="deployment issue"))

    result = discord_search_tool(
        mode="search",
        query="deployment",
        source=SessionSource(
            platform=Platform.DISCORD,
            chat_id="dm-1",
            chat_type="dm",
            user_id="999",
        ),
    )

    assert "hit_message_id" in result
    rows = _csv_rows(result)
    assert {row["hit_message_id"] for row in rows} == {"1", "2"}


def test_discord_search_dm_disallowed_user_is_rejected(isolated_discord_db):
    db = DiscordArchiveDB()
    db.upsert_message(_message(message_id=1, guild_id=10, channel_id=100, channel_name="alpha", content="deployment issue"))

    result = discord_search_tool(
        mode="search",
        query="deployment",
        source=SessionSource(
            platform=Platform.DISCORD,
            chat_id="dm-2",
            chat_type="dm",
            user_id="555",
        ),
    )

    assert "only allowed in DMs from allowed users" in result
