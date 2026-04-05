#!/usr/bin/env python3
"""Structured Discord archive search tool."""

import csv
import io
import json
from datetime import UTC, datetime
from functools import lru_cache
from os import getenv
from typing import Any

import sqlalchemy as sa

from gateway.platforms.discord_archive import DiscordArchiveDB, DiscordMessage
from gateway.platforms.discord_db import get_sync_engine
from gateway.session import Platform, SessionSource
from tools.registry import registry

_DEFAULT_LIMIT = 25
_MAX_LIMIT = 200
_MAX_AROUND = 5
_MAX_MESSAGE_IDS = 50
_CONTEXT_PREVIEW_LIMIT = 120


@lru_cache(maxsize=1)
def _get_archive_db() -> DiscordArchiveDB:
    """Lazily initialize the Discord archive schema."""
    return DiscordArchiveDB()


@lru_cache(maxsize=1)
def check_discord_search_requirements() -> bool:
    """Enable only when the archive DB connection works."""
    try:
        db = _get_archive_db()
        db._sync_engine.connect().close()
        return True
    except Exception:
        return False


def _parse_int(value: Any, *, field_name: str) -> tuple[int | None, str | None]:
    if value is None or value == "":
        return None, None
    try:
        return int(value), None
    except (TypeError, ValueError):
        return None, f"Invalid {field_name}: {value!r}"


def _parse_message_ids(value: Any) -> tuple[list[int], str | None]:
    if value is None or value == "":
        return [], None
    if isinstance(value, (list, tuple, set)):
        raw_items = list(value)
    else:
        raw_items = [part.strip() for part in str(value).split(",")]

    parsed_ids: list[int] = []
    seen: set[int] = set()
    for raw_item in raw_items:
        if raw_item is None or raw_item == "":
            continue
        try:
            message_id = int(raw_item)
        except (TypeError, ValueError):
            return [], f"Invalid Discord message ID: {raw_item!r}"
        if message_id in seen:
            continue
        seen.add(message_id)
        parsed_ids.append(message_id)
    return parsed_ids[:_MAX_MESSAGE_IDS], None


def _clean_discord_id(value: Any) -> str:
    text = str(value or "").strip()
    if text.startswith("<@") and text.endswith(">"):
        text = text.lstrip("<@!").rstrip(">")
    if text.lower().startswith("user:"):
        text = text[5:]
    return text.strip()


def _allowed_discord_user_ids() -> set[int]:
    raw_env = str(getenv("DISCORD_ALLOWED_USERS", "") or "").strip()
    if not raw_env:
        return set()
    allowed_ids: set[int] = set()
    for raw_item in raw_env.split(","):
        cleaned = _clean_discord_id(raw_item)
        if not cleaned:
            continue
        try:
            allowed_ids.add(int(cleaned))
        except (TypeError, ValueError):
            continue
    return allowed_ids


def _resolve_secure_scope(
    source: SessionSource | None,
) -> tuple[int | None, str | None]:
    if source is None:
        return None, None

    if source.platform != Platform.DISCORD:
        return None, None

    if source.chat_type == "dm":
        try:
            user_id = int(source.user_id) if source.user_id is not None else None
        except (TypeError, ValueError):
            user_id = None
        if user_id is not None and user_id in _allowed_discord_user_ids():
            return None, None
        return (
            None,
            "Discord archive search outside the current guild is only allowed in DMs from allowed users.",
        )

    try:
        guild_id = int(source.guild_id) if source.guild_id is not None else None
    except (TypeError, ValueError):
        guild_id = None
    if guild_id is None:
        return None, "Discord archive search in guild contexts requires a guild_id."
    return guild_id, None


def _channel_guild_id(channel_id: int) -> int | None:
    db = _get_archive_db()
    channel = db.get_channel(channel_id)
    if channel and channel.get("guild_id") is not None:
        return int(channel["guild_id"])

    engine = get_sync_engine()
    with engine.connect() as conn:
        row = conn.execute(
            sa.select(DiscordMessage.guild_id).where(DiscordMessage.channel_id == channel_id).limit(1)
        ).first()
    if row is None or row.guild_id is None:
        return None
    return int(row.guild_id)


def _ensure_channel_in_scope(channel_id: int | None, scoped_guild_id: int | None) -> str | None:
    if channel_id is None or scoped_guild_id is None:
        return None
    channel_guild_id = _channel_guild_id(channel_id)
    if channel_guild_id is None:
        return None
    if channel_guild_id != scoped_guild_id:
        return "Requested channel is outside the current Discord guild."
    return None


def _parse_datetime(value: Any, *, field_name: str) -> tuple[datetime | None, str | None]:
    if value is None or value == "":
        return None, None
    if isinstance(value, datetime):
        return (value if value.tzinfo else value.replace(tzinfo=UTC)), None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=UTC), None

    text = str(value).strip()
    if not text:
        return None, None

    try:
        if text.replace(".", "", 1).isdigit():
            return datetime.fromtimestamp(float(text), tz=UTC), None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
        return (parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)), None
    except (TypeError, ValueError):
        return None, f"Invalid {field_name}: {value!r}"


def _clamp_limit(limit: Any) -> int:
    try:
        limit_int = int(limit)
    except (TypeError, ValueError):
        limit_int = _DEFAULT_LIMIT
    return max(1, min(limit_int, _MAX_LIMIT))


def _clamp_around(around: Any) -> int:
    try:
        around_int = int(around)
    except (TypeError, ValueError):
        around_int = 1
    return max(0, min(around_int, _MAX_AROUND))


def _stringify_cell(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return value


def _truncate_text(value: str, limit: int = _CONTEXT_PREVIEW_LIMIT) -> str:
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 3)].rstrip() + "..."


def _compact_content(value: str | None) -> str:
    normalized = " ".join((value or "").split()).strip()
    if not normalized:
        return "[non-text message]"
    return _truncate_text(normalized)


def _context_line(row: dict[str, Any]) -> str:
    author = row.get("author_display") or row.get("author_name") or row.get("author_id") or "unknown"
    return f"{author}: {_compact_content(row.get('content'))}"


def _search_context_columns() -> list[str]:
    columns: list[str] = []
    for idx in range(1, _MAX_AROUND + 1):
        columns.append(f"before_{idx}")
    for idx in range(1, _MAX_AROUND + 1):
        columns.append(f"after_{idx}")
    return columns


def _search_context_cells(
    before_rows: list[dict[str, Any]],
    after_rows: list[dict[str, Any]],
) -> dict[str, str]:
    cells: dict[str, str] = {}
    for idx in range(_MAX_AROUND):
        cells[f"before_{idx + 1}"] = _context_line(before_rows[idx]) if idx < len(before_rows) else ""
        cells[f"after_{idx + 1}"] = _context_line(after_rows[idx]) if idx < len(after_rows) else ""
    return cells


def _rows_to_csv(columns: list[str], rows: list[dict[str, Any]]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(columns)
    for row in rows:
        writer.writerow([_stringify_cell(row.get(col, "")) for col in columns])
    return buf.getvalue().rstrip("\n")


def _fetch_full_messages(message_ids: list[int]) -> dict[str, Any]:
    ordered_ids = message_ids[:_MAX_MESSAGE_IDS]
    if not ordered_ids:
        return {
            "columns": ["message_id", "created_at", "channel_id", "author_name", "content"],
            "rows": [],
        }

    engine = get_sync_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            sa.select(
                DiscordMessage.message_id,
                DiscordMessage.created_at,
                DiscordMessage.channel_id,
                DiscordMessage.channel_name,
                DiscordMessage.thread_id,
                DiscordMessage.author_id,
                DiscordMessage.author_name,
                DiscordMessage.author_display,
                DiscordMessage.content,
                DiscordMessage.deleted,
            ).where(DiscordMessage.message_id.in_(ordered_ids))
        )
        by_id = {int(row.message_id): row for row in rows}

    payload_rows: list[dict[str, Any]] = []
    for message_id in ordered_ids:
        row = by_id.get(message_id)
        if row is None:
            continue
        payload_rows.append(
            {
                "message_id": row.message_id,
                "created_at": row.created_at,
                "channel_id": row.channel_id,
                "channel_name": row.channel_name,
                "thread_id": row.thread_id,
                "author_id": row.author_id,
                "author_name": row.author_name,
                "author_display": row.author_display,
                "content": row.content,
                "deleted": row.deleted,
            }
        )

    return {
        "columns": [
            "message_id",
            "created_at",
            "channel_id",
            "channel_name",
            "thread_id",
            "author_id",
            "author_name",
            "author_display",
            "content",
            "deleted",
        ],
        "rows": payload_rows,
    }


def _fetch_full_messages_scoped(
    message_ids: list[int],
    *,
    scoped_guild_id: int | None,
) -> dict[str, Any]:
    result = _fetch_full_messages(message_ids)
    if scoped_guild_id is None:
        return result

    engine = get_sync_engine()
    with engine.connect() as conn:
        rows = conn.execute(
            sa.select(DiscordMessage.message_id).where(
                DiscordMessage.message_id.in_(message_ids),
                DiscordMessage.guild_id == scoped_guild_id,
            )
        )
        allowed_ids = {int(row.message_id) for row in rows}

    result["rows"] = [row for row in result["rows"] if int(row["message_id"]) in allowed_ids]
    return result


def _search_rows(
    *,
    query: str,
    guild_id: int | None,
    channel_id: int | None,
    since: datetime | None,
    until: datetime | None,
    limit: int,
    around: int,
) -> dict[str, Any]:
    db = _get_archive_db()
    results = db.search_messages(
        query=query,
        guild_id=guild_id,
        channel_id=channel_id,
        since_ts=since,
        until_ts=until,
        limit=limit,
        around=around,
    )

    rows: list[dict[str, Any]] = []
    for result in results:
        hit = db.enrich_reply_context_rows([result["hit"]])[0]
        row = {
            "hit_message_id": hit.get("message_id"),
            "hit_created_at": hit.get("created_at"),
            "hit_channel_id": hit.get("channel_id"),
            "hit_channel_name": hit.get("channel_name"),
            "hit_thread_id": hit.get("thread_id"),
            "hit_author_id": hit.get("author_id"),
            "hit_author_name": hit.get("author_name"),
            "hit_author_display": hit.get("author_display"),
            "hit_snippet": hit.get("snippet"),
            "hit_preview": _compact_content(hit.get("content")),
            "reply_to_message_id": hit.get("reply_to_message_id"),
            "reply_author_display": hit.get("reply_author_display", ""),
            "reply_preview": hit.get("reply_preview", ""),
        }
        row.update(_search_context_cells(result["context_before"], result["context_after"]))
        rows.append(row)

    return {
        "columns": [
            "hit_message_id",
            "hit_created_at",
            "hit_channel_id",
            "hit_channel_name",
            "hit_thread_id",
            "hit_author_id",
            "hit_author_name",
            "hit_author_display",
            "hit_snippet",
            "hit_preview",
            "reply_to_message_id",
            "reply_author_display",
            "reply_preview",
            *_search_context_columns(),
        ],
        "rows": rows,
    }


def _recent_rows(*, channel_id: int, limit: int, include_bots: bool) -> dict[str, Any]:
    db = _get_archive_db()
    messages = db.list_recent_messages(
        channel_id=channel_id,
        limit=limit,
        include_bots=include_bots,
    )
    return {
        "columns": [
            "message_id",
            "created_at",
            "channel_id",
            "channel_name",
            "thread_id",
            "author_id",
            "author_name",
            "author_display",
            "content",
            "deleted",
            "reply_to_message_id",
        ],
        "rows": [
            {
                "message_id": row.get("message_id"),
                "created_at": row.get("created_at"),
                "channel_id": row.get("channel_id"),
                "channel_name": row.get("channel_name"),
                "thread_id": row.get("thread_id"),
                "author_id": row.get("author_id"),
                "author_name": row.get("author_name"),
                "author_display": row.get("author_display"),
                "content": row.get("content"),
                "deleted": row.get("deleted"),
                "reply_to_message_id": row.get("reply_to_message_id"),
            }
            for row in messages
        ],
    }


def _messages_after_rows(
    *,
    channel_id: int,
    after_message_id: int,
    limit: int,
    include_bots: bool,
) -> dict[str, Any]:
    db = _get_archive_db()
    messages = db.list_messages_after(
        channel_id=channel_id,
        after_message_id=after_message_id,
        limit=limit,
        include_bots=include_bots,
    )
    return {
        "columns": [
            "message_id",
            "created_at",
            "channel_id",
            "channel_name",
            "thread_id",
            "author_id",
            "author_name",
            "author_display",
            "content",
            "deleted",
            "reply_to_message_id",
        ],
        "rows": [
            {
                "message_id": row.get("message_id"),
                "created_at": row.get("created_at"),
                "channel_id": row.get("channel_id"),
                "channel_name": row.get("channel_name"),
                "thread_id": row.get("thread_id"),
                "author_id": row.get("author_id"),
                "author_name": row.get("author_name"),
                "author_display": row.get("author_display"),
                "content": row.get("content"),
                "deleted": row.get("deleted"),
                "reply_to_message_id": row.get("reply_to_message_id"),
            }
            for row in messages
        ],
    }


def _change_rows(
    *,
    channel_id: int,
    anchor_message_id: int,
    change_kind: str,
    limit: int,
    include_bots: bool,
) -> dict[str, Any]:
    db = _get_archive_db()
    match change_kind:
        case "message":
            changes = db.list_changes_since_anchor(
                channel_id=channel_id,
                anchor_message_id=anchor_message_id,
                limit=limit,
                include_bots=include_bots,
            )
        case "reaction":
            changes = db.list_reaction_changes_since_anchor(
                channel_id=channel_id,
                anchor_message_id=anchor_message_id,
                limit=limit,
                include_bots=include_bots,
            )
        case _:
            changes = db.list_all_changes_since_anchor(
                channel_id=channel_id,
                anchor_message_id=anchor_message_id,
                limit=limit,
                include_bots=include_bots,
            )

    return {
        "columns": [
            "id",
            "message_id",
            "channel_id",
            "guild_id",
            "author_id",
            "author_name",
            "author_display",
            "changed_at",
            "change_type",
            "before_content",
            "after_content",
            "emoji_display",
            "emoji_key",
            "emoji_name",
            "message_author_display",
        ],
        "rows": [
            {
                "id": row.get("id"),
                "message_id": row.get("message_id"),
                "channel_id": row.get("channel_id"),
                "guild_id": row.get("guild_id"),
                "author_id": row.get("author_id"),
                "author_name": row.get("author_name"),
                "author_display": row.get("author_display"),
                "changed_at": row.get("changed_at"),
                "change_type": row.get("change_type"),
                "before_content": row.get("before_content"),
                "after_content": row.get("after_content"),
                "emoji_display": row.get("emoji_display"),
                "emoji_key": row.get("emoji_key"),
                "emoji_name": row.get("emoji_name"),
                "message_author_display": row.get("message_author_display"),
            }
            for row in changes
        ],
    }


def discord_search_tool(
    mode: str = "search",
    *,
    query: str = "",
    channel_id: int | str = None,
    guild_id: int | str = None,
    message_ids: str | int | list[int] | list[str] | None = None,
    after_message_id: int | str = None,
    anchor_message_id: int | str = None,
    change_kind: str = "all",
    limit: int = _DEFAULT_LIMIT,
    around: int = 1,
    include_bots: bool = False,
    since: datetime | None = None,
    until: datetime | None = None,
    source: SessionSource | None = None,
) -> str:
    """Search the Discord archive using structured parameters."""
    mode = str(mode or "search").strip().lower()
    limit = _clamp_limit(limit)
    around = _clamp_around(around)

    parsed_channel_id, channel_error = _parse_int(channel_id, field_name="channel_id")
    if channel_error:
        return json.dumps({"success": False, "error": channel_error}, ensure_ascii=False)

    parsed_guild_id, guild_error = _parse_int(guild_id, field_name="guild_id")
    if guild_error:
        return json.dumps({"success": False, "error": guild_error}, ensure_ascii=False)

    parsed_after_id, after_error = _parse_int(after_message_id, field_name="after_message_id")
    if after_error:
        return json.dumps({"success": False, "error": after_error}, ensure_ascii=False)

    parsed_anchor_id, anchor_error = _parse_int(anchor_message_id, field_name="anchor_message_id")
    if anchor_error:
        return json.dumps({"success": False, "error": anchor_error}, ensure_ascii=False)

    parsed_since, since_error = _parse_datetime(since, field_name="since")
    if since_error:
        return json.dumps({"success": False, "error": since_error}, ensure_ascii=False)

    parsed_until, until_error = _parse_datetime(until, field_name="until")
    if until_error:
        return json.dumps({"success": False, "error": until_error}, ensure_ascii=False)

    normalized_ids, ids_error = _parse_message_ids(message_ids)
    if ids_error:
        return json.dumps({"success": False, "error": ids_error}, ensure_ascii=False)

    scoped_guild_id, scope_error = _resolve_secure_scope(source)
    if scope_error:
        return json.dumps({"success": False, "error": scope_error}, ensure_ascii=False)
    if scoped_guild_id is not None:
        if parsed_guild_id is not None and parsed_guild_id != scoped_guild_id:
            return json.dumps(
                {"success": False, "error": "Requested guild is outside the current Discord guild."},
                ensure_ascii=False,
            )
        parsed_guild_id = scoped_guild_id

    try:
        match mode:
            case "search":
                query = (query or "").strip()
                if not query:
                    return json.dumps(
                        {"success": False, "error": "Provide 'query' for mode='search'."},
                        ensure_ascii=False,
                    )
                result = _search_rows(
                    query=query,
                    guild_id=parsed_guild_id,
                    channel_id=parsed_channel_id,
                    since=parsed_since,
                    until=parsed_until,
                    limit=limit,
                    around=around,
                )
            case "fetch_messages":
                if not normalized_ids:
                    return json.dumps(
                        {"success": False, "error": "Provide 'message_ids' for mode='fetch_messages'."},
                        ensure_ascii=False,
                    )
                result = _fetch_full_messages_scoped(
                    normalized_ids,
                    scoped_guild_id=scoped_guild_id,
                )
            case "recent_messages":
                if parsed_channel_id is None:
                    return json.dumps(
                        {"success": False, "error": "Provide 'channel_id' for mode='recent_messages'."},
                        ensure_ascii=False,
                    )
                channel_scope_error = _ensure_channel_in_scope(parsed_channel_id, scoped_guild_id)
                if channel_scope_error:
                    return json.dumps({"success": False, "error": channel_scope_error}, ensure_ascii=False)
                result = _recent_rows(
                    channel_id=parsed_channel_id,
                    limit=limit,
                    include_bots=bool(include_bots),
                )
            case "messages_after":
                if parsed_channel_id is None or parsed_after_id is None:
                    return json.dumps(
                        {
                            "success": False,
                            "error": "Provide both 'channel_id' and 'after_message_id' for mode='messages_after'.",
                        },
                        ensure_ascii=False,
                    )
                channel_scope_error = _ensure_channel_in_scope(parsed_channel_id, scoped_guild_id)
                if channel_scope_error:
                    return json.dumps({"success": False, "error": channel_scope_error}, ensure_ascii=False)
                result = _messages_after_rows(
                    channel_id=parsed_channel_id,
                    after_message_id=parsed_after_id,
                    limit=limit,
                    include_bots=bool(include_bots),
                )
            case "changes_since_anchor":
                if parsed_channel_id is None or parsed_anchor_id is None:
                    return json.dumps(
                        {
                            "success": False,
                            "error": "Provide both 'channel_id' and 'anchor_message_id' for mode='changes_since_anchor'.",
                        },
                        ensure_ascii=False,
                    )
                if change_kind not in {"all", "message", "reaction"}:
                    return json.dumps(
                        {
                            "success": False,
                            "error": "change_kind must be one of: all, message, reaction.",
                        },
                        ensure_ascii=False,
                    )
                channel_scope_error = _ensure_channel_in_scope(parsed_channel_id, scoped_guild_id)
                if channel_scope_error:
                    return json.dumps({"success": False, "error": channel_scope_error}, ensure_ascii=False)
                result = _change_rows(
                    channel_id=parsed_channel_id,
                    anchor_message_id=parsed_anchor_id,
                    change_kind=change_kind,
                    limit=limit,
                    include_bots=bool(include_bots),
                )
            case _:
                return json.dumps(
                    {
                        "success": False,
                        "error": "mode must be one of: search, fetch_messages, recent_messages, messages_after, changes_since_anchor.",
                    },
                    ensure_ascii=False,
                )
    except Exception as e:
        return json.dumps({"success": False, "error": f"Discord search failed: {e}"}, ensure_ascii=False)

    return _rows_to_csv(columns=result["columns"], rows=result["rows"])


DISCORD_SEARCH_SCHEMA = {
    "name": "discord_search",
    "description": (
        "Search the local Discord archive using structured parameters instead of raw SQL. "
        "Returns CSV with headers. Use `mode='search'` for full-text search, "
        "`mode='fetch_messages'` to fetch exact messages by ID, "
        "`mode='recent_messages'` to list recent channel history, "
        "`mode='messages_after'` to list channel messages after an anchor message, and "
        "`mode='changes_since_anchor'` to list edit/delete/reaction events after an anchor message."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "mode": {
                "type": "string",
                "enum": [
                    "search",
                    "fetch_messages",
                    "recent_messages",
                    "messages_after",
                    "changes_since_anchor",
                ],
                "description": "Which Discord archive operation to run.",
            },
            "query": {
                "type": "string",
                "description": "Full-text search query for mode='search'.",
            },
            "channel_id": {
                "type": ["integer", "string"],
                "description": "Discord channel ID for channel-scoped operations.",
            },
            "guild_id": {
                "type": ["integer", "string"],
                "description": "Optional Discord guild ID filter for mode='search'.",
            },
            "message_ids": {
                "type": ["array", "string"],
                "items": {"type": ["integer", "string"]},
                "description": f"DiscordMessage IDs for mode='fetch_messages'. Maximum {_MAX_MESSAGE_IDS}.",
            },
            "after_message_id": {
                "type": ["integer", "string"],
                "description": "Anchor message ID for mode='messages_after'.",
            },
            "anchor_message_id": {
                "type": ["integer", "string"],
                "description": "Anchor message ID for mode='changes_since_anchor'.",
            },
            "change_kind": {
                "type": "string",
                "enum": ["all", "message", "reaction"],
                "description": "Change type filter for mode='changes_since_anchor'.",
            },
            "limit": {
                "type": "integer",
                "description": f"Maximum rows to return. Clamped to {_MAX_LIMIT}.",
            },
            "around": {
                "type": "integer",
                "description": f"How many neighboring messages of context to include on each side for mode='search'. Clamped to {_MAX_AROUND}.",
            },
            "include_bots": {
                "type": "boolean",
                "description": "Whether to include bot-authored messages in recent/after/change operations.",
            },
            "since": {
                "type": ["string", "number"],
                "description": "Optional lower time bound for mode='search'. Accepts ISO-8601 string or Unix timestamp.",
            },
            "until": {
                "type": ["string", "number"],
                "description": "Optional upper time bound for mode='search'. Accepts ISO-8601 string or Unix timestamp.",
            },
        },
        "required": [],
    },
}


registry.register(
    name="discord_search",
    toolset="discord_search",
    schema=DISCORD_SEARCH_SCHEMA,
    handler=lambda args, **kw: discord_search_tool(**args, **kw),
    check_fn=check_discord_search_requirements,
)
