"""
Restart gateway tool - saves session state and restarts the gateway.

This tool is intercepted in run_agent.py and handled specially.
"""

# Special handling - save session state and restart gateway
import json
import logging
import subprocess
from datetime import UTC, datetime
from typing import TYPE_CHECKING

import discord

from gateway.run import GatewayRunner
from gateway.session import SessionSource
from hermes_constants import get_hermes_dir
from hermes_state import SessionDB
from tools.registry import registry

logger = logging.getLogger(__name__)


def restart_gateway_tool(agent, args: dict) -> str:
    if not agent.session_source:
        return json.dumps({"success": False, "error": "No session source available"})

    # Get the data directory
    data_dir = get_hermes_dir("data", "data")
    data_dir.mkdir(parents=True, exist_ok=True)

    # Save session state for recovery
    restart_file = data_dir / "pending_restart.json"
    restart_data = {
        "session_source": agent.session_source.to_dict(),
        "session_id": agent.session_id,
        "timestamp": datetime.now(UTC).isoformat(),
        "reason": args.get("reason", ""),
    }

    try:
        restart_file.write_text(json.dumps(restart_data, indent=2))

        # Fire the restart command (non-blocking)
        result = subprocess.run(
            ["systemctl", "--user", "--no-block", "restart", "hermes-gateway.service"],
            capture_output=True,
            text=True,
        )

        if result.returncode == 0:
            # Sleep to let the gateway shut down cleanly - we won't return a response
            # because the process is about to be killed by the restart anyway
            import time
            time.sleep(30)  # Long sleep - gateway will be killed mid-sleep
            # This line is unreachable but here for completeness
            return json.dumps(
                {
                    "success": True,
                    "message": "Gateway restart initiated. Session will be resumed after restart.",
                    "session_id": agent.session_id,
                }
            )
        else:
            # Clean up the restart file on failure
            restart_file.unlink(missing_ok=True)
            return json.dumps({"success": False, "error": f"Failed to restart gateway: {result.stderr}"})
    except Exception as e:
        return json.dumps({"success": False, "error": str(e)})


async def recover_from_restart(gateway: GatewayRunner):
    """Check for pending restart and resume session if found."""
    from gateway.platforms.base import MessageEvent, MessageType

    data_dir = get_hermes_dir("data", "data")
    restart_file = data_dir / "pending_restart.json"

    if not restart_file.exists():
        return

    try:
        restart_data = json.loads(restart_file.read_text())

        session_source_dict = restart_data.get("session_source")
        if not session_source_dict:
            logger.warning("Pending restart file missing session_source")
            return

        source = SessionSource.from_dict(session_source_dict)
        session_id = restart_data.get("session_id")
        reason = restart_data.get("reason", "")
        timestamp = restart_data.get("timestamp", "")

        # Get the adapter for this platform
        adapter = gateway.adapters.get(source.platform)
        if not adapter:
            logger.warning(
                "Cannot resume session: no adapter for platform %s",
                source.platform.value,
            )
            return

        # For Discord DMs, resolve the actual DM channel ID from user ID
        # The chat_id might be a user ID, but we need the DM channel ID
        if source.platform.value == "discord" and source.chat_type == "dm":
            if hasattr(adapter, "_resolve_channel"):
                dm_channel: discord.DMChannel | None = await adapter._resolve_channel(source.chat_id)
                if dm_channel and dm_channel.id:
                    logger.info(
                        "Resolved DM channel %s for user %s",
                        dm_channel.id,
                        source.chat_id,
                    )
                    source = SessionSource(
                        platform=source.platform,
                        chat_id=str(dm_channel.id),
                        chat_name=source.chat_name,
                        chat_type=source.chat_type,
                        user_id=source.user_id,
                        user_name=source.user_name,
                        thread_id=source.thread_id,
                        guild_id=source.guild_id,
                        chat_topic=source.chat_topic,
                        user_id_alt=source.user_id_alt,
                        chat_id_alt=source.chat_id_alt,
                    )

        logger.info(
            "Recovering from restart: session=%s, platform=%s, chat_id=%s",
            session_id,
            source.platform.value,
            source.chat_id,
        )

        # Get the session to append the restart result
        session_entry = gateway.session_store.get_or_create_session(source)

        # Append a synthetic tool result to the session history
        # This tells the agent the restart succeeded and it can continue
        db = SessionDB()

        restart_result = json.dumps(
            {
                "success": True,
                "message": "Gateway restarted successfully. Resuming session.",
                "previous_session_id": session_id,
                "restart_reason": reason,
                "restart_timestamp": timestamp,
            }
        )

        db.append_message(
            session_id=session_entry.session_id,
            role="tool",
            content=restart_result,
            tool_name="restart_gateway",
            tool_call_id="restart_gateway_recovery",
        )

        # Send a message to the channel indicating we're back
        try:
            await adapter.send(
                source.chat_id,
                "🔄 Gateway restarted. Continuing...",
            )
        except Exception as e:
            logger.warning("Failed to send restart notification: %s", e)

        logger.info("Restart recovery complete for session %s", session_id)

        # Create a synthetic event to trigger the agent loop
        # The agent will see the tool result in history and continue
        continuation_event = MessageEvent(
            text="[System: Gateway restart completed. Please continue your previous response.]",
            message_type=MessageType.TEXT,
            source=source,
        )

        # Trigger the agent to process and continue
        return await gateway._handle_message(continuation_event)

    except Exception as e:
        logger.error("Error recovering from restart: %s", e, exc_info=True)
        # Clean up the file on error
        restart_file.unlink(missing_ok=True)
    finally:
        restart_file.unlink(missing_ok=True)


RESTART_GATEWAY_TOOL = {
    "name": "restart_gateway",
    "description": (
        "Restart the Hermes gateway service. "
        "Saves the current session state so it can be resumed after restart. "
        "Use this when code changes require a gateway restart to take effect."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "reason": {
                "type": "string",
                "description": "Optional reason for the restart (for logging)",
            }
        },
        "required": [],
    },
}

registry.register(
    name="restart_gateway",
    toolset="discord_search",
    schema=RESTART_GATEWAY_TOOL,
    handler=lambda args, **kw: restart_gateway_tool(**kw, args=args),
    emoji="🔄",
)
