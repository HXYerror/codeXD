from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence

from codexd.domain.ids import canonical_json, new_id

_PROMPT_REACTION_STATES = frozenset({"waiting", "completed", "failed"})


def supersede_coalesced_outbox(
    connection: sqlite3.Connection,
    *,
    coalesce_key: str,
    now: int,
    states: Sequence[str] = ("pending", "retry", "reconciling"),
) -> None:
    if not states:
        return
    placeholders = ", ".join("?" for _ in states)
    connection.execute(
        f"""
        UPDATE discord_outbox
        SET state = 'superseded',
            event_sequence = NULL,
            payload_json = json_object(
                'kind', 'retained_tombstone',
                'original_kind', COALESCE(
                    json_extract(payload_json, '$.kind'),
                    'unknown'
                )
            ),
            updated_at = ?
        WHERE coalesce_key = ?
          AND state IN ({placeholders})
        """,
        (now, coalesce_key, *states),
    )


def insert_initial_progress(
    connection: sqlite3.Connection,
    *,
    turn_id: str,
    discord_thread_id: int | str,
    sandbox_profile: str,
    now: int,
) -> str:
    destination_key = f"thread:{discord_thread_id}"
    outbox_id = new_id()
    connection.execute(
        """
        INSERT INTO turn_progress_views(
            turn_id, destination_key, content_revision, state, created_at, updated_at
        ) VALUES (?, ?, 1, 'queued', ?, ?)
        """,
        (turn_id, destination_key, now, now),
    )
    connection.execute(
        """
        INSERT INTO discord_outbox(
            id, destination_key, operation, payload_json, dedupe_key,
            coalesce_key, delivery_marker, state, attempts,
            next_attempt_at, created_at, updated_at
        ) VALUES (?, ?, 'send', ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
        """,
        (
            outbox_id,
            destination_key,
            canonical_json(
                {
                    "kind": "turn_progress",
                    "turn_id": turn_id,
                    "revision": 1,
                    "state": "queued",
                    "content": (
                        "Queued · waiting for Codex · "
                        f"{sandbox_profile.upper()}"
                    ),
                }
            ),
            f"turn:{turn_id}:progress:1",
            f"turn:{turn_id}:progress",
            f"turn-{turn_id[:8]}-progress-1",
            now,
            now,
            now,
        ),
    )
    return outbox_id


def insert_prompt_reaction_update(
    connection: sqlite3.Connection,
    *,
    turn_id: str,
    input_message_id: str | int | None,
    discord_thread_id: str | int,
    discord_parent_channel_id: str | int,
    state: str,
    now: int,
    event_sequence: int | None = None,
) -> str | None:
    if input_message_id is None:
        return None
    if state not in _PROMPT_REACTION_STATES:
        raise ValueError("invalid prompt reaction state")
    ingress = connection.execute(
        """
        SELECT 1
        FROM ingress_messages
        WHERE discord_message_id = ?
        """,
        (str(input_message_id),),
    ).fetchone()
    if ingress is None:
        return None
    dedupe_key = f"turn:{turn_id}:prompt-reaction:{state}"
    existing = connection.execute(
        "SELECT id FROM discord_outbox WHERE dedupe_key = ?",
        (dedupe_key,),
    ).fetchone()
    if existing is not None:
        return str(existing["id"])
    coalesce_key = f"turn:{turn_id}:prompt-reaction"
    supersede_coalesced_outbox(
        connection,
        coalesce_key=coalesce_key,
        now=now,
    )
    input_in_parent = str(input_message_id) == str(discord_thread_id)
    destination_key = (
        f"channel:{discord_parent_channel_id}"
        if input_in_parent
        else f"thread:{discord_thread_id}"
    )
    outbox_id = new_id()
    connection.execute(
        """
        INSERT INTO discord_outbox(
            id, event_sequence, destination_key, operation, payload_json,
            dedupe_key, coalesce_key, delivery_marker, state, attempts,
            next_attempt_at, created_at, updated_at
        ) VALUES (?, ?, ?, 'edit', ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
        """,
        (
            outbox_id,
            event_sequence,
            destination_key,
            canonical_json(
                {
                    "kind": "prompt_reaction",
                    "turn_id": turn_id,
                    "message_id": str(input_message_id),
                    "state": state,
                }
            ),
            dedupe_key,
            coalesce_key,
            f"prompt-reaction-{turn_id[:8]}-{state}",
            now,
            now,
            now,
        ),
    )
    return outbox_id


def insert_progress_update(
    connection: sqlite3.Connection,
    *,
    turn_id: str,
    state: str,
    content: str | None,
    now: int,
    event_sequence: int | None = None,
    min_interval_ms: int = 0,
) -> str | None:
    if min_interval_ms < 0:
        raise ValueError("progress update interval cannot be negative")
    view = connection.execute(
        "SELECT * FROM turn_progress_views WHERE turn_id = ?",
        (turn_id,),
    ).fetchone()
    if view is None or str(view["cleanup_state"]) != "active":
        return None
    revision = int(view["content_revision"]) + 1
    coalesce_key = f"turn:{turn_id}:progress"
    next_attempt_at = now
    previous = connection.execute(
        """
        SELECT id, operation, state, next_attempt_at, updated_at, payload_json
        FROM discord_outbox
        WHERE coalesce_key = ? AND state <> 'superseded'
        ORDER BY enqueue_sequence DESC
        LIMIT 1
        """,
        (coalesce_key,),
    ).fetchone()
    previous_content = "Running · Codex is working"
    previous_payload: dict[str, object] = {}
    if previous is not None:
        try:
            previous_payload = json.loads(str(previous["payload_json"]))
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise ValueError("progress outbox payload is invalid") from exc
        if not isinstance(previous_payload, dict):
            raise ValueError("progress outbox payload must be an object")
        stored_content = previous_payload.get("content")
        if not isinstance(stored_content, str) or not stored_content:
            raise ValueError("progress outbox content is invalid")
        previous_content = stored_content
    resolved_content = content if content is not None else previous_content
    if not resolved_content:
        raise ValueError("progress content cannot be empty")
    if (
        previous is not None
        and state != "terminal"
        and previous_payload.get("state") == state
        and previous_content == resolved_content[:1900]
    ):
        return None
    connection.execute(
        """
        UPDATE turn_progress_views
        SET content_revision = ?, state = ?, updated_at = ?
        WHERE turn_id = ?
        """,
        (revision, state, now, turn_id),
    )
    if state != "terminal" and min_interval_ms and previous is not None:
        previous_state = str(previous["state"])
        if previous_state == "sent":
            next_attempt_at = max(
                now,
                int(previous["updated_at"]) + min_interval_ms,
            )
        elif previous_state == "pending":
            if str(previous["operation"]) == "send":
                next_attempt_at = max(now, int(previous["next_attempt_at"]))
            else:
                next_attempt_at = (
                    int(previous["next_attempt_at"])
                    if int(previous["next_attempt_at"]) > now
                    else now + min_interval_ms
                )
        else:
            next_attempt_at = max(
                now + min_interval_ms,
                int(previous["next_attempt_at"]),
            )
    payload_json = canonical_json(
        {
            "kind": "turn_progress",
            "turn_id": turn_id,
            "revision": revision,
            "state": state,
            "content": resolved_content[:1900],
        }
    )
    dedupe_key = f"turn:{turn_id}:progress:{revision}"
    delivery_marker = f"turn-{turn_id[:8]}-progress-{revision}"
    if previous is not None and str(previous["state"]) == "pending":
        outbox_id = str(previous["id"])
        changed = connection.execute(
            """
            UPDATE discord_outbox
            SET event_sequence = ?, payload_json = ?, dedupe_key = ?,
                delivery_marker = ?, next_attempt_at = ?, updated_at = ?
            WHERE id = ? AND state = 'pending'
            """,
            (
                event_sequence,
                payload_json,
                dedupe_key,
                delivery_marker,
                next_attempt_at,
                now,
                outbox_id,
            ),
        ).rowcount
        if changed == 1:
            return outbox_id
    supersede_coalesced_outbox(
        connection,
        coalesce_key=coalesce_key,
        now=now,
        states=("pending",),
    )
    outbox_id = new_id()
    connection.execute(
        """
        INSERT INTO discord_outbox(
            id, event_sequence, destination_key, operation, payload_json,
            dedupe_key, coalesce_key, delivery_marker, state, attempts,
            next_attempt_at, created_at, updated_at
        ) VALUES (?, ?, ?, 'edit', ?, ?, ?, ?, 'pending', 0, ?, ?, ?)
        """,
        (
            outbox_id,
            event_sequence,
            view["destination_key"],
            payload_json,
            dedupe_key,
            coalesce_key,
            delivery_marker,
            next_attempt_at,
            now,
            now,
        ),
    )
    return outbox_id
