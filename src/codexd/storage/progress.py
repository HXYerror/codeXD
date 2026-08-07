from __future__ import annotations

import json
import sqlite3
from collections.abc import Sequence

from codexd.domain.ids import canonical_json, new_id


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
        SET state = 'superseded', updated_at = ?
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
                    "plain_text": "",
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


def insert_progress_update(
    connection: sqlite3.Connection,
    *,
    turn_id: str,
    state: str,
    content: str | None,
    plain_text: str | None = None,
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
    if view is None:
        return None
    revision = int(view["content_revision"]) + 1
    connection.execute(
        """
        UPDATE turn_progress_views
        SET content_revision = ?, state = ?, updated_at = ?
        WHERE turn_id = ?
        """,
        (revision, state, now, turn_id),
    )
    coalesce_key = f"turn:{turn_id}:progress"
    next_attempt_at = now
    previous = connection.execute(
        """
        SELECT state, next_attempt_at, updated_at, payload_json
        FROM discord_outbox
        WHERE coalesce_key = ? AND state <> 'superseded'
        ORDER BY enqueue_sequence DESC
        LIMIT 1
        """,
        (coalesce_key,),
    ).fetchone()
    previous_content = "Running · Codex is working"
    previous_plain_text = ""
    if previous is not None:
        try:
            previous_payload = json.loads(str(previous["payload_json"]))
        except (json.JSONDecodeError, TypeError, ValueError) as exc:
            raise ValueError("progress outbox payload is invalid") from exc
        if not isinstance(previous_payload, dict):
            raise ValueError("progress outbox payload must be an object")
        stored_content = previous_payload.get("content")
        stored_plain_text = previous_payload.get("plain_text", "")
        if not isinstance(stored_content, str) or not stored_content:
            raise ValueError("progress outbox content is invalid")
        if not isinstance(stored_plain_text, str):
            raise ValueError("progress outbox plain text is invalid")
        previous_content = stored_content
        previous_plain_text = stored_plain_text
    resolved_content = content if content is not None else previous_content
    if not resolved_content:
        raise ValueError("progress content cannot be empty")
    resolved_plain_text = (
        ""
        if state == "terminal"
        else plain_text
        if plain_text is not None
        else previous_plain_text
    )
    if state != "terminal" and min_interval_ms and previous is not None:
        previous_state = str(previous["state"])
        if previous_state == "sent":
            next_attempt_at = max(
                now,
                int(previous["updated_at"]) + min_interval_ms,
            )
        elif previous_state == "pending":
            next_attempt_at = max(now, int(previous["next_attempt_at"]))
        else:
            next_attempt_at = max(
                now + min_interval_ms,
                int(previous["next_attempt_at"]),
            )
    supersede_coalesced_outbox(
        connection,
        coalesce_key=coalesce_key,
        now=now,
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
            canonical_json(
                {
                    "kind": "turn_progress",
                    "turn_id": turn_id,
                    "revision": revision,
                    "state": state,
                    "content": resolved_content[:1900],
                    "plain_text": resolved_plain_text[:1800],
                }
            ),
            f"turn:{turn_id}:progress:{revision}",
            coalesce_key,
            f"turn-{turn_id[:8]}-progress-{revision}",
            next_attempt_at,
            now,
            now,
        ),
    )
    return outbox_id
