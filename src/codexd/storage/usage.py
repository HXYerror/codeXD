from __future__ import annotations

import json
import sqlite3
from collections.abc import Mapping

from codexd.errors import InvariantError

USAGE_SCOPE = "provider_last_and_thread_total"

_BREAKDOWN_FIELDS = (
    "input_tokens",
    "output_tokens",
    "cached_input_tokens",
    "reasoning_output_tokens",
    "total_tokens",
)


def validate_usage_payload(payload: Mapping[str, object]) -> dict[str, object]:
    normalized: dict[str, object] = {}
    for name in ("last", "total"):
        value = payload.get(name)
        if not isinstance(value, Mapping):
            raise InvariantError(f"usage.updated {name} breakdown is missing")
        breakdown: dict[str, int] = {}
        for field in _BREAKDOWN_FIELDS:
            token_count = value.get(field)
            if (
                isinstance(token_count, bool)
                or not isinstance(token_count, int)
                or token_count < 0
            ):
                raise InvariantError(
                    f"usage.updated {name}.{field} must be a non-negative integer"
                )
            breakdown[field] = token_count
        normalized[name] = breakdown
    context_window = payload.get("model_context_window")
    if context_window is not None:
        if (
            isinstance(context_window, bool)
            or not isinstance(context_window, int)
            or context_window <= 0
        ):
            raise InvariantError(
                "usage.updated model_context_window must be a positive integer"
            )
        normalized["model_context_window"] = context_window
    return normalized


def latest_usage_payload(
    connection: sqlite3.Connection,
    *,
    turn_id: str,
    max_sequence: int | None = None,
) -> dict[str, object] | None:
    if max_sequence is None:
        row = connection.execute(
            """
            SELECT payload_json
            FROM events
            WHERE turn_id = ? AND kind = 'usage.updated'
            ORDER BY sequence DESC
            LIMIT 1
            """,
            (turn_id,),
        ).fetchone()
    else:
        row = connection.execute(
            """
            SELECT payload_json
            FROM events
            WHERE turn_id = ? AND kind = 'usage.updated' AND sequence <= ?
            ORDER BY sequence DESC
            LIMIT 1
            """,
            (turn_id, max_sequence),
        ).fetchone()
    if row is None:
        return None
    try:
        payload = json.loads(str(row["payload_json"]))
    except json.JSONDecodeError as exc:
        raise InvariantError("persisted usage.updated payload is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise InvariantError("persisted usage.updated payload must be an object")
    return validate_usage_payload(payload)
