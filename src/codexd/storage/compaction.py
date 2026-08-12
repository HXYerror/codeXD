from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass

from codexd.domain.ids import utc_now_ms
from codexd.storage.sqlite import SQLiteStore

_TERMINAL_TURNS = ("completed", "failed", "cancelled", "interrupted")
_STREAM_DETAIL_KINDS = (
    "assistant.text.delta",
    "plan.delta",
    "reasoning.summary",
    "reasoning.hidden_delta_discarded",
    "command.output.delta",
    "file_change.output.delta",
)
_LATEST_SNAPSHOT_KINDS = ("diff.updated", "usage.updated", "plan.updated")


@dataclass(frozen=True)
class DatabaseCompactionResult:
    superseded_outbox_payloads: int
    redundant_progress_rows: int
    delivered_final_payloads: int
    detached_event_links: int
    compacted_tool_projections: int
    compacted_stream_events: int
    deleted_stream_events: int
    deleted_snapshot_events: int

    def as_dict(self) -> dict[str, int]:
        return asdict(self)


def compact_database(
    store: SQLiteStore,
    *,
    now_ms: int | None = None,
    progress: Callable[[str], None] | None = None,
) -> DatabaseCompactionResult:
    """Remove redundant high-volume detail while preserving recovery projections.

    The caller must hold the process instance lock. Physical file reclamation is
    intentionally separate so a verified backup can be created before VACUUM.
    """

    now = utc_now_ms() if now_ms is None else now_ms
    terminal_placeholders = _placeholders(_TERMINAL_TURNS)
    detail_placeholders = _placeholders(_STREAM_DETAIL_KINDS)
    snapshot_placeholders = _placeholders(_LATEST_SNAPSHOT_KINDS)
    report = progress or (lambda _stage: None)
    with store.transaction() as connection:
        report("coalescing progress outbox")
        connection.execute(
            """
            WITH ranked_progress AS (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY coalesce_key
                           ORDER BY enqueue_sequence DESC
                       ) AS rank
                FROM discord_outbox
                WHERE coalesce_key LIKE 'turn:%:progress'
                  AND state <> 'sending'
            )
            DELETE FROM discord_outbox
            WHERE id IN (
                SELECT id FROM ranked_progress WHERE rank > 1
            )
              AND id NOT IN (
                  SELECT depends_on_outbox_id
                  FROM discord_outbox
                  WHERE depends_on_outbox_id IS NOT NULL
                  UNION
                  SELECT thread_creation_outbox_id
                  FROM ingress_messages
                  WHERE thread_creation_outbox_id IS NOT NULL
                  UNION
                  SELECT progress_outbox_id
                  FROM ingress_messages
                  WHERE progress_outbox_id IS NOT NULL
                  UNION
                  SELECT confirmation_outbox_id
                  FROM schedule_drafts
                  WHERE confirmation_outbox_id IS NOT NULL
                  UNION
                  SELECT outbox_id
                  FROM dynamic_tool_invocations
                  WHERE outbox_id IS NOT NULL
              )
            """,
        )
        redundant_progress_rows = int(
            connection.execute("SELECT changes()").fetchone()[0]
        )
        connection.execute(
            """
            WITH ranked_progress AS (
                SELECT id,
                       ROW_NUMBER() OVER (
                           PARTITION BY coalesce_key
                           ORDER BY enqueue_sequence DESC
                       ) AS rank
                FROM discord_outbox
                WHERE coalesce_key LIKE 'turn:%:progress'
                  AND state <> 'sending'
            )
            UPDATE discord_outbox
            SET state = 'superseded', event_sequence = NULL,
                payload_json = json_object(
                    'kind', 'retained_tombstone',
                    'original_kind', 'turn_progress'
                ),
                updated_at = ?
            WHERE id IN (
                SELECT id FROM ranked_progress WHERE rank > 1
            )
            """,
            (now,),
        )
        report("tombstoning superseded outbox")
        superseded_outbox_payloads = connection.execute(
            """
            UPDATE discord_outbox
            SET event_sequence = NULL,
                payload_json = json_object(
                    'kind', 'retained_tombstone',
                    'original_kind', COALESCE(
                        json_extract(payload_json, '$.kind'),
                        'unknown'
                    )
                ),
                updated_at = ?
            WHERE state = 'superseded'
              AND json_extract(payload_json, '$.kind') IS NOT 'retained_tombstone'
            """,
            (now,),
        ).rowcount
        report("compacting delivered final payloads")
        delivered_final_payloads = connection.execute(
            """
            UPDATE discord_outbox
            SET event_sequence = NULL,
                payload_json = json_object(
                    'kind', 'turn_final',
                    'turn_id', json_extract(payload_json, '$.turn_id'),
                    'state', json_extract(payload_json, '$.state'),
                    'terminal_code', json_extract(
                        payload_json,
                        '$.terminal_code'
                    ),
                    'delivery_state', state,
                    'compacted', 1
                ),
                updated_at = ?
            WHERE state IN ('sent', 'dead_letter')
              AND json_extract(payload_json, '$.kind') = 'turn_final'
              AND json_extract(payload_json, '$.compacted') IS NOT 1
              AND json_extract(payload_json, '$.delivered') IS NOT 1
            """,
            (now,),
        ).rowcount
        report("detaching delivered event links")
        detached_event_links = connection.execute(
            """
            UPDATE discord_outbox
            SET event_sequence = NULL
            WHERE event_sequence IS NOT NULL
              AND state IN ('sent', 'dead_letter', 'superseded')
            """
        ).rowcount
        report("compacting terminal tool projections")
        compacted_tool_projections = connection.execute(
            f"""
            UPDATE tool_projections
            SET summary_json = json_remove(
                summary_json,
                '$.text',
                '$.output',
                '$.aggregated_output'
            )
            WHERE turn_id IN (
                SELECT id FROM turns WHERE state IN ({terminal_placeholders})
            )
              AND (
                  json_type(summary_json, '$.text') IS NOT NULL
                  OR json_type(summary_json, '$.output') IS NOT NULL
                  OR json_type(summary_json, '$.aggregated_output') IS NOT NULL
              )
            """,
            _TERMINAL_TURNS,
        ).rowcount
        report("deleting terminal stream detail")
        deleted_stream_events = connection.execute(
            f"""
            DELETE FROM events
            WHERE kind IN ({detail_placeholders})
              AND turn_id IN (
                  SELECT id FROM turns WHERE state IN ({terminal_placeholders})
              )
              AND sequence NOT IN (
                  SELECT event_sequence FROM discord_outbox
                  WHERE event_sequence IS NOT NULL
              )
              AND sequence NOT IN (
                  SELECT last_event_sequence FROM message_projections
                  UNION
                  SELECT last_event_sequence FROM tool_projections
                  UNION
                  SELECT last_event_sequence FROM task_projections
              )
            """,
            (*_STREAM_DETAIL_KINDS, *_TERMINAL_TURNS),
        ).rowcount
        report("compacting referenced terminal stream detail")
        compacted_stream_events = connection.execute(
            f"""
            UPDATE events
            SET payload_json = json_object(
                    'compacted', 1,
                    'item_id', json_extract(payload_json, '$.item_id')
                ),
                raw_type = NULL, raw_hash = NULL, raw_size = NULL
            WHERE kind IN ({detail_placeholders})
              AND turn_id IN (
                  SELECT id FROM turns WHERE state IN ({terminal_placeholders})
              )
              AND json_extract(payload_json, '$.compacted') IS NOT 1
            """,
            (*_STREAM_DETAIL_KINDS, *_TERMINAL_TURNS),
        ).rowcount
        report("coalescing active stream detail")
        deleted_stream_events += connection.execute(
            f"""
            DELETE FROM events
            WHERE kind IN ({detail_placeholders})
              AND turn_id IN (
                  SELECT id FROM turns
                  WHERE state NOT IN ({terminal_placeholders})
              )
              AND sequence NOT IN (
                  SELECT MAX(e2.sequence)
                  FROM events e2
                  JOIN turns t2 ON t2.id = e2.turn_id
                  WHERE e2.kind IN ({detail_placeholders})
                    AND t2.state NOT IN ({terminal_placeholders})
                  GROUP BY e2.turn_id, e2.kind,
                           json_extract(e2.payload_json, '$.item_id'),
                           json_extract(e2.payload_json, '$.summary_index')
              )
              AND sequence NOT IN (
                  SELECT event_sequence FROM discord_outbox
                  WHERE event_sequence IS NOT NULL
              )
              AND sequence NOT IN (
                  SELECT last_event_sequence FROM message_projections
                  UNION
                  SELECT last_event_sequence FROM tool_projections
                  UNION
                  SELECT last_event_sequence FROM task_projections
              )
            """,
            (
                *_STREAM_DETAIL_KINDS,
                *_TERMINAL_TURNS,
                *_STREAM_DETAIL_KINDS,
                *_TERMINAL_TURNS,
            ),
        ).rowcount
        report("coalescing latest snapshots")
        deleted_snapshot_events = connection.execute(
            f"""
            DELETE FROM events
            WHERE kind IN ({snapshot_placeholders})
              AND sequence NOT IN (
                  SELECT MAX(sequence)
                  FROM events
                  WHERE kind IN ({snapshot_placeholders})
                  GROUP BY turn_id, kind
              )
              AND sequence NOT IN (
                  SELECT event_sequence FROM discord_outbox
                  WHERE event_sequence IS NOT NULL
              )
              AND sequence NOT IN (
                  SELECT last_event_sequence FROM message_projections
                  UNION
                  SELECT last_event_sequence FROM tool_projections
                  UNION
                  SELECT last_event_sequence FROM task_projections
              )
            """,
            (*_LATEST_SNAPSHOT_KINDS, *_LATEST_SNAPSHOT_KINDS),
        ).rowcount
    return DatabaseCompactionResult(
        superseded_outbox_payloads=superseded_outbox_payloads,
        redundant_progress_rows=redundant_progress_rows,
        delivered_final_payloads=delivered_final_payloads,
        detached_event_links=detached_event_links,
        compacted_tool_projections=compacted_tool_projections,
        compacted_stream_events=compacted_stream_events,
        deleted_stream_events=deleted_stream_events,
        deleted_snapshot_events=deleted_snapshot_events,
    )


def _placeholders(values: tuple[str, ...]) -> str:
    return ", ".join("?" for _ in values)
