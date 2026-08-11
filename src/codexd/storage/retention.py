from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
import logging
import os
import sqlite3
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from codexd.config import RetentionConfig
from codexd.domain.ids import utc_now_ms
from codexd.errors import StorageError
from codexd.paths import AppPaths
from codexd.storage.sqlite import SQLiteStore

_TERMINAL_TURNS = (
    "completed",
    "failed",
    "cancelled",
    "interrupted",
)
logger = logging.getLogger(__name__)
try:
    _FCNTL: Any | None = importlib.import_module("fcntl")
except ImportError:
    _FCNTL = None


@dataclass(frozen=True)
class RetentionResult:
    input_attachments: int
    render_plans: int
    events: int
    event_tombstones: int
    terminal_turn_tombstones: int
    terminal_turns: int
    command_intents: int
    modal_intents: int
    schedule_drafts: int
    final_projections: int
    outbox_payloads: int
    schedule_fires: int
    incidents: int
    audit_entries: int
    log_files: int
    orphan_artifacts: int


class RetentionWorker:
    def __init__(
        self,
        *,
        store: SQLiteStore,
        paths: AppPaths,
        config: RetentionConfig,
        poll_seconds: float = 6 * 60 * 60,
    ) -> None:
        self._store = store
        self._paths = paths
        self._config = config
        self._poll_seconds = poll_seconds
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="codexd-retention")

    async def close(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def run_once(self, *, now_ms: int | None = None) -> RetentionResult:
        return await asyncio.to_thread(
            run_retention,
            self._store,
            self._paths,
            self._config,
            now_ms=now_ms,
        )

    async def _run(self) -> None:
        while not self._stop.is_set():
            try:
                await self.run_once()
            except (OSError, sqlite3.Error, StorageError) as exc:
                logger.error(
                    "Retention cleanup failed",
                    extra={
                        "stable_code": "retention_cleanup_failed",
                        "exception_type": type(exc).__name__,
                    },
                )
            with suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=self._poll_seconds)


def run_retention(
    store: SQLiteStore,
    paths: AppPaths,
    config: RetentionConfig,
    *,
    now_ms: int | None = None,
) -> RetentionResult:
    now = utc_now_ms() if now_ms is None else now_ms
    input_artifacts: list[tuple[str, Path]] = []
    render_artifacts: list[tuple[str, tuple[Path, ...]]] = []
    with store.transaction() as connection:
        attachment_rows = connection.execute(
            f"""
            SELECT a.id, a.relative_path
            FROM attachments a
            LEFT JOIN turns t ON t.id = a.turn_id
            LEFT JOIN ingress_messages i ON i.id = a.ingress_id
            WHERE a.kind IN ('input_image', 'input_file')
              AND a.retention_until <= ?
              AND (
                (a.turn_id IS NOT NULL AND t.state IN ({_placeholders(_TERMINAL_TURNS)}))
                OR
                (a.turn_id IS NULL AND i.state = 'rejected')
              )
            LIMIT 1000
            """,
            (now, *_TERMINAL_TURNS),
        ).fetchall()
        for row in attachment_rows:
            input_artifacts.append(
                (
                    str(row["id"]),
                    _safe_relative_path(paths.data_dir, str(row["relative_path"])),
                )
            )

        render_rows = connection.execute(
            """
            SELECT rp.turn_id, rp.plan_json
            FROM discord_render_plans rp
            WHERE rp.retention_until <= ?
              AND EXISTS (
                  SELECT 1 FROM discord_outbox o
                  WHERE json_extract(o.payload_json, '$.kind') = 'turn_final'
                    AND json_extract(o.payload_json, '$.turn_id') = rp.turn_id
                   AND o.state IN ('sent', 'dead_letter', 'superseded')
              )
            LIMIT 250
            """,
            (now,),
        ).fetchall()
        for row in render_rows:
            paths_for_plan = _render_plan_paths(
                paths.attachments / "render",
                str(row["plan_json"]),
            )
            turn_id = str(row["turn_id"])
            render_artifacts.append((turn_id, paths_for_plan))
        event_cutoff = now - config.events_days * 24 * 60 * 60 * 1000
        tool_output_cutoff = now - 30 * 24 * 60 * 60 * 1000
        content_cutoff = now - 180 * 24 * 60 * 60 * 1000
        outbox_payloads = connection.execute(
            """
            UPDATE discord_outbox
            SET event_sequence = NULL,
                payload_json = json_object(
                    'kind', 'retained_tombstone',
                    'original_kind', json_extract(payload_json, '$.kind')
                ),
                updated_at = ?
            WHERE state IN ('sent', 'dead_letter', 'superseded')
              AND updated_at < ?
              AND json_extract(payload_json, '$.kind') IN (
                  'turn_final', 'turn_progress', 'task_card',
                  'schedule_draft_card'
              )
            """,
            (now, content_cutoff),
        ).rowcount
        final_projections = connection.execute(
            """
            DELETE FROM message_projections
            WHERE is_final = 1
              AND turn_id IN (
                  SELECT t.id FROM turns t
                  WHERE t.ended_at IS NOT NULL AND t.ended_at < ?
                    AND EXISTS (
                        SELECT 1 FROM discord_outbox o
                        WHERE o.dedupe_key = 'turn:' || t.id || ':final'
                          AND o.state IN ('sent', 'dead_letter', 'superseded')
                    )
              )
            """,
            (content_cutoff,),
        ).rowcount
        connection.execute(
            f"""
            UPDATE tool_projections
            SET summary_json = json_object(
                    'compacted', 1,
                    'kind', kind,
                    'label', label,
                    'state', state
                )
            WHERE last_event_sequence IN (
                SELECT e.sequence
                FROM events e
                JOIN turns t ON t.id = e.turn_id
                WHERE e.kind LIKE '%.output.delta'
                  AND e.recorded_at < ?
                  AND t.state IN ({_placeholders(_TERMINAL_TURNS)})
            )
            """,
            (tool_output_cutoff, *_TERMINAL_TURNS),
        )
        connection.execute(
            f"""
            UPDATE events
            SET payload_json = json_object('compacted', 1),
                raw_type = NULL,
                raw_hash = NULL,
                raw_size = NULL
            WHERE kind LIKE '%.output.delta'
              AND recorded_at < ?
              AND turn_id IN (
                  SELECT id FROM turns
                  WHERE state IN ({_placeholders(_TERMINAL_TURNS)})
              )
              AND sequence IN (
                  SELECT last_event_sequence FROM tool_projections
              )
            """,
            (tool_output_cutoff, *_TERMINAL_TURNS),
        )
        compacted_tool_events = connection.execute(
            f"""
            DELETE FROM events
            WHERE kind LIKE '%.output.delta'
              AND recorded_at < ?
              AND turn_id IN (
                  SELECT id FROM turns
                  WHERE state IN ({_placeholders(_TERMINAL_TURNS)})
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
            (tool_output_cutoff, *_TERMINAL_TURNS),
        ).rowcount
        connection.execute(
            f"""
            DELETE FROM task_card_views
            WHERE task_projection_id IN (
                SELECT tp.id
                FROM task_projections tp
                JOIN turns t ON t.id = tp.turn_id
                WHERE t.state IN ({_placeholders(_TERMINAL_TURNS)})
                  AND t.ended_at IS NOT NULL
                  AND t.ended_at < ?
            )
              AND NOT EXISTS (
                  SELECT 1
                  FROM discord_outbox o
                  WHERE json_extract(o.payload_json, '$.view_id') = task_card_views.id
                    AND o.state NOT IN ('sent', 'dead_letter', 'superseded')
              )
            """,
            (*_TERMINAL_TURNS, event_cutoff),
        )
        connection.execute(
            f"""
            DELETE FROM task_projection_agents
            WHERE task_projection_id IN (
                SELECT tp.id
                FROM task_projections tp
                JOIN turns t ON t.id = tp.turn_id
                WHERE t.state IN ({_placeholders(_TERMINAL_TURNS)})
                  AND t.ended_at IS NOT NULL
                  AND t.ended_at < ?
            )
              AND NOT EXISTS (
                  SELECT 1
                  FROM task_card_views v
                  WHERE v.task_projection_id =
                        task_projection_agents.task_projection_id
              )
            """,
            (*_TERMINAL_TURNS, event_cutoff),
        )
        connection.execute(
            f"""
            DELETE FROM task_projections
            WHERE turn_id IN (
                SELECT id
                FROM turns
                WHERE state IN ({_placeholders(_TERMINAL_TURNS)})
                  AND ended_at IS NOT NULL
                  AND ended_at < ?
            )
              AND NOT EXISTS (
                  SELECT 1
                  FROM task_card_views v
                  WHERE v.task_projection_id = task_projections.id
              )
            """,
            (*_TERMINAL_TURNS, event_cutoff),
        )
        connection.execute(
            f"""
            DELETE FROM tool_projections
            WHERE turn_id IN (
                SELECT id
                FROM turns
                WHERE state IN ({_placeholders(_TERMINAL_TURNS)})
                  AND ended_at IS NOT NULL
                  AND ended_at < ?
            )
            """,
            (*_TERMINAL_TURNS, event_cutoff),
        )
        connection.execute(
            """
            UPDATE discord_outbox
            SET event_sequence = NULL
            WHERE event_sequence IS NOT NULL
              AND state IN ('sent', 'dead_letter', 'superseded')
              AND updated_at < ?
            """,
            (event_cutoff,),
        )
        events = connection.execute(
            f"""
            DELETE FROM events
            WHERE recorded_at < ?
              AND turn_id IN (
                  SELECT id FROM turns
                  WHERE state IN ({_placeholders(_TERMINAL_TURNS)})
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
            (event_cutoff, *_TERMINAL_TURNS),
        ).rowcount + compacted_tool_events
        event_tombstones = connection.execute(
            f"""
            UPDATE events
            SET payload_json = json_object(
                    'kind', 'retained_tombstone',
                    'original_kind', kind
                ),
                raw_type = NULL,
                raw_hash = NULL,
                raw_size = NULL
            WHERE recorded_at < ?
              AND turn_id IN (
                  SELECT id FROM turns
                  WHERE state IN ({_placeholders(_TERMINAL_TURNS)})
              )
              AND json_extract(payload_json, '$.kind') IS NOT 'retained_tombstone'
            """,
            (event_cutoff, *_TERMINAL_TURNS),
        ).rowcount
        terminal_turn_tombstones = connection.execute(
            f"""
            UPDATE turns
            SET provider_turn_id = NULL,
                runtime_lease_id = NULL,
                runtime_generation = NULL,
                effective_skill_names_json = NULL,
                effective_model = NULL,
                effective_reasoning_effort = NULL,
                effective_reasoning_summary = NULL,
                effective_personality = NULL,
                effective_service_tier = NULL,
                error_message_redacted = NULL,
                usage_scope = NULL,
                requested_by_user_id = NULL,
                retained_at = ?
            WHERE state IN ({_placeholders(_TERMINAL_TURNS)})
              AND ended_at IS NOT NULL
              AND ended_at < ?
              AND retained_at IS NULL
            """,
            (now, *_TERMINAL_TURNS, event_cutoff),
        ).rowcount
        connection.execute(
            f"""
            UPDATE schedule_fires
            SET scheduled_for = NULL,
                scheduled_local = '[retained]',
                error_code = NULL,
                retained_at = ?
            WHERE retained_at IS NULL
              AND turn_id IN (
                  SELECT id FROM turns
                  WHERE state IN ({_placeholders(_TERMINAL_TURNS)})
                    AND ended_at IS NOT NULL
                    AND ended_at < ?
              )
            """,
            (now, *_TERMINAL_TURNS, event_cutoff),
        )
        intent_cutoff = now - 90 * 24 * 60 * 60 * 1000
        command_intents = connection.execute(
            """
            DELETE FROM command_intents
            WHERE completed_at IS NOT NULL AND completed_at < ?
              AND state IN ('succeeded', 'rejected', 'failed')
            """,
            (intent_cutoff,),
        ).rowcount
        connection.execute(
            """
            UPDATE modal_intents
            SET state = 'expired'
            WHERE state = 'open' AND expires_at <= ?
            """,
            (now,),
        )
        modal_intents = connection.execute(
            """
            DELETE FROM modal_intents
            WHERE state IN ('consumed', 'expired')
              AND COALESCE(consumed_at, expires_at) < ?
            """,
            (intent_cutoff,),
        ).rowcount
        connection.execute(
            """
            UPDATE schedule_drafts
            SET state = 'expired', payload_json = '{}', updated_at = ?
            WHERE state = 'pending' AND expires_at <= ?
            """,
            (now, now),
        )
        outbox_payloads += connection.execute(
            """
            UPDATE discord_outbox
            SET payload_json = json_object(
                    'kind', 'retained_tombstone',
                    'original_kind', 'schedule_draft_card'
                ),
                updated_at = ?
            WHERE state IN ('sent', 'dead_letter', 'superseded')
              AND json_extract(payload_json, '$.kind') = 'schedule_draft_card'
              AND json_extract(payload_json, '$.draft_id') IN (
                  SELECT id FROM schedule_drafts
                  WHERE updated_at < ?
                    AND state IN ('confirmed', 'cancelled', 'expired')
              )
            """,
            (now, intent_cutoff),
        ).rowcount
        schedule_drafts = connection.execute(
            """
            DELETE FROM schedule_drafts
            WHERE updated_at < ?
              AND state IN ('confirmed', 'cancelled', 'expired')
              AND NOT EXISTS (
                  SELECT 1 FROM discord_outbox o
                  WHERE json_extract(o.payload_json, '$.kind') = 'schedule_draft_card'
                    AND json_extract(o.payload_json, '$.draft_id') = schedule_drafts.id
                    AND o.state NOT IN ('sent', 'dead_letter', 'superseded')
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM dynamic_tool_invocations dti
                  JOIN turns t ON t.id = dti.turn_id
                  WHERE dti.draft_id = schedule_drafts.id
                    AND t.state IN ('queued', 'starting', 'running', 'cancelling')
              )
            """,
            (intent_cutoff,),
        ).rowcount
        schedule_fires = connection.execute(
            """
            DELETE FROM schedule_fires
            WHERE turn_id IS NULL AND created_at < ?
              AND state IN ('skipped', 'blocked')
            """,
            (event_cutoff,),
        ).rowcount
        incidents = connection.execute(
            """
            DELETE FROM incidents
            WHERE resolved_at IS NOT NULL AND resolved_at < ?
            """,
            (content_cutoff,),
        ).rowcount
        audit_entries = connection.execute(
            "DELETE FROM audit_log WHERE occurred_at < ?",
            (content_cutoff,),
        ).rowcount
    removed_attachment_ids = tuple(
        attachment_id
        for attachment_id, path in input_artifacts
        if _unlink_artifact(
            path,
            artifact_id=attachment_id,
            artifact_kind="input_attachment",
        )
    )
    removed_render_turn_ids: list[str] = []
    for turn_id, artifact_paths in render_artifacts:
        removed_all = True
        for index, path in enumerate(artifact_paths):
            removed_all = (
                _unlink_artifact(
                    path,
                    artifact_id=f"{turn_id}:{index}",
                    artifact_kind="render_attachment",
                )
                and removed_all
            )
        if removed_all:
            removed_render_turn_ids.append(turn_id)
    with store.transaction() as connection:
        if removed_attachment_ids:
            connection.executemany(
                "DELETE FROM attachments WHERE id = ?",
                ((attachment_id,) for attachment_id in removed_attachment_ids),
            )
        if removed_render_turn_ids:
            connection.executemany(
                "DELETE FROM discord_render_plans WHERE turn_id = ?",
                ((turn_id,) for turn_id in removed_render_turn_ids),
            )
    terminal_turns, linked_schedule_fires = _delete_expired_terminal_turns(
        store,
        older_than_ms=content_cutoff,
    )
    orphan_artifacts = _sweep_orphan_artifacts(
        store,
        paths,
        older_than_ms=now - 24 * 60 * 60 * 1000,
    )
    log_files = _remove_old_logs(
        paths.log_file,
        older_than_ms=now - config.logs_days * 24 * 60 * 60 * 1000,
    )
    return RetentionResult(
        input_attachments=len(removed_attachment_ids),
        render_plans=len(removed_render_turn_ids),
        events=events,
        event_tombstones=event_tombstones,
        terminal_turn_tombstones=terminal_turn_tombstones,
        terminal_turns=terminal_turns,
        command_intents=command_intents,
        modal_intents=modal_intents,
        schedule_drafts=schedule_drafts,
        final_projections=final_projections,
        outbox_payloads=outbox_payloads,
        schedule_fires=schedule_fires + linked_schedule_fires,
        incidents=incidents,
        audit_entries=audit_entries,
        log_files=log_files,
        orphan_artifacts=orphan_artifacts,
    )


def _render_plan_paths(root: Path, plan_json: str) -> tuple[Path, ...]:
    try:
        payload = json.loads(plan_json)
    except json.JSONDecodeError as exc:
        raise StorageError("stored render plan JSON is invalid") from exc
    attachments = payload.get("attachments") if isinstance(payload, dict) else None
    if not isinstance(attachments, list):
        raise StorageError("stored render plan attachments are invalid")
    result: list[Path] = []
    for attachment in attachments:
        relative = (
            attachment.get("relative_path")
            if isinstance(attachment, dict)
            else None
        )
        if not isinstance(relative, str):
            raise StorageError("stored render plan path is invalid")
        result.append(_safe_relative_path(root, relative))
    return tuple(result)


def _delete_expired_terminal_turns(
    store: SQLiteStore,
    *,
    older_than_ms: int,
) -> tuple[int, int]:
    with store.transaction() as connection:
        rows = connection.execute(
            f"""
            SELECT t.id
            FROM turns t
            WHERE t.state IN ({_placeholders(_TERMINAL_TURNS)})
              AND t.retained_at IS NOT NULL
              AND t.ended_at IS NOT NULL
              AND t.ended_at < ?
              AND NOT EXISTS (
                  SELECT 1 FROM attachments a WHERE a.turn_id = t.id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM discord_render_plans rp WHERE rp.turn_id = t.id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM message_projections mp WHERE mp.turn_id = t.id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM tool_projections tp WHERE tp.turn_id = t.id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM task_projections tp WHERE tp.turn_id = t.id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM events e WHERE e.turn_id = t.id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM discord_outbox o
                  WHERE json_extract(o.payload_json, '$.turn_id') = t.id
                    AND o.state NOT IN ('sent', 'dead_letter', 'superseded')
              )
            ORDER BY t.ended_at, t.enqueue_sequence
            LIMIT 500
            """,
            (*_TERMINAL_TURNS, older_than_ms),
        ).fetchall()
        turn_ids = tuple(str(row["id"]) for row in rows)
        if not turn_ids:
            return 0, 0
        placeholders = _placeholders(turn_ids)
        linked_schedule_fires = int(
            connection.execute(
                f"""
                SELECT COUNT(*)
                FROM schedule_fires
                WHERE turn_id IN ({placeholders})
                """,
                turn_ids,
            ).fetchone()[0]
        )
        connection.execute(
            f"DELETE FROM turn_progress_views WHERE turn_id IN ({placeholders})",
            turn_ids,
        )
        connection.execute(
            f"""
            DELETE FROM modal_intents
            WHERE turn_id IN ({placeholders})
              AND state IN ('consumed', 'expired')
            """,
            turn_ids,
        )
        for table in ("ingress_messages", "command_intents", "incidents", "audit_log"):
            connection.execute(
                f"UPDATE {table} SET turn_id = NULL WHERE turn_id IN ({placeholders})",
                turn_ids,
            )
        deleted = connection.execute(
            f"DELETE FROM turns WHERE id IN ({placeholders})",
            turn_ids,
        ).rowcount
        return deleted, linked_schedule_fires


def _safe_relative_path(root: Path, value: str) -> Path:
    relative = Path(value)
    if relative.is_absolute() or ".." in relative.parts:
        raise StorageError("retention artifact path escapes its root")
    root_resolved = root.resolve()
    candidate = root_resolved / relative
    if not candidate.parent.resolve().is_relative_to(root_resolved):
        raise StorageError("retention artifact path escapes its root")
    return candidate


def _unlink_artifact(
    path: Path,
    *,
    artifact_id: str,
    artifact_kind: str,
) -> bool:
    descriptor = -1
    try:
        if path.is_symlink():
            path.unlink()
            return True
        if not path.is_file():
            return True
        if _FCNTL is not None and hasattr(os, "O_NOFOLLOW"):
            descriptor = os.open(
                path,
                os.O_RDONLY | os.O_NOFOLLOW | getattr(os, "O_CLOEXEC", 0),
            )
            _FCNTL.flock(descriptor, _FCNTL.LOCK_EX | _FCNTL.LOCK_NB)
            opened = os.fstat(descriptor)
            named = path.lstat()
            if opened.st_dev != named.st_dev or opened.st_ino != named.st_ino:
                return False
        path.unlink()
        return True
    except FileNotFoundError:
        return True
    except BlockingIOError:
        return False
    except OSError as exc:
        logger.warning(
            "Retention artifact removal failed",
            extra={
                "stable_code": "retention_artifact_unlink_failed",
                "artifact_id": artifact_id,
                "artifact_kind": artifact_kind,
                "exception_type": type(exc).__name__,
            },
        )
        return False
    finally:
        if descriptor >= 0:
            if _FCNTL is not None:
                with suppress(OSError):
                    _FCNTL.flock(descriptor, _FCNTL.LOCK_UN)
            with suppress(OSError):
                os.close(descriptor)


def _remove_old_logs(path: Path, *, older_than_ms: int) -> int:
    removed = 0
    for candidate in path.parent.glob(f"{path.name}.*"):
        if (
            candidate.is_file()
            and not candidate.is_symlink()
            and int(candidate.stat().st_mtime * 1000) < older_than_ms
        ):
            artifact_id = hashlib.sha256(candidate.name.encode()).hexdigest()[:16]
            if _unlink_artifact(
                candidate,
                artifact_id=artifact_id,
                artifact_kind="log",
            ):
                removed += 1
    return removed


def _sweep_orphan_artifacts(
    store: SQLiteStore,
    paths: AppPaths,
    *,
    older_than_ms: int,
) -> int:
    referenced: set[Path] = set()
    for row in store.query_all(
        """
        SELECT relative_path FROM attachments
        WHERE kind IN ('input_image', 'input_file')
        """
    ):
        referenced.add(
            _safe_relative_path(paths.data_dir, str(row["relative_path"]))
        )
    render_root = paths.attachments / "render"
    for row in store.query_all("SELECT plan_json FROM discord_render_plans"):
        referenced.update(_render_plan_paths(render_root, str(row["plan_json"])))

    removed = 0
    for root in (
        paths.attachments / "input",
        render_root,
        paths.attachments / ".quarantine",
    ):
        if not root.exists() or root.is_symlink():
            continue
        for candidate in root.rglob("*"):
            if removed >= 1000:
                return removed
            if (
                (candidate.is_file() or candidate.is_symlink())
                and candidate not in referenced
                and int(candidate.lstat().st_mtime * 1000) < older_than_ms
            ):
                artifact_id = hashlib.sha256(
                    str(candidate.relative_to(root)).encode()
                ).hexdigest()[:16]
                if _unlink_artifact(
                    candidate,
                    artifact_id=artifact_id,
                    artifact_kind="orphan_attachment",
                ):
                    removed += 1
    return removed


def _placeholders(values: tuple[str, ...]) -> str:
    return ",".join("?" for _value in values)
