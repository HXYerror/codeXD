from __future__ import annotations

import sqlite3

from codexd.domain.ids import canonical_json, new_id, utc_now_ms
from codexd.errors import ConflictError
from codexd.storage.records import MaterializedAttachmentRecord
from codexd.storage.sqlite import SQLiteStore


class MaterializedAttachmentRepository:
    def __init__(self, store: SQLiteStore) -> None:
        self._store = store

    def for_attachment(
        self,
        attachment_id: str,
    ) -> MaterializedAttachmentRecord | None:
        row = self._store.query_one(
            "SELECT * FROM materialized_attachments WHERE attachment_id = ?",
            (attachment_id,),
        )
        return _record(row) if row is not None else None

    def register(
        self,
        *,
        attachment_id: str,
        turn_id: str,
        kind: str,
        root_relative_path: str,
        manifest: dict[str, object],
        manifest_hash: str,
        file_count: int,
        total_bytes: int,
        retention_until: int,
    ) -> MaterializedAttachmentRecord:
        manifest_json = canonical_json(manifest)
        now = utc_now_ms()
        with self._store.transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM materialized_attachments WHERE attachment_id = ?",
                (attachment_id,),
            ).fetchone()
            expected = (
                turn_id,
                kind,
                root_relative_path,
                manifest_json,
                manifest_hash,
                file_count,
                total_bytes,
                retention_until,
            )
            if existing is not None:
                actual = (
                    existing["turn_id"],
                    existing["kind"],
                    existing["root_relative_path"],
                    existing["manifest_json"],
                    existing["manifest_hash"],
                    int(existing["file_count"]),
                    int(existing["total_bytes"]),
                    int(existing["retention_until"]),
                )
                if actual != expected:
                    raise ConflictError("materialized attachment identity changed")
                return _record(existing)
            materialized_id = new_id()
            connection.execute(
                """
                INSERT INTO materialized_attachments(
                    id, attachment_id, turn_id, kind, root_relative_path,
                    manifest_json, manifest_hash, file_count, total_bytes,
                    retention_until, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    materialized_id,
                    attachment_id,
                    turn_id,
                    kind,
                    root_relative_path,
                    manifest_json,
                    manifest_hash,
                    file_count,
                    total_bytes,
                    retention_until,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT * FROM materialized_attachments WHERE id = ?",
                (materialized_id,),
            ).fetchone()
            assert row is not None
            return _record(row)

    def remove_for_attachment(self, attachment_id: str) -> None:
        with self._store.transaction() as connection:
            connection.execute(
                "DELETE FROM materialized_attachments WHERE attachment_id = ?",
                (attachment_id,),
            )


def _record(row: sqlite3.Row) -> MaterializedAttachmentRecord:
    return MaterializedAttachmentRecord(
        id=str(row["id"]),
        attachment_id=str(row["attachment_id"]),
        turn_id=str(row["turn_id"]),
        kind=str(row["kind"]),
        root_relative_path=str(row["root_relative_path"]),
        manifest_json=str(row["manifest_json"]),
        manifest_hash=str(row["manifest_hash"]),
        file_count=int(row["file_count"]),
        total_bytes=int(row["total_bytes"]),
        retention_until=int(row["retention_until"]),
        created_at=int(row["created_at"]),
    )
