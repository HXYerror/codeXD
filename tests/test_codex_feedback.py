from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest

from codexd.errors import ConflictError
from codexd.storage import codex_feedback
from codexd.storage.codex_feedback import (
    codex_feedback_log_path,
    compact_codex_feedback_logs,
)


def test_codex_feedback_compaction_removes_transport_trace_and_old_detail(
    tmp_path: Path,
) -> None:
    path = tmp_path / "sqlite" / "logs_2.sqlite"
    path.parent.mkdir()
    now = 2_000_000_000
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER NOT NULL,
                ts_nanos INTEGER NOT NULL,
                level TEXT NOT NULL,
                target TEXT NOT NULL,
                feedback_log_body TEXT,
                estimated_bytes INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        rows = (
            (
                now,
                "TRACE",
                "codex_http_client::transport",
                "sensitive request " + ("x" * 1_000_000),
            ),
            (now, "TRACE", "codex_core::session", "recent trace"),
            (now - 25 * 60 * 60, "DEBUG", "codex_core", "old debug"),
            (now - 8 * 24 * 60 * 60, "INFO", "codex_core", "old info"),
            (now, "WARN", "codex_core", "recent warning"),
        )
        connection.executemany(
            """
            INSERT INTO logs(
                ts, ts_nanos, level, target, feedback_log_body, estimated_bytes
            ) VALUES (?, 0, ?, ?, ?, length(?))
            """,
            ((ts, level, target, body, body) for ts, level, target, body in rows),
        )
    before = path.stat().st_size
    backup = tmp_path / "backup" / "logs.sqlite"

    result = compact_codex_feedback_logs(
        path,
        retention_days=7,
        trace_hours=24,
        backup_path=backup,
        now_seconds=now,
    )

    assert result.before_rows == 5
    assert result.after_rows == 2
    assert result.deleted_rows == 3
    assert result.after_bytes < before
    assert backup.exists()
    with sqlite3.connect(path) as connection:
        retained = connection.execute(
            "SELECT level, target, feedback_log_body FROM logs ORDER BY id"
        ).fetchall()
        assert retained == [
            ("TRACE", "codex_core::session", "recent trace"),
            ("WARN", "codex_core", "recent warning"),
        ]
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
    with sqlite3.connect(backup) as connection:
        assert connection.execute("SELECT COUNT(*) FROM logs").fetchone()[0] == 5


def test_codex_feedback_compaction_rejects_open_database(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = tmp_path / "logs_2.sqlite"
    with sqlite3.connect(path) as connection:
        connection.execute(
            """
            CREATE TABLE logs (
                ts INTEGER NOT NULL,
                level TEXT NOT NULL,
                target TEXT NOT NULL,
                feedback_log_body TEXT
            )
            """
        )
    monkeypatch.setattr(codex_feedback, "_processes_using", lambda _path: (123,))

    with pytest.raises(ConflictError, match="still open"):
        compact_codex_feedback_logs(
            path,
            retention_days=7,
            trace_hours=24,
            backup_path=None,
        )


def test_codex_feedback_path_honors_sqlite_home(tmp_path: Path) -> None:
    codex_home = tmp_path / "codex-home"
    codex_home.mkdir()
    configured = tmp_path / "configured-state"
    (codex_home / "config.toml").write_text(
        f'sqlite_home = "{configured}"\n',
        encoding="utf-8",
    )

    assert codex_feedback_log_path(
        {
            "CODEX_HOME": str(codex_home),
            "CODEX_SQLITE_HOME": str(tmp_path / "environment-state"),
        }
    ) == (configured / "logs_2.sqlite").resolve()
