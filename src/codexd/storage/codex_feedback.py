from __future__ import annotations

import os
import sqlite3
import time
import tomllib
from dataclasses import asdict, dataclass
from pathlib import Path

import psutil

from codexd.errors import ConflictError, StorageError


@dataclass(frozen=True)
class CodexFeedbackCompactionResult:
    path: str
    before_bytes: int
    after_bytes: int
    before_rows: int
    after_rows: int
    deleted_rows: int

    def as_dict(self) -> dict[str, int | str]:
        return asdict(self)


def codex_feedback_log_path(
    environment: dict[str, str],
    *,
    cwd: Path | None = None,
) -> Path:
    working_directory = Path.cwd() if cwd is None else cwd
    home = Path(
        environment.get("CODEX_HOME")
        or Path(environment.get("HOME") or environment.get("USERPROFILE") or Path.home())
        / ".codex"
    ).expanduser()
    sqlite_home = environment.get("CODEX_SQLITE_HOME")
    try:
        config = tomllib.loads((home / "config.toml").read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError):
        config = {}
    configured = config.get("sqlite_home") if isinstance(config, dict) else None
    if isinstance(configured, str) and configured:
        sqlite_home = configured
    root = Path(sqlite_home).expanduser() if sqlite_home else home
    if not root.is_absolute():
        root = working_directory / root
    return (root / "logs_2.sqlite").resolve()


def compact_codex_feedback_logs(
    path: Path,
    *,
    retention_days: int,
    trace_hours: int,
    backup_path: Path | None,
    now_seconds: int | None = None,
) -> CodexFeedbackCompactionResult:
    if retention_days < 1 or trace_hours < 1:
        raise ValueError("Codex feedback retention must be positive")
    try:
        target = path.expanduser().resolve(strict=True)
    except OSError as exc:
        raise StorageError(f"Codex feedback log database is unavailable: {path}") from exc
    users = _processes_using(target)
    if users:
        raise ConflictError(
            "Codex feedback log database is still open by "
            f"{len(users)} process(es); stop codexD and all Codex apps first"
        )
    before_bytes = target.stat().st_size
    now = int(time.time()) if now_seconds is None else now_seconds
    oldest = now - retention_days * 24 * 60 * 60
    trace_cutoff = now - trace_hours * 60 * 60
    try:
        connection = sqlite3.connect(target, isolation_level=None, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA busy_timeout = 5000")
        columns = {
            str(row["name"])
            for row in connection.execute("PRAGMA table_info(logs)").fetchall()
        }
        required = {"ts", "level", "target", "feedback_log_body"}
        if not required.issubset(columns):
            raise StorageError("Codex feedback log schema is unsupported")
        checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if checkpoint is None or int(checkpoint[0]) != 0:
            raise StorageError("Codex feedback WAL checkpoint is busy")
        before_rows = int(connection.execute("SELECT COUNT(*) FROM logs").fetchone()[0])
        if backup_path is not None:
            _backup(connection, target, backup_path)
        connection.execute("BEGIN IMMEDIATE")
        try:
            deleted_rows = connection.execute(
                """
                DELETE FROM logs
                WHERE ts < ?
                   OR (
                       level IN ('TRACE', 'DEBUG')
                       AND ts < ?
                   )
                   OR (
                       level = 'TRACE'
                       AND target = 'codex_http_client::transport'
                   )
                """,
                (oldest, trace_cutoff),
            ).rowcount
        except BaseException:
            connection.rollback()
            raise
        else:
            connection.commit()
        connection.execute("VACUUM")
        connection.execute("PRAGMA optimize")
        checkpoint = connection.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        if checkpoint is None or int(checkpoint[0]) != 0:
            raise StorageError("Codex feedback WAL checkpoint is busy after vacuum")
        after_rows = int(connection.execute("SELECT COUNT(*) FROM logs").fetchone()[0])
        integrity = connection.execute("PRAGMA integrity_check").fetchone()
        if integrity is None or integrity[0] != "ok":
            raise StorageError("Codex feedback log integrity check failed")
    except StorageError:
        raise
    except sqlite3.Error as exc:
        raise StorageError(f"Codex feedback log compaction failed: {exc}") from exc
    finally:
        if "connection" in locals():
            connection.close()
    after_bytes = target.stat().st_size
    if os.name != "nt":
        target.chmod(0o600)
    return CodexFeedbackCompactionResult(
        path=str(target),
        before_bytes=before_bytes,
        after_bytes=after_bytes,
        before_rows=before_rows,
        after_rows=after_rows,
        deleted_rows=deleted_rows,
    )


def _backup(connection: sqlite3.Connection, source: Path, destination: Path) -> None:
    target = destination.expanduser().resolve()
    if target == source:
        raise StorageError("Codex feedback backup destination must differ from source")
    if target.exists():
        raise StorageError(f"Codex feedback backup already exists: {target}")
    target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    if temporary.exists():
        raise StorageError(f"Codex feedback backup staging file exists: {temporary}")
    try:
        with sqlite3.connect(temporary) as backup:
            connection.backup(backup)
            integrity = backup.execute("PRAGMA integrity_check").fetchone()
            if integrity is None or integrity[0] != "ok":
                raise StorageError("Codex feedback backup integrity check failed")
        if os.name != "nt":
            temporary.chmod(0o600)
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)


def _processes_using(path: Path) -> tuple[int, ...]:
    users: list[int] = []
    current_pid = os.getpid()
    for process in psutil.process_iter(["pid"]):
        try:
            pid = int(process.info["pid"])
            if pid == current_pid:
                continue
            if any(Path(item.path).resolve() == path for item in process.open_files()):
                users.append(pid)
        except (OSError, psutil.Error):
            continue
    return tuple(sorted(users))
