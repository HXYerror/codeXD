from __future__ import annotations

import hashlib
import importlib.resources
import os
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from codexd import __version__
from codexd.domain.ids import utc_now_ms
from codexd.errors import StorageError


@dataclass(frozen=True)
class Migration:
    version: int
    name: str
    checksum: str
    sql: str


class SQLiteStore:
    def __init__(self, path: Path, *, busy_timeout_ms: int = 5000) -> None:
        self.path = path
        self.busy_timeout_ms = busy_timeout_ms
        self._connection: sqlite3.Connection | None = None
        self._lock = threading.RLock()

    def __enter__(self) -> SQLiteStore:
        self.open()
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        self.close()

    @property
    def connection(self) -> sqlite3.Connection:
        if self._connection is None:
            raise StorageError("database is not open")
        return self._connection

    def open(self) -> None:
        with self._lock:
            if self._connection is not None:
                return
            self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            try:
                connection = sqlite3.connect(
                    self.path,
                    isolation_level=None,
                    check_same_thread=False,
                    timeout=self.busy_timeout_ms / 1000,
                )
                connection.row_factory = sqlite3.Row
                connection.execute("PRAGMA foreign_keys = ON")
                connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
                connection.execute("PRAGMA journal_mode = WAL")
                connection.execute("PRAGMA synchronous = FULL")
            except sqlite3.Error as exc:
                raise StorageError(f"cannot open database: {exc}") from exc
            self._connection = connection
            if os.name != "nt":
                self.path.chmod(0o600)

    def close(self) -> None:
        with self._lock:
            if self._connection is None:
                return
            try:
                self._connection.execute("PRAGMA wal_checkpoint(PASSIVE)")
                self._connection.close()
            finally:
                self._connection = None

    @contextmanager
    def transaction(self, *, immediate: bool = True) -> Iterator[sqlite3.Connection]:
        with self._lock:
            connection = self.connection
            try:
                connection.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
                yield connection
            except BaseException:
                connection.rollback()
                raise
            else:
                connection.commit()

    def migrate(self) -> int:
        with self._lock:
            connection = self.connection
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    checksum TEXT NOT NULL,
                    applied_at INTEGER NOT NULL,
                    codexd_version TEXT NOT NULL
                )
                """
            )
            applied = {
                int(row["version"]): row
                for row in connection.execute(
                    "SELECT version, name, checksum FROM schema_migrations"
                ).fetchall()
            }
            migrations = _load_migrations()
            known_versions = {migration.version for migration in migrations}
            unknown = sorted(set(applied) - known_versions)
            if unknown:
                raise StorageError(f"database has unknown migration versions: {unknown}")

            for migration in migrations:
                existing = applied.get(migration.version)
                if existing is not None:
                    if (
                        existing["name"] != migration.name
                        or existing["checksum"] != migration.checksum
                    ):
                        raise StorageError(
                            f"migration checksum mismatch for {migration.version}: {migration.name}"
                        )
                    continue
                self._apply_migration(migration)
            return max((migration.version for migration in migrations), default=0)

    def _apply_migration(self, migration: Migration) -> None:
        requires_foreign_keys_off = (
            "-- codexd:foreign_keys_off" in migration.sql
        )
        connection = self.connection
        if requires_foreign_keys_off:
            connection.execute("PRAGMA foreign_keys = OFF")
        try:
            with self.transaction() as connection:
                for statement in _sql_statements(migration.sql):
                    connection.execute(statement)
                if requires_foreign_keys_off:
                    violation = connection.execute(
                        "PRAGMA foreign_key_check"
                    ).fetchone()
                    if violation is not None:
                        raise sqlite3.IntegrityError(
                            "migration produced a foreign key violation"
                        )
                connection.execute(
                    """
                    INSERT INTO schema_migrations(
                        version, name, checksum, applied_at, codexd_version
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (
                        migration.version,
                        migration.name,
                        migration.checksum,
                        utc_now_ms(),
                        __version__,
                    ),
                )
        except sqlite3.Error as exc:
            raise StorageError(
                f"migration {migration.version} ({migration.name}) failed: {exc}"
            ) from exc
        finally:
            if requires_foreign_keys_off:
                connection.execute("PRAGMA foreign_keys = ON")

    def integrity_check(self) -> str:
        with self._lock:
            row = self.connection.execute("PRAGMA integrity_check").fetchone()
            return str(row[0]) if row else "no result"

    def validate_schema(self) -> int:
        with self._lock:
            return _validate_schema_connection(self.connection)

    def foreign_key_check(self) -> tuple[sqlite3.Row, ...]:
        with self._lock:
            return tuple(self.connection.execute("PRAGMA foreign_key_check").fetchall())

    def checkpoint(self, mode: str = "FULL") -> tuple[int, int, int]:
        if mode not in {"PASSIVE", "FULL", "RESTART", "TRUNCATE"}:
            raise StorageError(f"invalid WAL checkpoint mode: {mode}")
        with self._lock:
            row = self.connection.execute(f"PRAGMA wal_checkpoint({mode})").fetchone()
        if row is None:
            raise StorageError("database WAL checkpoint returned no result")
        return int(row[0]), int(row[1]), int(row[2])

    def backup(self, destination: Path) -> Path:
        source = self.path.resolve()
        target_path = destination.expanduser().resolve()
        if target_path == source:
            raise StorageError("database backup destination must differ from the source")
        if target_path.exists():
            raise StorageError(f"database backup already exists: {target_path}")
        target_path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        temporary = target_path.with_name(
            f".{target_path.name}.{os.getpid()}.tmp"
        )
        if temporary.exists():
            raise StorageError(f"database backup staging file exists: {temporary}")
        with self._lock:
            try:
                checkpoint = self.checkpoint("FULL")
                if checkpoint[0] != 0:
                    raise StorageError("database WAL checkpoint is busy")
                with sqlite3.connect(temporary) as target:
                    target.row_factory = sqlite3.Row
                    target.execute("PRAGMA foreign_keys = ON")
                    self.connection.backup(target)
                    row = target.execute("PRAGMA integrity_check").fetchone()
                    if row is None or row[0] != "ok":
                        raise StorageError("database backup integrity check failed")
                    foreign_key_violation = target.execute(
                        "PRAGMA foreign_key_check"
                    ).fetchone()
                    if foreign_key_violation is not None:
                        raise StorageError(
                            "database backup foreign key check failed"
                        )
                    _validate_schema_connection(target)
                if os.name != "nt":
                    temporary.chmod(0o600)
                os.replace(temporary, target_path)
            except sqlite3.Error as exc:
                raise StorageError(f"database backup failed: {exc}") from exc
            finally:
                temporary.unlink(missing_ok=True)
        if os.name != "nt":
            target_path.chmod(0o600)
        return target_path

    def query_one(self, sql: str, parameters: tuple[object, ...] = ()) -> sqlite3.Row | None:
        with self._lock:
            return cast(sqlite3.Row | None, self.connection.execute(sql, parameters).fetchone())

    def query_all(
        self, sql: str, parameters: tuple[object, ...] = ()
    ) -> tuple[sqlite3.Row, ...]:
        with self._lock:
            return tuple(self.connection.execute(sql, parameters).fetchall())


def _load_migrations() -> tuple[Migration, ...]:
    root = importlib.resources.files("codexd.storage.migrations")
    migrations: list[Migration] = []
    for entry in sorted(root.iterdir(), key=lambda path: path.name):
        if not entry.name.endswith(".sql"):
            continue
        prefix, separator, name = entry.name.partition("_")
        if not separator or not prefix.isdigit():
            raise StorageError(f"invalid migration filename: {entry.name}")
        sql = entry.read_text(encoding="utf-8")
        migrations.append(
            Migration(
                version=int(prefix),
                name=name.removesuffix(".sql"),
                checksum=hashlib.sha256(sql.encode("utf-8")).hexdigest(),
                sql=sql,
            )
        )
    versions = [migration.version for migration in migrations]
    if versions != sorted(set(versions)):
        raise StorageError("migration versions must be unique and ordered")
    return tuple(migrations)


def _validate_schema_connection(connection: sqlite3.Connection) -> int:
    table = connection.execute(
        """
        SELECT 1 FROM sqlite_master
        WHERE type = 'table' AND name = 'schema_migrations'
        """
    ).fetchone()
    if table is None:
        raise StorageError("database schema is not initialized")
    applied = {
        int(row["version"]): row
        for row in connection.execute(
            "SELECT version, name, checksum FROM schema_migrations"
        ).fetchall()
    }
    migrations = _load_migrations()
    expected_versions = {migration.version for migration in migrations}
    unknown = sorted(set(applied) - expected_versions)
    missing = sorted(expected_versions - set(applied))
    if unknown:
        raise StorageError(f"database has unknown migration versions: {unknown}")
    for migration in migrations:
        existing = applied.get(migration.version)
        if existing is not None and (
            existing["name"] != migration.name
            or existing["checksum"] != migration.checksum
        ):
            raise StorageError(
                f"migration checksum mismatch for "
                f"{migration.version}: {migration.name}"
            )
    if missing:
        raise StorageError(f"database has unapplied migrations: {missing}")
    return max(expected_versions, default=0)


def _sql_statements(script: str) -> Iterator[str]:
    buffer: list[str] = []
    for line in script.splitlines():
        buffer.append(line)
        candidate = "\n".join(buffer).strip()
        if candidate and sqlite3.complete_statement(candidate):
            yield candidate
            buffer.clear()
    remainder = "\n".join(buffer).strip()
    if remainder:
        raise StorageError("migration contains incomplete SQL")
