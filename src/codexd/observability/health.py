from __future__ import annotations

import asyncio
import json
import os
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from codexd.domain.ids import utc_now_ms
from codexd.storage.repository import Repository

RuntimeStatus = Callable[[], Awaitable[dict[str, int | str]]]
EventMetrics = Callable[[], dict[str, float | int]]
DiscordEgressMetrics = Callable[[], dict[str, float | int]]
DiscordReconnectStatus = Callable[[], dict[str, Any]]


@dataclass
class HealthReporter:
    path: Path
    repository: Repository
    runtime_status: RuntimeStatus
    boot_id: str
    process_start_token: str
    started_at: int
    sdk_version: str = "unknown"
    runtime_version: str = "unknown"
    runtime_sqlite_root: Path | None = None
    database_size_budget_bytes: int = 512 * 1024 * 1024
    runtime_sqlite_size_budget_bytes: int = 1024 * 1024 * 1024
    event_metrics: EventMetrics | None = None
    discord_egress_metrics: DiscordEgressMetrics | None = None
    discord_reconnect_status: DiscordReconnectStatus | None = None
    critical_failure: Callable[[BaseException], None] | None = None

    def __post_init__(self) -> None:
        self.service = "starting"
        self.discord = "disconnected"
        self.database = "healthy"
        self.codex_auth = "unknown"
        self.codex_auth_observed_at: int | None = None
        self.discord_reconnect_count = 0
        self._discord_ready_observed = False
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._critical_failure = self.critical_failure or (lambda _exc: None)
        self._last_storage_sample: tuple[int, int, int] | None = None

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="codexd-health")

    def observe_discord(self, status: str) -> None:
        if status in {"ready", "degraded"}:
            if self._discord_ready_observed and self.discord in {
                "connecting",
                "disconnected",
                "reconnecting",
            }:
                self.discord_reconnect_count += 1
            self._discord_ready_observed = True
        self.discord = status

    def observe_codex_auth(self, status: str) -> None:
        if status not in {"authenticated", "required", "unknown"}:
            raise ValueError("invalid Codex authentication status")
        self.codex_auth = status
        self.codex_auth_observed_at = utc_now_ms()

    async def close(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def write(self) -> None:
        await asyncio.to_thread(self.repository.heartbeat_daemon_lease, self.boot_id)
        counts = await asyncio.to_thread(self.repository.health_counts)
        inbound = await asyncio.to_thread(
            self.repository.ingress_reconciliation_counts
        )
        runtime = await self.runtime_status()
        now = utc_now_ms()
        storage = await asyncio.to_thread(
            _storage_metrics,
            self.repository.store.path,
            self.runtime_sqlite_root,
        )
        total_bytes = int(storage["total_bytes"])
        event_sequence = counts["event_sequence"]
        growth_bytes_per_minute = 0
        event_rows_per_minute = 0
        if self._last_storage_sample is not None:
            sampled_at, previous_bytes, previous_sequence = self._last_storage_sample
            elapsed_ms = max(1, now - sampled_at)
            growth_bytes_per_minute = max(
                0,
                int((total_bytes - previous_bytes) * 60_000 / elapsed_ms),
            )
            event_rows_per_minute = max(
                0,
                int((event_sequence - previous_sequence) * 60_000 / elapsed_ms),
            )
        self._last_storage_sample = (now, total_bytes, event_sequence)
        storage["growth_bytes_per_minute"] = growth_bytes_per_minute
        storage["event_rows_per_minute"] = event_rows_per_minute
        write_latency = self.repository.store.write_latency_snapshot()
        event_metrics = self.event_metrics() if self.event_metrics is not None else {}
        discord_egress = (
            self.discord_egress_metrics()
            if self.discord_egress_metrics is not None
            else {}
        )
        discord_connection = (
            self.discord_reconnect_status()
            if self.discord_reconnect_status is not None
            else {"connection_state": self.discord}
        )
        storage["write_latency"] = write_latency
        pressure_reasons: list[str] = []
        if int(storage["codexd_total_bytes"]) > self.database_size_budget_bytes:
            pressure_reasons.append("codexd_size_budget")
        if (
            int(storage["runtime_sqlite_total_bytes"])
            > self.runtime_sqlite_size_budget_bytes
        ):
            pressure_reasons.append("runtime_sqlite_size_budget")
        if int(write_latency["count"]) >= 20 and float(write_latency["p95_ms"]) > 100:
            pressure_reasons.append("sqlite_write_p95")
        if (
            int(event_metrics.get("count", 0)) >= 20
            and float(event_metrics.get("p95_ms", 0)) > 500
        ):
            pressure_reasons.append("event_persist_p95")
        storage["pressure_reasons"] = pressure_reasons
        if self.database != "failed":
            self.database = "degraded" if pressure_reasons else "healthy"
        payload: dict[str, Any] = {
            "schema_version": 1,
            "boot_id": self.boot_id,
            "pid": os.getpid(),
            "process_start_token": self.process_start_token,
            "started_at": self.started_at,
            "uptime_ms": max(0, now - self.started_at),
            "heartbeat_at": now,
            "service": self.service,
            "discord": self.discord,
            "database": self.database,
            "codex_auth": {
                "state": self.codex_auth,
                "observed_at": self.codex_auth_observed_at,
            },
            "runtime_slots": runtime,
            "turns": {
                "queued": counts["turns_queued"],
                "active": counts["turns_active"],
                "terminal": counts["turns_terminal"],
                "interrupted": counts["turns_interrupted"],
                "duration_ms_avg": counts["turn_duration_ms_avg"],
            },
            "schedules": {
                "active": counts["schedules_active"],
                "paused": counts["schedules_paused"],
                "blocked": counts["schedules_blocked"],
                "due_lag_ms": counts["schedule_due_lag_ms"],
            },
            "provider_barriers": counts["provider_barriers"],
            "outbox": {
                "pending": counts["outbox_pending"],
                "retry": counts["outbox_retry"],
                "dead_letter": counts["outbox_dead_letter"],
                "oldest_age_ms": counts["outbox_oldest_age_ms"],
                "lease_losses": counts["outbox_lease_losses"],
            },
            "unknown_provider_events": counts["unknown_provider_events"],
            "event_pump": event_metrics,
            "discord_egress": discord_egress,
            "discord_connection": discord_connection,
            "discord_reconnect_count": self.discord_reconnect_count,
            "inbound_reconciliation": inbound,
            "database_size_bytes": (
                self.repository.store.path.stat().st_size
                if self.repository.store.path.exists()
                else 0
            ),
            "storage": storage,
            "sdk_version": self.sdk_version,
            "runtime_version": self.runtime_version,
        }
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        temporary = self.path.with_name(f".{self.path.name}.{self.boot_id}.tmp")
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
            encoding="utf-8",
        )
        if os.name != "nt":
            temporary.chmod(0o600)
        os.replace(temporary, self.path)

    async def _run(self) -> None:
        try:
            while not self._stop.is_set():
                await self.write()
                with suppress(TimeoutError):
                    await asyncio.wait_for(self._stop.wait(), timeout=10)
            await self.write()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.database = "failed"
            self._critical_failure(exc)


def read_health(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def heartbeat_state(path: Path, *, now_ms: int | None = None) -> str:
    health = read_health(path)
    if not health or not isinstance(health.get("heartbeat_at"), int):
        return "missing"
    age = (utc_now_ms() if now_ms is None else now_ms) - health["heartbeat_at"]
    if age <= 20_000:
        return "fresh"
    if age <= 60_000:
        return "degraded"
    return "stale"


def _storage_metrics(database: Path, runtime_sqlite_root: Path | None) -> dict[str, Any]:
    database_bytes = _regular_file_size(database)
    database_wal_bytes = _regular_file_size(
        database.with_name(f"{database.name}-wal")
    )
    runtime_main_bytes = 0
    runtime_wal_bytes = 0
    runtime_feedback_bytes = 0
    runtime_homes = 0
    if (
        runtime_sqlite_root is not None
        and runtime_sqlite_root.exists()
        and runtime_sqlite_root.is_dir()
        and not runtime_sqlite_root.is_symlink()
    ):
        try:
            homes = tuple(runtime_sqlite_root.iterdir())
        except OSError:
            homes = ()
        for home in homes:
            if home.is_symlink() or not home.is_dir():
                continue
            runtime_homes += 1
            try:
                candidates = tuple(home.iterdir())
            except OSError:
                continue
            for candidate in candidates:
                if candidate.is_symlink() or not candidate.is_file():
                    continue
                try:
                    size = candidate.stat().st_size
                except OSError:
                    continue
                if candidate.name.endswith("-wal"):
                    runtime_wal_bytes += size
                elif candidate.suffix == ".sqlite":
                    runtime_main_bytes += size
                    if candidate.name.startswith("logs_"):
                        runtime_feedback_bytes += size
    codexd_total = database_bytes + database_wal_bytes
    runtime_total = runtime_main_bytes + runtime_wal_bytes
    return {
        "codexd_database_bytes": database_bytes,
        "codexd_wal_bytes": database_wal_bytes,
        "codexd_total_bytes": codexd_total,
        "runtime_sqlite_homes": runtime_homes,
        "runtime_sqlite_database_bytes": runtime_main_bytes,
        "runtime_sqlite_wal_bytes": runtime_wal_bytes,
        "runtime_sqlite_feedback_bytes": runtime_feedback_bytes,
        "runtime_sqlite_total_bytes": runtime_total,
        "total_bytes": codexd_total + runtime_total,
    }


def _regular_file_size(path: Path) -> int:
    try:
        if path.is_symlink() or not path.is_file():
            return 0
        return path.stat().st_size
    except OSError:
        return 0
