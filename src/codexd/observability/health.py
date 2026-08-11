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

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="codexd-health")

    def observe_discord(self, status: str) -> None:
        if status in {"ready", "degraded"}:
            if self._discord_ready_observed and self.discord == "disconnected":
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
            },
            "unknown_provider_events": counts["unknown_provider_events"],
            "discord_reconnect_count": self.discord_reconnect_count,
            "inbound_reconciliation": inbound,
            "database_size_bytes": (
                self.repository.store.path.stat().st_size
                if self.repository.store.path.exists()
                else 0
            ),
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
