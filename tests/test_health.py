from __future__ import annotations

import json

import pytest
from conftest import StorageContext

from codexd.domain.ids import utc_now_ms
from codexd.observability.health import HealthReporter


@pytest.mark.asyncio
async def test_health_reports_runtime_sqlite_capacity_and_write_latency(
    storage_context: StorageContext,
) -> None:
    runtime_root = storage_context.root.parent / "runtime-sqlite"
    runtime_home = runtime_root / "project-test"
    runtime_home.mkdir(parents=True)
    (runtime_home / "logs_2.sqlite").write_bytes(b"l" * 10)
    (runtime_home / "logs_2.sqlite-wal").write_bytes(b"w" * 5)
    (runtime_home / "state_5.sqlite").write_bytes(b"s" * 7)
    health_path = storage_context.root.parent / "health.json"
    boot_id = "health-boot"
    storage_context.repository.acquire_daemon_lease(
        boot_id=boot_id,
        pid=123,
        process_start_token="process",
        stale_before=0,
    )

    async def runtime_status() -> dict[str, int | str]:
        return {
            "topology": "project_scoped",
            "ready": 1,
            "capacity_limit": 10,
            "capacity_in_use": 1,
        }

    reporter = HealthReporter(
        path=health_path,
        repository=storage_context.repository,
        runtime_status=runtime_status,
        boot_id=boot_id,
        process_start_token="process",
        started_at=utc_now_ms(),
        runtime_sqlite_root=runtime_root,
        database_size_budget_bytes=1,
        runtime_sqlite_size_budget_bytes=1,
        event_metrics=lambda: {
            "count": 2,
            "p50_ms": 1.0,
            "p95_ms": 2.0,
            "max_ms": 2.0,
        },
        discord_egress_metrics=lambda: {
            "governor_wait_count": 3,
            "discord_429_count": 0,
            "route_turn_progress_edit_count": 4,
        },
        discord_reconnect_status=lambda: {
            "connection_state": "reconnecting",
            "tier": 6,
            "selected_delay_seconds": 60.0,
        },
    )

    await reporter.write()

    payload = json.loads(health_path.read_text(encoding="utf-8"))
    assert payload["database"] == "degraded"
    assert payload["runtime_slots"]["capacity_limit"] == 10
    assert payload["event_pump"]["p95_ms"] == 2.0
    assert payload["discord_egress"] == {
        "governor_wait_count": 3,
        "discord_429_count": 0,
        "route_turn_progress_edit_count": 4,
    }
    assert payload["discord_connection"] == {
        "connection_state": "reconnecting",
        "tier": 6,
        "selected_delay_seconds": 60.0,
    }
    storage = payload["storage"]
    assert storage["runtime_sqlite_homes"] == 1
    assert storage["runtime_sqlite_database_bytes"] == 17
    assert storage["runtime_sqlite_wal_bytes"] == 5
    assert storage["runtime_sqlite_feedback_bytes"] == 10
    assert storage["runtime_sqlite_total_bytes"] == 22
    assert storage["write_latency"]["count"] > 0
    assert storage["write_latency"]["p95_ms"] >= 0
    assert storage["growth_bytes_per_minute"] == 0
    assert storage["event_rows_per_minute"] == 0


def test_health_counts_success_after_reconnecting(
    storage_context: StorageContext,
) -> None:
    async def runtime_status() -> dict[str, int | str]:
        return {}

    reporter = HealthReporter(
        path=storage_context.root / "health-reconnect.json",
        repository=storage_context.repository,
        runtime_status=runtime_status,
        boot_id="reconnect-health",
        process_start_token="process",
        started_at=utc_now_ms(),
    )
    reporter.observe_discord("ready")
    reporter.observe_discord("reconnecting")
    reporter.observe_discord("ready")

    assert reporter.discord_reconnect_count == 1
