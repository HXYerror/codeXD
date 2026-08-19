from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import dataclass

from codexd.domain.ids import utc_now_ms

DISCORD_RECONNECT_DELAYS_SECONDS = (
    1.0,
    10.0,
    30.0,
    30.0,
    60.0,
    60.0,
    60.0,
    120.0,
    240.0,
    300.0,
)


@dataclass(frozen=True)
class DiscordReconnectSnapshot:
    connection_state: str
    tier: int
    consecutive_failures: int
    selected_delay_seconds: float
    next_retry_at: int | None
    last_failure_at: int | None
    last_ready_at: int | None
    last_session_mode: str | None
    last_error_code: str | None
    ready_count: int
    resumed_count: int
    reset_count: int
    server_retry_after_seconds: float | None


class DiscordReconnectBackoff:
    def __init__(
        self,
        *,
        monotonic: Callable[[], float] = time.monotonic,
        wall_clock_ms: Callable[[], int] = utc_now_ms,
    ) -> None:
        self._monotonic = monotonic
        self._wall_clock_ms = wall_clock_ms
        self._stop = asyncio.Event()
        self.connection_state = "connecting"
        self.tier = 0
        self.consecutive_failures = 0
        self.selected_delay_seconds = 0.0
        self.next_retry_at: int | None = None
        self.last_failure_at: int | None = None
        self.last_ready_at: int | None = None
        self.last_session_mode: str | None = None
        self.last_error_code: str | None = None
        self.ready_count = 0
        self.resumed_count = 0
        self.reset_count = 0
        self.server_retry_after_seconds: float | None = None

    def next_delay(
        self,
        *,
        error_code: str,
        retry_after: float | None = None,
    ) -> float:
        self.consecutive_failures += 1
        self.tier = min(
            self.consecutive_failures,
            len(DISCORD_RECONNECT_DELAYS_SECONDS),
        )
        local_delay = DISCORD_RECONNECT_DELAYS_SECONDS[self.tier - 1]
        self.server_retry_after_seconds = (
            max(0.0, retry_after) if retry_after is not None else None
        )
        delay = max(local_delay, self.server_retry_after_seconds or 0.0)
        self.selected_delay_seconds = delay
        self.next_retry_at = self._wall_clock_ms() + int(delay * 1000)
        self.last_failure_at = self._wall_clock_ms()
        self.last_error_code = error_code
        self.connection_state = "reconnecting"
        # Retry duration is fixed and never derived from wall-clock deltas.
        self._monotonic()
        return delay

    def reset(self, mode: str) -> None:
        if mode not in {"ready", "resumed"}:
            raise ValueError("Discord session mode must be ready or resumed")
        duplicate = (
            self.connection_state in {"ready", "resumed"}
            and self.last_session_mode == mode
            and self.tier == 0
        )
        self.connection_state = mode
        self.tier = 0
        self.consecutive_failures = 0
        self.selected_delay_seconds = 0.0
        self.next_retry_at = None
        self.last_ready_at = self._wall_clock_ms()
        self.last_session_mode = mode
        self.last_error_code = None
        self.server_retry_after_seconds = None
        if not duplicate:
            self.reset_count += 1
            if mode == "ready":
                self.ready_count += 1
            else:
                self.resumed_count += 1

    def mark_connecting(self) -> None:
        if self.connection_state not in {"ready", "resumed"}:
            self.connection_state = "connecting" if self.tier == 0 else "reconnecting"

    async def wait(self, delay: float) -> bool:
        if delay <= 0:
            return not self._stop.is_set()
        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay)
        except TimeoutError:
            return True
        return False

    def stop(self) -> None:
        self._stop.set()

    def snapshot(self) -> DiscordReconnectSnapshot:
        return DiscordReconnectSnapshot(
            connection_state=self.connection_state,
            tier=self.tier,
            consecutive_failures=self.consecutive_failures,
            selected_delay_seconds=self.selected_delay_seconds,
            next_retry_at=self.next_retry_at,
            last_failure_at=self.last_failure_at,
            last_ready_at=self.last_ready_at,
            last_session_mode=self.last_session_mode,
            last_error_code=self.last_error_code,
            ready_count=self.ready_count,
            resumed_count=self.resumed_count,
            reset_count=self.reset_count,
            server_retry_after_seconds=self.server_retry_after_seconds,
        )
