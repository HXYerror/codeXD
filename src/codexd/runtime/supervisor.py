from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path

from codexd.config import resolve_project_path
from codexd.domain.capabilities import CapabilityManifest
from codexd.domain.ids import utc_now_ms
from codexd.domain.models import AccountStatus, ModelCatalogSnapshot
from codexd.errors import InvariantError, SecurityError
from codexd.runtime.errors import (
    AdapterError,
    AdapterFailure,
    AdapterInvariantError,
    RuntimeUnavailable,
)
from codexd.runtime.port import CodexRuntime, RuntimeSlotConfig
from codexd.security import private_files
from codexd.storage.records import ProjectRecord, RuntimeLeaseRecord
from codexd.storage.repository import Repository

RuntimeFactory = Callable[[RuntimeSlotConfig, int], Awaitable[CodexRuntime]]
_BACKOFF_SECONDS = (1, 2, 5, 10, 30, 60)
_RUNTIME_STARTUP_TIMEOUT_SECONDS = 30.0
_RUNTIME_CLOSE_TIMEOUT_SECONDS = 30.0
_RUNTIME_WATCHDOG_INTERVAL_SECONDS = 30.0
_RUNTIME_WATCHDOG_TIMEOUT_SECONDS = 5.0
_CRASH_WINDOW_MS = 10 * 60 * 1000
_CRASH_LOOP_THRESHOLD = 6


@dataclass
class RuntimeSlot:
    key: str
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    runtime: CodexRuntime | None = None
    lease: RuntimeLeaseRecord | None = None
    failures: int = 0
    retry_at: float = 0
    ready_since: float | None = None
    last_watchdog_at: float = 0
    startup_task: asyncio.Task[object] | None = None
    last_used_at: float = 0
    capacity_reserved: bool = False


class RuntimeSupervisor:
    def __init__(
        self,
        *,
        repository: Repository,
        factory: RuntimeFactory,
        topology: str,
        environment: dict[str, str],
        environment_hash: str,
        codex_home: Path | None,
        neutral_cwd: Path,
        allowed_roots: tuple[Path, ...],
        codex_bin: Path | None = None,
        sqlite_root: Path | None = None,
        max_active_runtimes: int = 4,
        idle_ttl_seconds: float = 15 * 60,
        startup_timeout_seconds: float = _RUNTIME_STARTUP_TIMEOUT_SECONDS,
        watchdog_interval_seconds: float = _RUNTIME_WATCHDOG_INTERVAL_SECONDS,
        watchdog_timeout_seconds: float = _RUNTIME_WATCHDOG_TIMEOUT_SECONDS,
    ) -> None:
        if topology not in {"project_scoped", "shared"}:
            raise ValueError("runtime topology must be project_scoped or shared")
        if (
            startup_timeout_seconds <= 0
            or watchdog_interval_seconds <= 0
            or watchdog_timeout_seconds <= 0
            or max_active_runtimes <= 0
            or idle_ttl_seconds <= 0
        ):
            raise ValueError("runtime timeouts and watchdog intervals must be positive")
        self._repository = repository
        self._factory = factory
        self._topology = topology
        self._environment = dict(environment)
        self._environment_hash = environment_hash
        self._codex_home = codex_home
        self._neutral_cwd = neutral_cwd
        self._allowed_roots = allowed_roots
        self._codex_bin = codex_bin
        self._sqlite_root = sqlite_root
        self._max_active_runtimes = max_active_runtimes
        self._idle_ttl_seconds = idle_ttl_seconds
        self._capacity_gate = asyncio.BoundedSemaphore(max_active_runtimes)
        self._capacity_start_lock = asyncio.Lock()
        self._capacity_in_use = 0
        self._capacity_waiters = 0
        self._idle_evictions = 0
        self._startup_timeout_seconds = startup_timeout_seconds
        self._watchdog_interval_seconds = watchdog_interval_seconds
        self._watchdog_timeout_seconds = watchdog_timeout_seconds
        self._slots: dict[str, RuntimeSlot] = {}
        self._slots_lock = asyncio.Lock()
        self._closing = False
        self._watchdog_task: asyncio.Task[None] | None = None
        self._watchdog_failures = 0

    @property
    def topology(self) -> str:
        return self._topology

    async def ensure(self, project: ProjectRecord) -> tuple[CodexRuntime, RuntimeLeaseRecord]:
        if self._closing:
            raise InvariantError("runtime supervisor is closing")
        resolved_root = resolve_project_path(
            str(project.root_path),
            self._allowed_roots,
        )
        if resolved_root != project.root_path:
            raise SecurityError(
                "stored project root changed identity after canonical resolution"
            )
        key = project.id if self._topology == "project_scoped" else "shared"
        attributed_project_id = (
            project.id if self._topology == "project_scoped" else None
        )
        slot = await self._slot(key)
        async with slot.lock:
            if self._closing:
                raise InvariantError("runtime supervisor is closing")
            if slot.runtime is not None and slot.lease is not None:
                if slot.lease.state == "ready":
                    slot.last_used_at = time.monotonic()
                    if slot.ready_since and time.monotonic() - slot.ready_since >= 300:
                        slot.failures = 0
                    return slot.runtime, slot.lease
                await self._close_ready_slot(
                    slot,
                    deadline=time.monotonic() + _RUNTIME_CLOSE_TIMEOUT_SECONDS,
                )
            elif slot.runtime is not None or slot.lease is not None:
                raise InvariantError("runtime slot has incomplete in-memory state")
            if time.monotonic() < slot.retry_at:
                failure = AdapterFailure(
                    code="runtime_restart_backoff",
                    provider_exception="RuntimeBackoff",
                    message="Codex runtime is in restart backoff",
                    retryable=True,
                    runtime_generation=slot.lease.generation if slot.lease else 0,
                )
                raise RuntimeUnavailable(failure)

            slot_config = self._slot_config(project)
            startup_task = asyncio.current_task()
            if startup_task is None:
                raise InvariantError("runtime startup has no owning task")
            slot.startup_task = startup_task
            lease: RuntimeLeaseRecord | None = None
            runtime: CodexRuntime | None = None
            manifest: CapabilityManifest | None = None
            try:
                await self._reserve_capacity(slot)
                lease = await asyncio.to_thread(
                    self._repository.create_runtime_lease,
                    scope_kind=(
                        "project" if self._topology == "project_scoped" else "shared"
                    ),
                    scope_key=key,
                    project_id=(
                        project.id if self._topology == "project_scoped" else None
                    ),
                    environment_hash=slot_config.environment_hash,
                )
                slot.lease = lease

                async def initialize_runtime() -> None:
                    nonlocal manifest, runtime
                    runtime = await self._factory(slot_config, lease.generation)
                    if runtime.generation != lease.generation:
                        raise AdapterInvariantError(
                            AdapterFailure(
                                code="runtime_generation_mismatch",
                                provider_exception="RuntimeFactory",
                                message="Runtime factory returned the wrong generation",
                                retryable=False,
                                runtime_generation=lease.generation,
                            )
                        )
                    manifest = await runtime.capabilities()
                    manifest.assert_required()
                    account = await runtime.account_status()
                    if account.auth_required:
                        failure = AdapterFailure(
                            code="codex_auth_required",
                            provider_exception="AccountStatus",
                            message="Codex authentication is required",
                            retryable=False,
                            runtime_generation=lease.generation,
                        )
                        raise AdapterError(failure)
                    catalog = await runtime.list_models()
                    _assert_image_capability(catalog)
                    if self._closing:
                        raise InvariantError("runtime supervisor closed during startup")

                try:
                    await asyncio.wait_for(
                        initialize_runtime(),
                        timeout=self._startup_timeout_seconds,
                    )
                except TimeoutError as exc:
                    raise RuntimeUnavailable(
                        AdapterFailure(
                            code="runtime_start_timeout",
                            provider_exception=type(exc).__name__,
                            message="Codex runtime startup exceeded its deadline",
                            retryable=True,
                            runtime_generation=lease.generation,
                        )
                    ) from exc
                if runtime is None or manifest is None:
                    raise InvariantError("runtime startup did not produce a ready runtime")
                await asyncio.to_thread(
                    self._repository.mark_runtime_ready,
                    lease.id,
                    sdk_version=manifest.sdk_version,
                    runtime_version=manifest.runtime_version,
                    capability_hash=manifest.digest,
                )
            except BaseException as exc:
                close_error: BaseException | None = None
                if runtime is not None:
                    try:
                        await asyncio.wait_for(
                            runtime.close(),
                            timeout=_RUNTIME_CLOSE_TIMEOUT_SECONDS,
                        )
                    except BaseException as runtime_close_error:
                        close_error = runtime_close_error
                mark_error: BaseException | None = None
                if lease is not None:
                    try:
                        await asyncio.to_thread(
                            self._repository.mark_runtime_failed,
                            lease.id,
                            failure_code=(
                                exc.failure.code
                                if isinstance(exc, AdapterError)
                                else getattr(exc, "code", "runtime_start_failed")
                            ),
                        )
                    except BaseException as runtime_mark_error:
                        mark_error = runtime_mark_error
                    if close_error is not None:
                        try:
                            await asyncio.to_thread(
                                self._repository.mark_runtime_close_failed,
                                lease.id,
                                failure_code="runtime_start_cleanup_failed",
                            )
                        except BaseException as incident_error:
                            if mark_error is None:
                                mark_error = incident_error
                            else:
                                mark_error = BaseExceptionGroup(
                                    "runtime startup persistence failed",
                                    (mark_error, incident_error),
                                )
                failure_bookkeeping_error: BaseException | None = None
                try:
                    await self._register_failure(
                        slot,
                        scope_key=key,
                        project_id=attributed_project_id,
                        failure_code=(
                            exc.failure.code
                            if isinstance(exc, AdapterError)
                            else getattr(exc, "code", "runtime_start_failed")
                        ),
                    )
                except BaseException as register_error:
                    failure_bookkeeping_error = register_error
                if close_error is not None and runtime is not None and lease is not None:
                    slot.runtime = runtime
                    slot.lease = RuntimeLeaseRecord(
                        id=lease.id,
                        scope_key=lease.scope_key,
                        generation=lease.generation,
                        state="failed",
                    )
                    slot.ready_since = None
                    slot.last_watchdog_at = 0
                else:
                    slot.runtime = None
                    slot.lease = None
                    self._release_capacity(slot)
                secondary_errors = tuple(
                    error
                    for error in (
                        close_error,
                        mark_error,
                        failure_bookkeeping_error,
                    )
                    if error is not None
                )
                if secondary_errors:
                    raise BaseExceptionGroup(
                        "runtime startup bookkeeping failed",
                        (exc, *secondary_errors),
                    ) from exc
                raise
            finally:
                if slot.startup_task is startup_task:
                    slot.startup_task = None
            if runtime is None:
                raise InvariantError("runtime startup completed without a runtime")
            slot.runtime = runtime
            slot.lease = RuntimeLeaseRecord(
                id=lease.id,
                scope_key=lease.scope_key,
                generation=lease.generation,
                state="ready",
            )
            slot.ready_since = time.monotonic()
            slot.last_watchdog_at = slot.ready_since
            slot.last_used_at = slot.ready_since
            if self._closing:
                await self._close_ready_slot(
                    slot,
                    deadline=time.monotonic() + _RUNTIME_CLOSE_TIMEOUT_SECONDS,
                )
                raise InvariantError("runtime supervisor closed during startup")
            return runtime, slot.lease

    async def report_failure(
        self,
        project: ProjectRecord,
        *,
        expected_lease_id: str,
        expected_generation: int,
        failure_code: str,
    ) -> tuple[str, ...]:
        key = project.id if self._topology == "project_scoped" else "shared"
        return await self._report_failure_for_key(
            key,
            project_id=(
                project.id if self._topology == "project_scoped" else None
            ),
            expected_lease_id=expected_lease_id,
            expected_generation=expected_generation,
            failure_code=failure_code,
        )

    async def _report_failure_for_key(
        self,
        key: str,
        *,
        project_id: str | None,
        expected_lease_id: str,
        expected_generation: int,
        failure_code: str,
    ) -> tuple[str, ...]:
        slot = await self._slot(key)
        async with slot.lock:
            runtime = slot.runtime
            lease = slot.lease
            if runtime is None or lease is None:
                return ()
            if (
                lease.id != expected_lease_id
                or lease.generation != expected_generation
            ):
                return ()
            interrupted = await asyncio.to_thread(
                self._repository.mark_runtime_unhealthy,
                lease.id,
                failure_code=failure_code,
            )
            close_error: BaseException | None = None
            try:
                await asyncio.wait_for(
                    runtime.close(),
                    timeout=_RUNTIME_CLOSE_TIMEOUT_SECONDS,
                )
            except BaseException as exc:
                close_error = exc
                await asyncio.to_thread(
                    self._repository.mark_runtime_close_failed,
                    lease.id,
                    failure_code="runtime_failure_cleanup_failed",
                )
            finally:
                slot.ready_since = None
                slot.last_watchdog_at = 0
                if close_error is None:
                    slot.runtime = None
                    slot.lease = None
                    self._release_capacity(slot)
                else:
                    slot.runtime = runtime
                    slot.lease = RuntimeLeaseRecord(
                        id=lease.id,
                        scope_key=lease.scope_key,
                        generation=lease.generation,
                        state="failed",
                    )
            await self._register_failure(
                slot,
                scope_key=key,
                project_id=project_id,
                failure_code=failure_code,
            )
            if close_error is not None:
                close_error.add_note(
                    "runtime failure cleanup was recorded as runtime_close_failed"
                )
            return interrupted

    async def close(
        self,
        *,
        timeout_seconds: float = _RUNTIME_CLOSE_TIMEOUT_SECONDS,
    ) -> None:
        errors: list[Exception] = []
        async with self._slots_lock:
            self._closing = True
            slots = tuple(self._slots.values())
        watchdog_task = self._watchdog_task
        if watchdog_task is not None and watchdog_task is not asyncio.current_task():
            if not watchdog_task.done():
                watchdog_task.cancel()
            try:
                await watchdog_task
            except asyncio.CancelledError:
                pass
            except Exception as exc:
                errors.append(exc)
        current_task = asyncio.current_task()
        for slot in slots:
            startup_task = slot.startup_task
            if (
                startup_task is not None
                and startup_task is not current_task
                and not startup_task.done()
            ):
                startup_task.cancel()
        deadline = time.monotonic() + timeout_seconds
        results = await asyncio.gather(
            *(self._close_slot_for_shutdown(slot, deadline=deadline) for slot in slots),
            return_exceptions=True,
        )
        for result in results:
            if isinstance(result, asyncio.CancelledError):
                raise result
            if isinstance(result, Exception):
                errors.append(result)
            elif isinstance(result, BaseException):
                raise result
        if errors:
            raise ExceptionGroup("one or more Codex runtimes did not close", errors)

    async def status(self) -> dict[str, int | str]:
        self._schedule_watchdog()
        async with self._slots_lock:
            slots = tuple(self._slots.values())
        return {
            "topology": self._topology,
            "ready": sum(
                slot.runtime is not None
                and slot.lease is not None
                and slot.lease.state == "ready"
                for slot in slots
            ),
            "starting": sum(
                slot.startup_task is not None and not slot.startup_task.done()
                for slot in slots
            ),
            "unhealthy": sum(
                (
                    slot.runtime is None
                    and slot.failures > 0
                )
                or (
                    slot.lease is not None
                    and slot.lease.state != "ready"
                )
                for slot in slots
            ),
            "watchdog": (
                "running"
                if self._watchdog_task is not None
                and not self._watchdog_task.done()
                else "idle"
            ),
            "watchdog_failures": self._watchdog_failures,
            "capacity_limit": self._max_active_runtimes,
            "capacity_in_use": self._capacity_in_use,
            "capacity_waiters": self._capacity_waiters,
            "idle_ttl_seconds": int(self._idle_ttl_seconds),
            "idle_evictions": self._idle_evictions,
            "sqlite_isolated": self._sqlite_root is not None,
        }

    def _schedule_watchdog(self) -> None:
        if self._closing:
            return
        if self._watchdog_task is not None and not self._watchdog_task.done():
            return
        if self._watchdog_task is not None:
            try:
                error = self._watchdog_task.exception()
            except asyncio.CancelledError:
                error = None
            if error is not None:
                self._watchdog_failures += 1
        self._watchdog_task = asyncio.create_task(
            self._run_scheduled_watchdog(),
            name="codexd-runtime-watchdog",
        )

    async def _run_scheduled_watchdog(self) -> None:
        try:
            await self.watchdog()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._watchdog_failures += 1
            await asyncio.to_thread(
                self._repository.record_incident,
                severity="error",
                code="runtime_watchdog_internal_error",
                summary="Runtime watchdog failed before completing its probes",
                details={"exception_type": type(exc).__name__},
            )

    async def watchdog(self) -> None:
        async with self._slots_lock:
            slots = tuple(self._slots.values())
        if slots:
            await asyncio.gather(*(self._watchdog_slot(slot) for slot in slots))

    async def _close_slot_for_shutdown(
        self,
        slot: RuntimeSlot,
        *,
        deadline: float,
    ) -> None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            await self._record_close_failure(
                slot,
                failure_code="runtime_close_timeout",
            )
            raise TimeoutError("Codex runtime close deadline expired")
        try:
            await asyncio.wait_for(slot.lock.acquire(), timeout=remaining)
        except Exception:
            await self._record_close_failure(
                slot,
                failure_code="runtime_close_lock_timeout",
            )
            raise
        try:
            await self._close_ready_slot(slot, deadline=deadline)
            if slot.runtime is None:
                self._release_capacity(slot)
        finally:
            slot.lock.release()

    async def project_status(self, project_id: str) -> dict[str, int | str]:
        key = project_id if self._topology == "project_scoped" else "shared"
        async with self._slots_lock:
            slot = self._slots.get(key)
        if slot is None:
            return {
                "state": "not_loaded",
                "generation": 0,
                "failures": 0,
            }
        if slot.runtime is not None and slot.lease is not None:
            state = "ready" if slot.lease.state == "ready" else "unhealthy"
        elif slot.startup_task is not None and not slot.startup_task.done():
            state = "starting"
        elif slot.failures:
            state = "unhealthy"
        else:
            state = "not_loaded"
        return {
            "state": state,
            "generation": slot.lease.generation if slot.lease is not None else 0,
            "failures": slot.failures,
        }

    async def account_status_if_loaded(
        self,
        project_id: str,
    ) -> AccountStatus | None:
        key = project_id if self._topology == "project_scoped" else "shared"
        async with self._slots_lock:
            slot = self._slots.get(key)
        if slot is None:
            return None
        async with slot.lock:
            if slot.runtime is None:
                return None
            return await slot.runtime.account_status()

    async def model_catalog_if_loaded(
        self,
        project_id: str,
    ) -> ModelCatalogSnapshot | None:
        """Read the catalog without creating or starting a Runtime Slot."""

        key = project_id if self._topology == "project_scoped" else "shared"
        async with self._slots_lock:
            slot = self._slots.get(key)
        if slot is None:
            return None
        async with slot.lock:
            if (
                slot.runtime is None
                or slot.lease is None
                or slot.lease.state != "ready"
            ):
                return None
            return await slot.runtime.list_models()

    async def _slot(self, key: str) -> RuntimeSlot:
        async with self._slots_lock:
            return self._slots.setdefault(key, RuntimeSlot(key=key))

    async def _close_ready_slot(
        self,
        slot: RuntimeSlot,
        *,
        deadline: float,
    ) -> None:
        runtime = slot.runtime
        if runtime is None:
            return
        lease = slot.lease
        if lease is None:
            raise InvariantError("ready runtime has no durable lease")
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            await self._record_close_failure(
                slot,
                failure_code="runtime_close_timeout",
            )
            raise TimeoutError("Codex runtime close deadline expired")
        await asyncio.wait_for(
            asyncio.to_thread(
                self._repository.mark_runtime_stopping,
                lease.id,
            ),
            timeout=remaining,
        )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            await self._record_close_failure(
                slot,
                failure_code="runtime_close_timeout",
            )
            raise TimeoutError("Codex runtime close deadline expired")
        try:
            await asyncio.wait_for(runtime.close(), timeout=remaining)
        except BaseException as exc:
            try:
                await asyncio.to_thread(
                    self._repository.mark_runtime_close_failed,
                    lease.id,
                    failure_code=(
                        "runtime_close_timeout"
                        if isinstance(exc, TimeoutError)
                        else "runtime_close_error"
                    ),
                )
            except BaseException as persistence_error:
                raise BaseExceptionGroup(
                    "runtime close and failure persistence both failed",
                    (exc, persistence_error),
                ) from exc
            raise
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            await self._record_close_failure(
                slot,
                failure_code="runtime_close_timeout",
            )
            raise TimeoutError("Codex runtime close deadline expired")
        await asyncio.wait_for(
            asyncio.to_thread(
                self._repository.mark_runtime_stopped,
                lease.id,
            ),
            timeout=remaining,
        )
        slot.runtime = None
        slot.lease = None
        slot.ready_since = None
        slot.last_watchdog_at = 0
        slot.last_used_at = 0
        self._release_capacity(slot)

    async def _watchdog_slot(self, slot: RuntimeSlot) -> None:
        now = time.monotonic()
        async with slot.lock:
            runtime = slot.runtime
            lease = slot.lease
            if (
                runtime is not None
                and lease is not None
                and lease.state == "ready"
                and slot.last_used_at > 0
                and now - slot.last_used_at >= self._idle_ttl_seconds
                and not await asyncio.to_thread(
                    self._repository.runtime_scope_has_live_work,
                    lease.id,
                    project_id=(
                        slot.key if self._topology == "project_scoped" else None
                    ),
                )
            ):
                await self._close_ready_slot(
                    slot,
                    deadline=time.monotonic() + _RUNTIME_CLOSE_TIMEOUT_SECONDS,
                )
                self._idle_evictions += 1
                return
            if (
                runtime is None
                or lease is None
                or now - slot.last_watchdog_at < self._watchdog_interval_seconds
            ):
                return
            slot.last_watchdog_at = now
            key = slot.key
        try:
            await asyncio.wait_for(
                runtime.account_status(),
                timeout=self._watchdog_timeout_seconds,
            )
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            failure_code = "runtime_watchdog_timeout"
        except AdapterError as exc:
            failure_code = f"runtime_watchdog_{exc.failure.code}"
        except Exception:
            failure_code = "runtime_watchdog_failed"
        else:
            async with slot.lock:
                if (
                    slot.runtime is not runtime
                    or slot.lease is None
                    or slot.lease.id != lease.id
                    or slot.lease.generation != lease.generation
                    or slot.lease.state != "ready"
                ):
                    return
                heartbeat = await asyncio.to_thread(
                    self._repository.heartbeat_runtime,
                    lease.id,
                )
                if heartbeat:
                    if slot.ready_since and now - slot.ready_since >= 300:
                        slot.failures = 0
                    return
            failure_code = "runtime_watchdog_lease_not_ready"
        await self._report_failure_for_key(
            key,
            project_id=(key if self._topology == "project_scoped" else None),
            expected_lease_id=lease.id,
            expected_generation=lease.generation,
            failure_code=failure_code,
        )

    async def _register_failure(
        self,
        slot: RuntimeSlot,
        *,
        scope_key: str,
        project_id: str | None,
        failure_code: str,
    ) -> None:
        slot.failures += 1
        failure_count = await asyncio.to_thread(
            self._repository.recent_runtime_failure_count,
            scope_key,
            since_ms=utc_now_ms() - _CRASH_WINDOW_MS,
        )
        delay = _BACKOFF_SECONDS[
            min(slot.failures - 1, len(_BACKOFF_SECONDS) - 1)
        ]
        if failure_count >= _CRASH_LOOP_THRESHOLD:
            await asyncio.to_thread(
                self._repository.record_incident,
                severity="error",
                code="runtime_crash_loop",
                summary="Codex runtime repeatedly failed within ten minutes",
                project_id=project_id,
                details={
                    "scope_key": scope_key,
                    "failure_count": failure_count,
                    "failure_code": failure_code,
                    "backoff_seconds": delay,
                },
            )
        slot.retry_at = time.monotonic() + delay

    async def _record_close_failure(
        self,
        slot: RuntimeSlot,
        *,
        failure_code: str,
    ) -> None:
        lease = slot.lease
        if slot.runtime is None or lease is None or lease.state != "ready":
            return
        await asyncio.to_thread(
            self._repository.mark_runtime_close_failed,
            lease.id,
            failure_code=failure_code,
        )

    async def _reserve_capacity(self, slot: RuntimeSlot) -> None:
        if slot.capacity_reserved:
            return
        self._capacity_waiters += 1
        try:
            async with self._capacity_start_lock:
                if self._capacity_in_use >= self._max_active_runtimes:
                    await self._evict_lru_idle_slot(exclude=slot)
                await self._capacity_gate.acquire()
                if self._closing:
                    self._capacity_gate.release()
                    raise InvariantError("runtime supervisor is closing")
                slot.capacity_reserved = True
                self._capacity_in_use += 1
        finally:
            self._capacity_waiters -= 1

    async def _evict_lru_idle_slot(self, *, exclude: RuntimeSlot) -> bool:
        async with self._slots_lock:
            candidates = sorted(
                (
                    slot
                    for slot in self._slots.values()
                    if slot is not exclude and slot.last_used_at > 0
                ),
                key=lambda slot: slot.last_used_at,
            )
        now = time.monotonic()
        for candidate in candidates:
            if now - candidate.last_used_at < self._idle_ttl_seconds:
                continue
            async with candidate.lock:
                lease = candidate.lease
                if (
                    candidate.runtime is None
                    or lease is None
                    or lease.state != "ready"
                    or await asyncio.to_thread(
                        self._repository.runtime_scope_has_live_work,
                        lease.id,
                        project_id=(
                            candidate.key
                            if self._topology == "project_scoped"
                            else None
                        ),
                    )
                ):
                    continue
                await self._close_ready_slot(
                    candidate,
                    deadline=time.monotonic() + _RUNTIME_CLOSE_TIMEOUT_SECONDS,
                )
                self._idle_evictions += 1
                return True
        return False

    def _release_capacity(self, slot: RuntimeSlot) -> None:
        if not slot.capacity_reserved:
            return
        slot.capacity_reserved = False
        self._capacity_in_use -= 1
        self._capacity_gate.release()

    def _slot_config(self, project: ProjectRecord) -> RuntimeSlotConfig:
        if self._topology == "project_scoped":
            cwd = project.root_path
            project_id: str | None = project.id
            scope_kind = "project"
        else:
            self._neutral_cwd.mkdir(mode=0o700, parents=True, exist_ok=True)
            cwd = self._neutral_cwd
            project_id = None
            scope_kind = "shared"
        environment = dict(self._environment)
        sqlite_home: Path | None = None
        if self._sqlite_root is not None:
            private_files.ensure_private_directory(self._sqlite_root)
            digest = hashlib.sha256(
                f"{scope_kind}:{project.id if project_id is not None else 'shared'}".encode()
            ).hexdigest()[:32]
            sqlite_home = self._sqlite_root / f"{scope_kind}-{digest}"
            private_files.ensure_private_directory(sqlite_home)
            environment["CODEX_SQLITE_HOME"] = str(sqlite_home)
        environment_hash = _environment_hash(environment)
        return RuntimeSlotConfig(
            scope_kind=scope_kind,
            project_id=project_id,
            cwd=cwd,
            codex_home=self._codex_home,
            environment=environment,
            environment_hash=environment_hash,
            topology_contract=self._topology,
            codex_bin=self._codex_bin,
            sqlite_home=sqlite_home,
        )


def _assert_image_capability(catalog: ModelCatalogSnapshot) -> None:
    if any("image" in model.input_modalities for model in catalog.models):
        return
    code = "model_catalog_incomplete" if not catalog.complete else "required_capability_unavailable"
    failure = AdapterFailure(
        code=code,
        provider_exception="ModelCatalog",
        message="No image-capable Codex model is visible in the model catalog",
        retryable=False,
        runtime_generation=0,
    )
    raise AdapterError(failure)


def _environment_hash(environment: dict[str, str]) -> str:
    digest = hashlib.sha256()
    for name, value in sorted(environment.items()):
        digest.update(name.encode())
        digest.update(b"\0")
        digest.update(value.encode())
        digest.update(b"\0")
    return digest.hexdigest()
