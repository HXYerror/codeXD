from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass

from codexd.application.conversation_locks import ConversationLocks
from codexd.application.volatile_turns import VolatileTurnStore
from codexd.domain.conversations import (
    ApprovalPolicy,
    ThreadConfig,
    ThreadIdentity,
    ThreadProviderState,
    TurnConfig,
    WebSearchMode,
)
from codexd.domain.ids import sha256_text
from codexd.domain.models import ModelCatalogSnapshot, ModelDescriptor
from codexd.domain.turns import InterruptOrigin, TurnIdentity, TurnInput, TurnSource, TurnState
from codexd.errors import (
    CodexDError,
    ConflictError,
    InvariantError,
    NotFoundError,
    SecurityError,
)
from codexd.runtime.errors import (
    AdapterError,
    EventJournalError,
    ProviderOutcomeUnknown,
    RuntimeUnavailable,
    file_input_unsupported,
)
from codexd.runtime.event_pump import EventPump
from codexd.runtime.mailbox import MailboxRegistry
from codexd.runtime.port import CodexRuntime, StartedTurn
from codexd.runtime.supervisor import RuntimeSupervisor
from codexd.storage.projectors import ProjectingEventSink
from codexd.storage.records import (
    ProjectRecord,
    RuntimeLeaseRecord,
    ThreadRevisionRecord,
    TurnRecord,
)
from codexd.storage.repository import Repository


@dataclass(frozen=True)
class ActiveTurn:
    runtime: CodexRuntime
    identity: TurnIdentity


_PROVIDER_CLEANUP_TIMEOUT_SECONDS = 5.0


class TurnCoordinator:
    def __init__(
        self,
        *,
        repository: Repository,
        runtime_supervisor: RuntimeSupervisor,
        event_sink: ProjectingEventSink,
        conversation_locks: ConversationLocks | None = None,
        critical_failure: Callable[[BaseException], None] | None = None,
        provider_barrier_observer: Callable[[str], None] | None = None,
        skill_input_supported: bool = True,
        schedule_tool_supported: bool = False,
    ) -> None:
        self._repository = repository
        self._runtime_supervisor = runtime_supervisor
        self._event_pump = EventPump(repository=repository, sink=event_sink)
        self._volatile_turns = event_sink.volatile_turns
        self._conversation_locks = conversation_locks or ConversationLocks()
        self._critical_failure = critical_failure or (lambda _exc: None)
        self._provider_barrier_observer = provider_barrier_observer or (
            lambda _conversation_id: None
        )
        self._skill_input_supported = skill_input_supported
        self._schedule_tool_supported = schedule_tool_supported
        self._active: dict[str, ActiveTurn] = {}
        self._inflight: set[str] = set()
        self._active_lock = asyncio.Lock()
        self._closing = False
        self._mailboxes = MailboxRegistry(
            repository=repository,
            handler=self._process_turn,
            error_handler=self._mailbox_error,
        )

    async def restore(self) -> int:
        interrupted = 0
        turn_ids = await asyncio.to_thread(
            self._repository.queued_schedule_turn_ids
        )
        for turn_id in turn_ids:
            turn = await asyncio.to_thread(self._repository.get_turn, turn_id)
            reason_code: str | None = None
            if (
                turn.provider_turn_id is not None
                or turn.runtime_lease_id is not None
                or turn.runtime_generation is not None
                or turn.started_at is not None
            ):
                reason_code = "provider_start_state_present"
            else:
                try:
                    turn_input = await asyncio.to_thread(
                        self._repository.load_turn_input,
                        turn_id,
                    )
                except (ConflictError, OSError, ValueError, TypeError, KeyError):
                    reason_code = "snapshot_invalid"
                else:
                    if turn_input.skill_inputs and not self._skill_input_supported:
                        reason_code = "skill_capability_missing"
            if reason_code is not None:
                await asyncio.to_thread(
                    self._repository.interrupt_unreplayable_schedule_turn,
                    turn_id,
                    reason_code=reason_code,
                )
                interrupted += 1
        await self._mailboxes.restore()
        return interrupted

    async def wake(self, conversation_id: str) -> None:
        if self._closing:
            return
        await self._mailboxes.wake(conversation_id)

    async def enqueue(
        self,
        *,
        conversation_id: str,
        source: TurnSource,
        turn_input: TurnInput,
        input_message_id: str | None = None,
        schedule_fire_id: str | None = None,
        ingress_message_id: str | None = None,
        requested_by_user_id: int | None = None,
    ) -> TurnRecord:
        if self._closing:
            raise ConflictError("Turn coordinator is shutting down")
        async with self._conversation_locks.hold(conversation_id):
            if turn_input.images:
                await self._assert_image_model(conversation_id)
            turn = await asyncio.to_thread(
                self._repository.enqueue_turn,
                conversation_id=conversation_id,
                source=source,
                turn_input=turn_input,
                input_message_id=input_message_id,
                schedule_fire_id=schedule_fire_id,
                ingress_message_id=ingress_message_id,
                requested_by_user_id=requested_by_user_id,
            )
            self._volatile_turns.put_input(turn.id, turn_input)
        await self._mailboxes.wake(conversation_id)
        return turn

    @property
    def volatile_turns(self) -> VolatileTurnStore:
        return self._volatile_turns

    def event_metrics(self) -> dict[str, float | int]:
        return self._event_pump.metrics()

    async def _assert_image_model(self, conversation_id: str) -> None:
        conversation = await asyncio.to_thread(
            self._repository.get_conversation, conversation_id
        )
        project = await asyncio.to_thread(
            self._repository.get_project, conversation.project_id
        )
        config = await asyncio.to_thread(
            self._repository.effective_thread_config, conversation_id
        )
        runtime, _lease = await self._runtime_supervisor.ensure(project)
        catalog = await runtime.list_models()
        candidates = tuple(
            model
            for model in catalog.models
            if (
                config.model in {model.id, model.model}
                if config.model is not None
                else model.is_default
            )
        )
        if len(candidates) != 1:
            raise ConflictError("effective image model is missing or ambiguous")
        if "image" not in candidates[0].input_modalities:
            raise ConflictError(
                f"effective model {candidates[0].model} does not support image input"
            )

    async def cancel(
        self,
        turn_id: str,
        *,
        interaction_id: str | None = None,
    ) -> TurnRecord:
        await asyncio.to_thread(
            self._repository.request_cancel,
            turn_id,
            origin=InterruptOrigin.USER,
            command_interaction_id=interaction_id,
        )
        async with self._active_lock:
            active = self._active.get(turn_id)
        if active is not None:
            await self._interrupt_if_requested(
                turn_id,
                active,
                raise_on_unknown=True,
            )
        return await asyncio.to_thread(self._repository.get_turn, turn_id)

    async def steer(
        self,
        turn_id: str,
        text: str,
        *,
        interaction_id: str | None = None,
        actor_user_id: int | None = None,
    ) -> None:
        if not text.strip():
            raise InvariantError("steer text may not be empty")
        turn = await asyncio.to_thread(self._repository.get_turn, turn_id)
        if turn.state is not TurnState.RUNNING:
            raise ConflictError(f"Turn is not steerable while {turn.state.value}")
        async with self._active_lock:
            active = self._active.get(turn_id)
        if active is None:
            raise ConflictError("active Turn handle is not available")
        instruction = text.strip()
        instruction_hash = sha256_text(instruction)
        if interaction_id is not None:
            await asyncio.to_thread(
                self._repository.mark_command_effect,
                interaction_id,
                effect_kind="turn_steer",
                effect_correlation_id=turn_id,
                turn_id=turn_id,
                actor_user_id=actor_user_id,
                audit_action="turn.steer_requested",
                audit_payload={"instruction_hash": instruction_hash},
            )
        try:
            await active.runtime.steer(active.identity, instruction)
        except (ProviderOutcomeUnknown, asyncio.CancelledError):
            raise
        except CodexDError as exc:
            if interaction_id is not None:
                try:
                    await asyncio.to_thread(
                        self._repository.record_steer_rejected,
                        turn_id=turn_id,
                        instruction_hash=instruction_hash,
                        actor_user_id=actor_user_id,
                        interaction_id=interaction_id,
                        code=exc.code,
                        message=str(exc)[:512],
                    )
                except Exception as commit_error:
                    exc.add_note(
                        "steer rejection persistence failed: "
                        f"{type(commit_error).__name__}"
                    )
            raise
        if interaction_id is not None:
            await asyncio.to_thread(
                self._repository.record_steer_accepted,
                turn_id=turn_id,
                instruction_hash=instruction_hash,
                actor_user_id=actor_user_id,
                interaction_id=interaction_id,
            )

    async def close(self, *, drain_seconds: float = 30) -> None:
        async with self._active_lock:
            if self._closing:
                return
            self._closing = True
        started_closing = time.monotonic()
        deadline = started_closing + drain_seconds
        force_close_at = started_closing + (drain_seconds * 0.8)
        interrupt_requested: set[str] = set()
        runtime_close_started = False
        while time.monotonic() < deadline:
            async with self._active_lock:
                active = tuple(self._active.items())
                inflight = bool(self._inflight)
            pending_interrupts = tuple(
                (turn_id, handle)
                for turn_id, handle in active
                if turn_id not in interrupt_requested
            )
            interrupt_requested.update(
                turn_id for turn_id, _handle in pending_interrupts
            )
            if pending_interrupts:
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    interrupt_timeout = min(
                        remaining,
                        max(0.001, drain_seconds * 0.1),
                    )
                    await asyncio.gather(
                        *(
                            self._interrupt_for_shutdown(
                                turn_id,
                                handle,
                                interrupt_timeout=interrupt_timeout,
                            )
                            for turn_id, handle in pending_interrupts
                        )
                    )
            if not active and not inflight:
                break
            if (
                not runtime_close_started
                and time.monotonic() >= force_close_at
            ):
                runtime_close_started = True
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    try:
                        await asyncio.wait_for(
                            self._runtime_supervisor.close(
                                timeout_seconds=remaining,
                            ),
                            timeout=remaining,
                        )
                    except Exception as exc:
                        self._critical_failure(exc)
            await asyncio.sleep(0.01)
        await self._mailboxes.close()
        await asyncio.to_thread(self._repository.interrupt_for_shutdown)

    async def _interrupt_for_shutdown(
        self,
        turn_id: str,
        handle: ActiveTurn,
        *,
        interrupt_timeout: float,
    ) -> None:
        with suppress(CodexDError):
            await asyncio.to_thread(
                self._repository.request_cancel,
                turn_id,
                origin=InterruptOrigin.SHUTDOWN,
            )
        with suppress(AdapterError, NotFoundError, TimeoutError):
            await asyncio.wait_for(
                handle.runtime.interrupt(handle.identity),
                timeout=interrupt_timeout,
            )

    async def _process_turn(self, queued: TurnRecord) -> float | None:
        async with self._active_lock:
            if self._closing:
                return 3600.0
            self._inflight.add(queued.id)
        try:
            return await self._process_turn_inner(queued)
        finally:
            async with self._active_lock:
                self._inflight.discard(queued.id)

    async def _process_turn_inner(self, queued: TurnRecord) -> float | None:
        conversation = await asyncio.to_thread(
            self._repository.get_conversation, queued.conversation_id
        )
        if conversation.state.value not in {"active", "uninitialized"}:
            await asyncio.to_thread(
                self._repository.terminal_turn,
                queued.id,
                target=TurnState.INTERRUPTED,
                terminal_code=f"conversation_{conversation.state.value}",
            )
            return None
        if conversation.provider_barrier_kind in {"compact", "unknown_effect"}:
            return 2.0
        project = await asyncio.to_thread(
            self._repository.get_project, conversation.project_id
        )
        if self._closing:
            return None
        try:
            runtime, lease = await self._runtime_supervisor.ensure(project)
        except RuntimeUnavailable as exc:
            if exc.failure.retryable:
                return 5.0
            await self._terminal_before_provider(queued, project, exc.failure.code)
            return None
        except AdapterError as exc:
            await self._terminal_before_provider(queued, project, exc.failure.code)
            return None
        except (SecurityError, InvariantError) as exc:
            if self._closing:
                return None
            await asyncio.to_thread(
                self._repository.block_conversation,
                conversation.id,
                reason=getattr(exc, "code", "runtime_configuration_invalid"),
            )
            await self._terminal_before_provider(
                queued,
                project,
                getattr(exc, "code", "runtime_configuration_invalid"),
            )
            return None

        thread_config = _thread_config(queued)
        try:
            revision = await asyncio.to_thread(
                self._repository.get_active_revision, conversation.id
            )
            if (
                revision is not None
                and self._schedule_tool_supported
                and not revision.dynamic_tools_enabled
                and queued.source_kind is TurnSource.DISCORD
            ):
                await asyncio.to_thread(
                    self._repository.enqueue_dynamic_tool_upgrade_notice,
                    conversation_id=conversation.id,
                    turn_id=queued.id,
                )
            if revision is None:
                await asyncio.to_thread(
                    self._repository.set_provider_barrier,
                    conversation.id,
                    "unknown_effect",
                )
                provider_succeeded = False
                try:
                    thread_identity = await runtime.start_thread(
                        cwd=project.root_path, config=thread_config
                    )
                    provider_succeeded = True
                    revision = await asyncio.to_thread(
                        self._repository.activate_thread_revision,
                        conversation_id=conversation.id,
                        identity=thread_identity,
                        config=thread_config,
                    )
                    await asyncio.to_thread(
                        self._repository.attach_turn_revision, queued.id, revision.id
                    )
                except BaseException as exc:
                    await self._finish_provider_effect_failure(
                        conversation_id=conversation.id,
                        project_id=project.id,
                        operation="automatic_thread_start",
                        error=exc,
                        provider_succeeded=provider_succeeded,
                    )
                    raise
                try:
                    await asyncio.to_thread(
                        self._repository.clear_provider_barrier,
                        conversation.id,
                    )
                except BaseException as exc:
                    await self._finish_provider_effect_failure(
                        conversation_id=conversation.id,
                        project_id=project.id,
                        operation="automatic_thread_start_commit",
                        error=exc,
                        provider_succeeded=True,
                    )
                    raise
            else:
                try:
                    snapshot = await runtime.read_thread(revision.provider_thread_id)
                except NotFoundError:
                    identity = await runtime.resume_thread(
                        thread_id=revision.provider_thread_id,
                        cwd=project.root_path,
                        config=thread_config,
                    )
                    await self._validate_revision_identity(
                        conversation.id,
                        revision,
                        identity,
                        require_requested_id=True,
                    )
                    revision = await asyncio.to_thread(
                        self._repository.activate_thread_revision,
                        conversation_id=conversation.id,
                        identity=identity,
                        config=thread_config,
                    )
                    snapshot = await runtime.read_thread(identity.thread_id)
                await self._validate_revision_identity(
                    conversation.id,
                    revision,
                    snapshot.identity,
                    require_requested_id=False,
                )
                thread_identity = snapshot.identity
                if snapshot.state is ThreadProviderState.ACTIVE:
                    await asyncio.to_thread(
                        self._repository.set_provider_barrier,
                        conversation.id,
                        "external_active",
                    )
                    self._provider_barrier_observer(conversation.id)
                    return 2.0
                if snapshot.state is ThreadProviderState.NOT_LOADED:
                    identity = await runtime.resume_thread(
                        thread_id=revision.provider_thread_id,
                        cwd=project.root_path,
                        config=thread_config,
                    )
                    await self._validate_revision_identity(
                        conversation.id,
                        revision,
                        identity,
                        require_requested_id=True,
                    )
                    revision = await asyncio.to_thread(
                        self._repository.activate_thread_revision,
                        conversation_id=conversation.id,
                        identity=identity,
                        config=thread_config,
                    )
                    snapshot = await runtime.read_thread(identity.thread_id)
                    await self._validate_revision_identity(
                        conversation.id,
                        revision,
                        snapshot.identity,
                        require_requested_id=False,
                    )
                    if snapshot.state is ThreadProviderState.NOT_LOADED:
                        return 1.0
                    thread_identity = snapshot.identity
                    if snapshot.state is ThreadProviderState.ACTIVE:
                        await asyncio.to_thread(
                            self._repository.set_provider_barrier,
                            conversation.id,
                            "external_active",
                        )
                        self._provider_barrier_observer(conversation.id)
                        return 2.0
                if snapshot.state in {
                    ThreadProviderState.SYSTEM_ERROR,
                    ThreadProviderState.UNKNOWN,
                }:
                    await asyncio.to_thread(
                        self._repository.block_conversation,
                        conversation.id,
                        reason=f"provider_thread_{snapshot.state.value}",
                    )
                    await asyncio.to_thread(
                        self._repository.terminal_turn,
                        queued.id,
                        target=TurnState.INTERRUPTED,
                        terminal_code=f"provider_thread_{snapshot.state.value}",
                    )
                    return None
                if conversation.provider_barrier_kind == "external_active":
                    await asyncio.to_thread(
                        self._repository.clear_provider_barrier, conversation.id
                    )
        except AdapterError as exc:
            await self._terminal_before_provider(
                queued, project, exc.failure.code
            )
            if isinstance(exc, RuntimeUnavailable):
                await self._runtime_supervisor.report_failure(
                    project,
                    expected_lease_id=lease.id,
                    expected_generation=lease.generation,
                    failure_code=exc.failure.code,
                )
            return None
        except (ConflictError, InvariantError) as exc:
            await self._terminal_before_provider(
                queued, project, getattr(exc, "code", "thread_identity_error")
            )
            return None

        if self._closing:
            return None
        claimed = await asyncio.to_thread(
            self._repository.claim_turn,
            queued.id,
            runtime_lease_id=lease.id,
            runtime_generation=lease.generation,
        )
        try:
            if claimed.source_kind is TurnSource.DISCORD:
                volatile_input = self._volatile_turns.input(claimed.id)
                if volatile_input is None:
                    raise ConflictError("volatile Turn input is unavailable")
                turn_input = await asyncio.to_thread(
                    self._repository.load_turn_input,
                    claimed.id,
                    volatile_text=volatile_input.text,
                    use_volatile_text=True,
                )
            else:
                turn_input = await asyncio.to_thread(
                    self._repository.load_turn_input,
                    claimed.id,
                )
            await self._validate_turn_catalog(
                runtime,
                turn=claimed,
                turn_input=turn_input,
            )
            if self._closing:
                await self._terminal_claimed_for_shutdown(claimed.id)
                return None
            started = await runtime.start_turn(
                local_turn_id=claimed.id,
                thread=thread_identity,
                input=turn_input,
                config=_turn_config(claimed, project),
            )
            self._volatile_turns.drop_input(claimed.id)
        except RuntimeUnavailable as exc:
            await asyncio.to_thread(
                self._repository.terminal_turn,
                claimed.id,
                target=TurnState.INTERRUPTED,
                terminal_code=exc.failure.code,
                error_code=exc.failure.code,
                error_message_redacted=exc.failure.message,
            )
            await self._runtime_supervisor.report_failure(
                project,
                expected_lease_id=lease.id,
                expected_generation=lease.generation,
                failure_code=exc.failure.code,
            )
            return None
        except AdapterError as exc:
            await asyncio.to_thread(
                self._repository.terminal_turn,
                claimed.id,
                target=TurnState.FAILED,
                terminal_code=exc.failure.code,
                error_code=exc.failure.code,
                error_message_redacted=exc.failure.message,
            )
            return None
        except (
            CodexDError,
            OSError,
            ValueError,
            TypeError,
            KeyError,
        ) as exc:
            await asyncio.to_thread(
                self._repository.terminal_turn,
                claimed.id,
                target=TurnState.FAILED,
                terminal_code=getattr(exc, "code", "input_validation_failed"),
                error_code=getattr(exc, "code", "input_validation_failed"),
                error_message_redacted=str(exc),
            )
            return None

        try:
            self._validate_started_turn_identity(
                started=started,
                turn=claimed,
                lease=lease,
            )
        except BaseException as exc:
            await self._abandon_unpersisted_provider_turn(
                project=project,
                lease=lease,
                runtime=runtime,
                started=started,
                turn_id=claimed.id,
                failure_code="runtime_turn_identity_mismatch",
                cause=exc,
            )
        if self._closing:
            await asyncio.to_thread(
                self._repository.request_cancel,
                claimed.id,
                origin=InterruptOrigin.SHUTDOWN,
            )

        try:
            provider_turn_id = started.identity.provider_turn_id
            assert provider_turn_id is not None
            running = await asyncio.to_thread(
                self._repository.mark_turn_running,
                claimed.id,
                provider_turn_id,
            )
        except BaseException as exc:
            await self._abandon_unpersisted_provider_turn(
                project=project,
                lease=lease,
                runtime=runtime,
                started=started,
                turn_id=claimed.id,
                failure_code="provider_identity_persist_failed",
                cause=exc,
            )
        pump_task = asyncio.create_task(
            self._event_pump.run(local_turn_id=running.id, started=started),
            name=f"codexd-event-pump-{running.id}",
        )
        try:
            async with self._active_lock:
                if running.id in self._active:
                    raise InvariantError("Turn already has an EventPump")
                active = ActiveTurn(runtime, started.identity)
                self._active[running.id] = active
            current = await asyncio.to_thread(
                self._repository.get_turn,
                running.id,
            )
            if current.state is TurnState.CANCELLING:
                await self._interrupt_if_requested(
                    running.id,
                    active,
                    raise_on_unknown=False,
                )
            try:
                result = await pump_task
            except EventJournalError as exc:
                try:
                    await runtime.interrupt(started.identity)
                except (AdapterError, NotFoundError) as interrupt_error:
                    exc.add_note(
                        "provider interrupt also failed: "
                        f"{type(interrupt_error).__name__}"
                    )
                finally:
                    self._critical_failure(exc)
                raise
            if result.terminal_code.startswith(("runtime_", "stream_")):
                await self._runtime_supervisor.report_failure(
                    project,
                    expected_lease_id=lease.id,
                    expected_generation=lease.generation,
                    failure_code=result.terminal_code,
                )
        finally:
            async with self._active_lock:
                self._active.pop(running.id, None)
            if not pump_task.done():
                pump_task.cancel()
                with suppress(asyncio.CancelledError):
                    await pump_task
        return None

    async def _interrupt_if_requested(
        self,
        turn_id: str,
        active: ActiveTurn,
        *,
        raise_on_unknown: bool,
    ) -> None:
        claimed = await asyncio.to_thread(
            self._repository.claim_turn_interrupt,
            turn_id,
        )
        if not claimed:
            return
        try:
            await active.runtime.interrupt(active.identity)
        except asyncio.CancelledError:
            await asyncio.shield(
                asyncio.to_thread(
                    self._repository.resolve_turn_interrupt,
                    turn_id,
                    outcome="unknown",
                    code="command_cancelled",
                )
            )
            raise
        except ProviderOutcomeUnknown as exc:
            await asyncio.to_thread(
                self._repository.resolve_turn_interrupt,
                turn_id,
                outcome="unknown",
                code=exc.failure.code,
            )
            await self._record_interrupt_incident(
                turn_id,
                code="turn_interrupt_outcome_unknown",
                adapter_code=exc.failure.code,
            )
            if raise_on_unknown:
                raise
        except AdapterError as exc:
            await asyncio.to_thread(
                self._repository.resolve_turn_interrupt,
                turn_id,
                outcome="failed",
                code=exc.failure.code,
            )
            await self._record_interrupt_incident(
                turn_id,
                code="turn_interrupt_failed",
                adapter_code=exc.failure.code,
            )
        except CodexDError as exc:
            code = getattr(exc, "code", "interrupt_failed")
            await asyncio.to_thread(
                self._repository.resolve_turn_interrupt,
                turn_id,
                outcome="failed",
                code=code,
            )
            await self._record_interrupt_incident(
                turn_id,
                code="turn_interrupt_failed",
                adapter_code=code,
            )
        except Exception as exc:
            await asyncio.to_thread(
                self._repository.resolve_turn_interrupt,
                turn_id,
                outcome="unknown",
                code=type(exc).__name__,
            )
            await self._record_interrupt_incident(
                turn_id,
                code="turn_interrupt_outcome_unknown",
                adapter_code=type(exc).__name__,
            )
            if raise_on_unknown:
                raise
        else:
            await asyncio.to_thread(
                self._repository.resolve_turn_interrupt,
                turn_id,
                outcome="sent",
                code="ok",
            )

    async def _record_interrupt_incident(
        self,
        turn_id: str,
        *,
        code: str,
        adapter_code: str,
    ) -> None:
        turn = await asyncio.to_thread(self._repository.get_turn, turn_id)
        conversation = await asyncio.to_thread(
            self._repository.get_conversation,
            turn.conversation_id,
        )
        await asyncio.to_thread(
            self._repository.record_incident,
            severity="warning",
            code=code,
            summary=(
                "Turn cancellation remains pending while codexD continues "
                "observing the provider stream"
            ),
            project_id=conversation.project_id,
            conversation_id=conversation.id,
            turn_id=turn_id,
            details={"adapter_code": adapter_code},
        )

    async def _validate_revision_identity(
        self,
        conversation_id: str,
        revision: ThreadRevisionRecord,
        identity: ThreadIdentity,
        *,
        require_requested_id: bool,
    ) -> None:
        mismatch = (
            identity.thread_id != revision.provider_thread_id
            or identity.provider_session_id != revision.provider_session_id
            or (
                require_requested_id
                and identity.requested_thread_id != revision.provider_thread_id
            )
        )
        if not mismatch:
            return
        await asyncio.to_thread(
            self._repository.block_conversation,
            conversation_id,
            reason="provider_thread_identity_mismatch",
        )
        raise InvariantError("provider resumed a different Thread identity")

    @staticmethod
    def _validate_started_turn_identity(
        *,
        started: StartedTurn,
        turn: TurnRecord,
        lease: RuntimeLeaseRecord,
    ) -> None:
        if not hasattr(started, "identity"):
            raise InvariantError("provider Turn identity is missing")
        identity = started.identity
        if (
            identity.local_turn_id != turn.id
            or identity.runtime_generation != lease.generation
            or identity.provider_turn_id is None
        ):
            raise InvariantError(
                "provider Turn identity does not match the claimed runtime lease"
            )

    async def _abandon_unpersisted_provider_turn(
        self,
        *,
        project: ProjectRecord,
        lease: RuntimeLeaseRecord,
        runtime: CodexRuntime,
        started: StartedTurn,
        turn_id: str,
        failure_code: str,
        cause: BaseException,
    ) -> None:
        cleanup_errors: list[BaseException] = []
        try:
            await asyncio.wait_for(
                runtime.interrupt(started.identity),
                timeout=_PROVIDER_CLEANUP_TIMEOUT_SECONDS,
            )
        except BaseException as exc:
            cleanup_errors.append(exc)
        finally:
            try:
                await asyncio.to_thread(
                    self._repository.terminal_turn,
                    turn_id,
                    target=TurnState.INTERRUPTED,
                    terminal_code=failure_code,
                    error_code=failure_code,
                    error_message_redacted=str(cause),
                )
            except BaseException as exc:
                cleanup_errors.append(exc)
        try:
            await self._runtime_supervisor.report_failure(
                project,
                expected_lease_id=lease.id,
                expected_generation=lease.generation,
                failure_code=failure_code,
            )
        except BaseException as exc:
            cleanup_errors.append(exc)
        for cleanup_error in cleanup_errors:
            cause.add_note(
                "unpersisted provider Turn cleanup also failed: "
                f"{type(cleanup_error).__name__}"
            )
        self._critical_failure(cause)
        raise cause

    async def _finish_provider_effect_failure(
        self,
        *,
        conversation_id: str,
        project_id: str,
        operation: str,
        error: BaseException,
        provider_succeeded: bool,
    ) -> None:
        outcome_unknown = (
            provider_succeeded
            or isinstance(error, (ProviderOutcomeUnknown, asyncio.CancelledError))
        )
        if not outcome_unknown:
            try:
                await asyncio.to_thread(
                    self._repository.clear_provider_barrier,
                    conversation_id,
                )
            except Exception as cleanup_error:
                error.add_note(
                    "provider barrier cleanup failed: "
                    f"{type(cleanup_error).__name__}"
                )
            return
        failures: list[Exception] = []
        try:
            await asyncio.to_thread(
                self._repository.block_conversation,
                conversation_id,
                reason="provider_effect_outcome_unknown",
            )
        except Exception as exc:
            failures.append(exc)
        try:
            await asyncio.to_thread(
                self._repository.record_incident,
                severity="critical",
                code="provider_effect_outcome_unknown",
                summary="Provider mutation outcome could not be committed safely",
                project_id=project_id,
                conversation_id=conversation_id,
                details={"operation": operation},
            )
        except Exception as exc:
            failures.append(exc)
        for failure in failures:
            error.add_note(
                "provider outcome fencing also failed: "
                f"{type(failure).__name__}"
            )

    async def _validate_turn_catalog(
        self,
        runtime: CodexRuntime,
        *,
        turn: TurnRecord,
        turn_input: TurnInput,
    ) -> ModelDescriptor:
        if turn_input.files:
            manifest = await runtime.capabilities()
            if manifest.optional.get("mention.input") is not True:
                raise file_input_unsupported(
                    generation=runtime.generation,
                    turn_id=turn.id,
                )
        catalog = await runtime.list_models()
        descriptor = _effective_model(catalog, turn.effective_model)
        if turn_input.images and "image" not in descriptor.input_modalities:
            raise InvariantError(
                f"effective model {descriptor.model} does not support image input"
            )
        if (
            turn.effective_reasoning_effort is not None
            and turn.effective_reasoning_effort
            not in descriptor.supported_reasoning_efforts
        ):
            raise InvariantError(
                f"effective reasoning effort is unavailable for {descriptor.model}"
            )
        if (
            turn.effective_personality is not None
            and not descriptor.supports_personality
        ):
            raise InvariantError(
                f"effective personality is unavailable for {descriptor.model}"
            )
        if (
            turn.effective_service_tier is not None
            and turn.effective_service_tier
            not in {tier.id for tier in descriptor.service_tiers}
        ):
            raise InvariantError(
                f"effective service tier is unavailable for {descriptor.model}"
            )
        return descriptor

    async def _terminal_claimed_for_shutdown(self, turn_id: str) -> None:
        self._volatile_turns.discard(turn_id)
        await asyncio.to_thread(
            self._repository.request_cancel,
            turn_id,
            origin=InterruptOrigin.SHUTDOWN,
        )
        await asyncio.to_thread(
            self._repository.terminal_turn,
            turn_id,
            target=TurnState.INTERRUPTED,
            terminal_code="shutdown_before_provider",
        )

    async def _terminal_before_provider(
        self, turn: TurnRecord, project: ProjectRecord, code: str
    ) -> None:
        self._volatile_turns.discard(turn.id)
        await asyncio.to_thread(
            self._repository.terminal_turn,
            turn.id,
            target=TurnState.INTERRUPTED,
            terminal_code=code,
        )
        await asyncio.to_thread(
            self._repository.record_incident,
            severity="error",
            code=code,
            summary="Turn could not reach the Codex provider",
            project_id=project.id,
            conversation_id=turn.conversation_id,
            turn_id=turn.id,
        )

    async def _mailbox_error(self, conversation_id: str, exc: Exception) -> None:
        conversation = await asyncio.to_thread(
            self._repository.get_conversation, conversation_id
        )
        await asyncio.to_thread(
            self._repository.record_incident,
            severity="error",
            code=f"mailbox_{type(exc).__name__}",
            summary="Conversation mailbox handler failed",
            project_id=conversation.project_id,
            conversation_id=conversation_id,
        )


def _thread_config(turn: TurnRecord) -> ThreadConfig:
    return ThreadConfig(
        model=turn.effective_model,
        personality=turn.effective_personality,
        sandbox=turn.effective_sandbox,
        approval_mode=ApprovalPolicy.AUTO_REVIEW,
        service_tier=turn.effective_service_tier,
        web_search_mode=WebSearchMode(turn.effective_web_search_mode),
    )


def _turn_config(turn: TurnRecord, project: ProjectRecord) -> TurnConfig:
    return TurnConfig(
        cwd=project.root_path,
        sandbox=turn.effective_sandbox,
        approval_mode=ApprovalPolicy.AUTO_REVIEW,
        model=turn.effective_model,
        reasoning_effort=turn.effective_reasoning_effort,
        reasoning_summary=turn.effective_reasoning_summary,
        personality=turn.effective_personality,
        service_tier=turn.effective_service_tier,
    )


def _effective_model(
    catalog: ModelCatalogSnapshot,
    requested: str | None,
) -> ModelDescriptor:
    candidates = tuple(
        model
        for model in catalog.models
        if (
            requested in {model.id, model.model}
            if requested is not None
            else model.is_default
        )
    )
    if len(candidates) == 1:
        return candidates[0]
    if not candidates and not catalog.complete:
        raise InvariantError("Codex model catalog is incomplete")
    if not candidates:
        raise InvariantError("effective model is not present in the Codex catalog")
    raise InvariantError("effective model is ambiguous in the Codex catalog")
