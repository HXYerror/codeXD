from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass, replace

from codexd.application.conversation_locks import ConversationLocks
from codexd.domain.conversations import (
    ApprovalPolicy,
    ConversationState,
    SandboxProfile,
    ThreadConfig,
    ThreadIdentity,
    ThreadProviderState,
    WebSearchMode,
)
from codexd.domain.ids import sha256_text
from codexd.domain.models import ModelCatalogSnapshot, ModelDescriptor
from codexd.errors import InvariantError, NotFoundError
from codexd.runtime.errors import AdapterError, ProviderOutcomeUnknown
from codexd.runtime.port import CodexRuntime
from codexd.runtime.supervisor import RuntimeSupervisor
from codexd.storage.records import (
    ConversationRecord,
    ProjectRecord,
    ThreadRevisionRecord,
)
from codexd.storage.repository import Repository

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SessionStatus:
    conversation: ConversationRecord
    active_revision: ThreadRevisionRecord | None


class SessionLifecycleCoordinator:
    def __init__(
        self,
        *,
        repository: Repository,
        runtimes: RuntimeSupervisor,
        locks: ConversationLocks,
    ) -> None:
        self._repository = repository
        self._runtimes = runtimes
        self._locks = locks
        self._barrier_tasks: dict[str, asyncio.Task[None]] = {}
        self._closed = False

    async def restore_provider_barriers(self) -> None:
        conversation_ids = await asyncio.to_thread(
            self._repository.provider_barrier_conversation_ids
        )
        for conversation_id in conversation_ids:
            self.monitor_provider_barrier(conversation_id)

    async def close(self) -> None:
        self._closed = True
        tasks = tuple(self._barrier_tasks.values())
        self._barrier_tasks.clear()
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task

    async def list_revisions(
        self, conversation_id: str, *, limit: int | None = None
    ) -> tuple[ThreadRevisionRecord, ...]:
        return await asyncio.to_thread(
            self._repository.list_thread_revisions, conversation_id, limit=limit
        )

    async def status(self, conversation_id: str) -> SessionStatus:
        conversation, revision = await asyncio.gather(
            asyncio.to_thread(self._repository.get_conversation, conversation_id),
            asyncio.to_thread(self._repository.get_active_revision, conversation_id),
        )
        return SessionStatus(conversation, revision)

    async def new(
        self,
        conversation_id: str,
        *,
        interaction_id: str | None = None,
    ) -> ThreadRevisionRecord:
        async with self._locks.hold(conversation_id):
            conversation, project, config = await self._context(
                conversation_id, reject_active_schedules=False
            )
            runtime, _lease = await self._runtimes.ensure(project)
            await self._begin_provider_effect(
                conversation.id,
                interaction_id=interaction_id,
                effect_kind="session_new",
                effect_correlation_id=conversation.id,
            )
            provider_succeeded = False
            try:
                identity = await runtime.start_thread(
                    cwd=project.root_path,
                    config=config,
                )
                provider_succeeded = True
                revision = await self._activate(
                    conversation=conversation,
                    identity=identity,
                    config=config,
                    operation="session_new",
                    complete_provider_effect=True,
                )
            except BaseException as exc:
                await self._finish_provider_effect_failure(
                    conversation,
                    operation="session_new",
                    error=exc,
                    provider_succeeded=provider_succeeded,
                )
                raise
            return revision

    async def resume(
        self,
        conversation_id: str,
        revision_ref: str,
        *,
        interaction_id: str | None = None,
    ) -> ThreadRevisionRecord:
        async with self._locks.hold(conversation_id):
            conversation, project, _current_config = await self._context(
                conversation_id, reject_active_schedules=False
            )
            target = await asyncio.to_thread(
                self._repository.resolve_thread_revision,
                conversation_id,
                revision_ref,
            )
            config = _decode_thread_config(target.thread_config_json)
            runtime, _lease = await self._runtimes.ensure(project)
            if target.state == "archived":
                await _require_capabilities(
                    runtime, "thread.archive", "thread.unarchive"
                )
                await self._begin_provider_effect(
                    conversation.id,
                    interaction_id=interaction_id,
                    effect_kind="session_resume",
                    effect_correlation_id=target.id,
                )
                provider_succeeded = False
                try:
                    identity = await runtime.unarchive_thread(
                        target.provider_thread_id
                    )
                    provider_succeeded = True
                    await self._validate_identity(conversation_id, target, identity)
                    revision = await self._activate(
                        conversation=conversation,
                        identity=identity,
                        config=config,
                        operation="session_resume",
                        restore_conversation_config=True,
                        complete_provider_effect=True,
                    )
                except BaseException as exc:
                    await self._finish_provider_effect_failure(
                        conversation,
                        operation="session_unarchive",
                        error=exc,
                        provider_succeeded=provider_succeeded,
                    )
                    raise
                return revision
            await self._begin_provider_effect(
                conversation.id,
                interaction_id=interaction_id,
                effect_kind="session_resume",
                effect_correlation_id=target.id,
            )
            provider_succeeded = False
            try:
                identity = await runtime.resume_thread(
                    thread_id=target.provider_thread_id,
                    cwd=project.root_path,
                    config=config,
                )
                provider_succeeded = True
                await self._validate_identity(conversation_id, target, identity)
                revision = await self._activate(
                    conversation=conversation,
                    identity=identity,
                    config=config,
                    operation="session_resume",
                    restore_conversation_config=True,
                    complete_provider_effect=True,
                )
            except BaseException as exc:
                await self._finish_provider_effect_failure(
                    conversation,
                    operation="session_resume",
                    error=exc,
                    provider_succeeded=provider_succeeded,
                )
                raise
            return revision

    async def fork(
        self,
        conversation_id: str,
        *,
        interaction_id: str | None = None,
    ) -> ThreadRevisionRecord:
        async with self._locks.hold(conversation_id):
            conversation, project, config = await self._context(
                conversation_id, reject_active_schedules=False
            )
            source = await asyncio.to_thread(
                self._repository.get_active_revision, conversation_id
            )
            if source is None:
                raise InvariantError("Conversation has no active revision to fork")
            runtime, _lease = await self._runtimes.ensure(project)
            await _require_capabilities(runtime, "thread.fork")
            await self._begin_provider_effect(
                conversation.id,
                interaction_id=interaction_id,
                effect_kind="session_fork",
                effect_correlation_id=source.id,
            )
            provider_succeeded = False
            try:
                identity = await runtime.fork_thread(
                    thread_id=source.provider_thread_id,
                    cwd=project.root_path,
                    config=config,
                )
                provider_succeeded = True
                if (
                    identity.thread_id == source.provider_thread_id
                    or identity.forked_from_thread_id != source.provider_thread_id
                    or identity.provider_session_id != source.provider_session_id
                ):
                    await asyncio.to_thread(
                        self._repository.record_incident,
                        severity="critical",
                        code="fork_identity_mismatch",
                        summary="Codex fork returned an unexpected thread identity",
                        project_id=conversation.project_id,
                        conversation_id=conversation.id,
                        details={
                            "source_revision_id": source.id,
                            "source_thread_hash": sha256_text(
                                source.provider_thread_id
                            )[:16],
                            "returned_thread_hash": sha256_text(
                                identity.thread_id
                            )[:16],
                        },
                    )
                    raise InvariantError("provider fork identity validation failed")
                revision = await self._activate(
                    conversation=conversation,
                    identity=identity,
                    config=config,
                    operation="session_fork",
                    parent_revision_id=source.id,
                    complete_provider_effect=True,
                )
            except BaseException as exc:
                await self._finish_provider_effect_failure(
                    conversation,
                    operation="session_fork",
                    error=exc,
                    provider_succeeded=provider_succeeded,
                )
                raise
            return revision

    async def archive(
        self,
        conversation_id: str,
        *,
        interaction_id: str | None = None,
    ) -> ConversationRecord:
        async with self._locks.hold(conversation_id):
            conversation, project, _config = await self._context(
                conversation_id, reject_active_schedules=True
            )
            revision = await asyncio.to_thread(
                self._repository.get_active_revision, conversation_id
            )
            if revision is None:
                raise InvariantError("Conversation has no active revision to archive")
            runtime, _lease = await self._runtimes.ensure(project)
            await _require_capabilities(
                runtime, "thread.archive", "thread.unarchive"
            )
            await self._begin_provider_effect(
                conversation.id,
                interaction_id=interaction_id,
                effect_kind="session_archive",
                effect_correlation_id=revision.id,
            )
            provider_succeeded = False
            try:
                await runtime.archive_thread(revision.provider_thread_id)
                provider_succeeded = True
                archived = await asyncio.to_thread(
                    self._repository.archive_active_revision,
                    conversation_id,
                    revision.id,
                    complete_provider_effect=True,
                )
            except BaseException as exc:
                await self._finish_provider_effect_failure(
                    conversation,
                    operation="session_archive",
                    error=exc,
                    provider_succeeded=provider_succeeded,
                )
                raise
            return archived

    async def clear(
        self,
        conversation_id: str,
        *,
        interaction_id: str | None = None,
    ) -> ConversationRecord:
        async with self._locks.hold(conversation_id):
            await asyncio.to_thread(
                self._repository.assert_conversation_mutable,
                conversation_id,
                reject_active_schedules=True,
            )
            return await asyncio.to_thread(
                self._repository.clear_conversation,
                conversation_id,
                command_interaction_id=interaction_id,
            )

    async def rename(
        self,
        conversation_id: str,
        name: str,
        *,
        interaction_id: str | None = None,
    ) -> ThreadRevisionRecord:
        normalized = _normalize_thread_name(name)
        async with self._locks.hold(conversation_id):
            conversation, project, config = await self._context(
                conversation_id, reject_active_schedules=False
            )
            revision = await asyncio.to_thread(
                self._repository.get_active_revision, conversation_id
            )
            if revision is None:
                raise InvariantError("Conversation has no active revision to rename")
            runtime, _lease = await self._runtimes.ensure(project)
            await _require_capabilities(runtime, "thread.set_name")
            try:
                snapshot = await runtime.read_thread(revision.provider_thread_id)
            except NotFoundError:
                identity = await runtime.resume_thread(
                    thread_id=revision.provider_thread_id,
                    cwd=project.root_path,
                    config=config,
                )
            else:
                identity = snapshot.identity
            await self._validate_identity(conversation_id, revision, identity)
            await self._begin_provider_effect(
                conversation.id,
                interaction_id=interaction_id,
                effect_kind="session_rename",
                effect_correlation_id=revision.id,
            )
            provider_succeeded = False
            try:
                try:
                    await runtime.set_thread_name(
                        revision.provider_thread_id,
                        normalized,
                    )
                    provider_succeeded = True
                    renamed = await asyncio.to_thread(
                        self._repository.rename_active_revision,
                        conversation_id,
                        revision.id,
                        normalized,
                        complete_provider_effect=True,
                    )
                except Exception as commit_error:
                    if provider_succeeded:
                        try:
                            await self._record_commit_failure(
                                conversation,
                                "session_rename_commit_failed",
                                details={
                                    "revision_id": revision.id,
                                    "provider_thread_hash": sha256_text(
                                        revision.provider_thread_id
                                    )[:16],
                                },
                            )
                        except Exception as incident_error:
                            raise ExceptionGroup(
                                "rename commit and incident persistence failed",
                                (commit_error, incident_error),
                            ) from commit_error
                    raise
            except BaseException as exc:
                await self._finish_provider_effect_failure(
                    conversation,
                    operation="session_rename",
                    error=exc,
                    provider_succeeded=provider_succeeded,
                )
                raise
            return renamed

    async def compact(
        self,
        conversation_id: str,
        *,
        interaction_id: str | None = None,
    ) -> None:
        async with self._locks.hold(conversation_id):
            conversation, project, config = await self._context(
                conversation_id, reject_active_schedules=False
            )
            revision = await asyncio.to_thread(
                self._repository.get_active_revision, conversation_id
            )
            if revision is None:
                raise InvariantError("Conversation has no active revision to compact")
            runtime, _lease = await self._runtimes.ensure(project)
            await _require_capabilities(runtime, "thread.compact")
            identity = await runtime.resume_thread(
                thread_id=revision.provider_thread_id,
                cwd=project.root_path,
                config=config,
            )
            await self._validate_identity(conversation_id, revision, identity)
            if interaction_id is None:
                await asyncio.to_thread(
                    self._repository.set_provider_barrier,
                    conversation_id,
                    "compact",
                )
            else:
                await asyncio.to_thread(
                    self._repository.begin_provider_barrier_effect,
                    conversation_id=conversation_id,
                    interaction_id=interaction_id,
                    kind="compact",
                    effect_kind="session_compact",
                    effect_correlation_id=revision.provider_thread_id,
                )
            try:
                result = await runtime.compact_thread(revision.provider_thread_id)
            except (ProviderOutcomeUnknown, asyncio.CancelledError) as provider_error:
                await asyncio.shield(
                    self._preserve_compact_unknown_outcome(
                        conversation,
                        provider_error,
                    )
                )
                raise
            except BaseException as provider_error:
                await self._finish_provider_effect_failure(
                    conversation,
                    operation="session_compact",
                    error=provider_error,
                    provider_succeeded=False,
                )
                raise
            if not result.accepted:
                await asyncio.to_thread(
                    self._repository.resolve_provider_barrier_effect,
                    conversation_id,
                    state="rejected",
                    code="provider_rejected",
                    message="Codex did not accept the compaction request.",
                )
                raise InvariantError("Codex did not accept the compaction request")
            await asyncio.to_thread(
                self._repository.resolve_provider_barrier_effect,
                conversation_id,
                state="succeeded",
                code="ok",
                message="Compaction request accepted.",
                clear_barrier=False,
            )
            self.monitor_provider_barrier(conversation_id)

    async def _preserve_compact_unknown_outcome(
        self,
        conversation: ConversationRecord,
        error: BaseException,
    ) -> None:
        code = getattr(error, "code", "command_cancelled")
        await asyncio.to_thread(
            self._repository.mark_provider_barrier_outcome_unknown,
            conversation.id,
            code="provider_effect_outcome_unknown",
            message=(
                "Compaction may have started; codexD will observe this Thread "
                "until it is idle and will not replay the request."
            ),
        )
        await asyncio.to_thread(
            self._repository.record_incident,
            severity="warning",
            code="compact_effect_outcome_unknown",
            summary="Compaction outcome is unknown; read-only reconciliation is active",
            project_id=conversation.project_id,
            conversation_id=conversation.id,
            details={"cause_code": code},
        )
        self.monitor_provider_barrier(conversation.id)

    def monitor_provider_barrier(self, conversation_id: str) -> None:
        if self._closed:
            return
        task = self._barrier_tasks.get(conversation_id)
        if task is not None and not task.done():
            return
        task = asyncio.create_task(
            self._run_provider_barrier_monitor(conversation_id),
            name=f"codexd-provider-barrier-{conversation_id}",
        )
        self._barrier_tasks[conversation_id] = task

        def discard(completed: asyncio.Task[None]) -> None:
            if self._barrier_tasks.get(conversation_id) is completed:
                self._barrier_tasks.pop(conversation_id, None)

        task.add_done_callback(discard)

    async def _run_provider_barrier_monitor(self, conversation_id: str) -> None:
        try:
            await self._monitor_provider_barrier(conversation_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception(
                "Provider barrier monitor stopped unexpectedly",
                extra={"conversation_id": conversation_id},
            )
            try:
                conversation = await asyncio.to_thread(
                    self._repository.get_conversation,
                    conversation_id,
                )
                await asyncio.to_thread(
                    self._repository.record_incident,
                    severity="error",
                    code="provider_barrier_monitor_failed",
                    summary="Provider barrier recovery stopped unexpectedly",
                    project_id=conversation.project_id,
                    conversation_id=conversation_id,
                    details={"exception_type": type(exc).__name__},
                )
                await asyncio.to_thread(
                    self._repository.block_conversation,
                    conversation_id,
                    reason="provider_barrier_monitor_failed",
                )
            except Exception:
                logger.exception(
                    "Could not persist provider barrier monitor failure",
                    extra={"conversation_id": conversation_id},
                )

    async def _monitor_provider_barrier(self, conversation_id: str) -> None:
        while not self._closed:
            conversation = await asyncio.to_thread(
                self._repository.get_conversation,
                conversation_id,
            )
            if conversation.provider_barrier_kind is None:
                return
            if conversation.provider_barrier_kind == "unknown_effect":
                await self._block_barrier(
                    conversation,
                    code="provider_effect_outcome_unknown",
                )
                return
            revision = await asyncio.to_thread(
                self._repository.get_active_revision,
                conversation_id,
            )
            if revision is None:
                await self._block_barrier(
                    conversation,
                    code="provider_barrier_revision_missing",
                )
                return
            project = await asyncio.to_thread(
                self._repository.get_project,
                conversation.project_id,
            )
            config = _decode_thread_config(revision.thread_config_json)
            try:
                runtime, _lease = await self._runtimes.ensure(project)
                try:
                    snapshot = await runtime.read_thread(
                        revision.provider_thread_id
                    )
                except NotFoundError:
                    identity = await runtime.resume_thread(
                        thread_id=revision.provider_thread_id,
                        cwd=project.root_path,
                        config=config,
                    )
                    await self._validate_identity(
                        conversation_id,
                        revision,
                        identity,
                    )
                    snapshot = await runtime.read_thread(identity.thread_id)
            except asyncio.CancelledError:
                raise
            except AdapterError as exc:
                await asyncio.to_thread(
                    self._repository.record_incident,
                    severity="warning" if exc.failure.retryable else "error",
                    code="provider_barrier_observation_failed",
                    summary="Could not observe the provider Thread barrier state",
                    project_id=project.id,
                    conversation_id=conversation_id,
                    details={"adapter_code": exc.failure.code},
                )
                if exc.failure.retryable:
                    await asyncio.sleep(2)
                    continue
                await asyncio.to_thread(
                    self._repository.block_conversation,
                    conversation_id,
                    reason="provider_barrier_observation_failed",
                )
                return
            if snapshot.state is ThreadProviderState.IDLE:
                await asyncio.to_thread(
                    self._repository.resolve_idle_provider_barrier,
                    conversation_id,
                )
                return
            if snapshot.state is ThreadProviderState.ACTIVE:
                await asyncio.sleep(2)
                continue
            if snapshot.state is ThreadProviderState.NOT_LOADED:
                try:
                    identity = await runtime.resume_thread(
                        thread_id=revision.provider_thread_id,
                        cwd=project.root_path,
                        config=config,
                    )
                    await self._validate_identity(
                        conversation_id,
                        revision,
                        identity,
                    )
                    snapshot = await runtime.read_thread(identity.thread_id)
                except asyncio.CancelledError:
                    raise
                except AdapterError as exc:
                    await asyncio.to_thread(
                        self._repository.record_incident,
                        severity="warning" if exc.failure.retryable else "error",
                        code="provider_barrier_resume_failed",
                        summary="Could not reload the provider Thread for barrier recovery",
                        project_id=project.id,
                        conversation_id=conversation_id,
                        details={"adapter_code": exc.failure.code},
                    )
                    if not exc.failure.retryable:
                        await asyncio.to_thread(
                            self._repository.block_conversation,
                            conversation_id,
                            reason="provider_barrier_resume_failed",
                        )
                        return
                await asyncio.sleep(1)
                continue
            await self._block_barrier(
                conversation,
                code=f"provider_barrier_{snapshot.state.value}",
            )
            return

    async def _block_barrier(
        self,
        conversation: ConversationRecord,
        *,
        code: str,
    ) -> None:
        await asyncio.to_thread(
            self._repository.record_incident,
            severity="error",
            code=code,
            summary="Provider barrier could not be reconciled safely",
            project_id=conversation.project_id,
            conversation_id=conversation.id,
        )
        await asyncio.to_thread(
            self._repository.block_conversation,
            conversation.id,
            reason=code,
        )

    async def model_catalog(self, conversation_id: str) -> ModelCatalogSnapshot:
        conversation = await asyncio.to_thread(
            self._repository.get_conversation, conversation_id
        )
        project = await asyncio.to_thread(
            self._repository.get_project, conversation.project_id
        )
        runtime, _lease = await self._runtimes.ensure(project)
        return await runtime.list_models()

    async def set_model(
        self,
        conversation_id: str,
        model: str | None,
        *,
        interaction_id: str | None = None,
    ) -> ConversationRecord:
        async with self._locks.hold(conversation_id):
            await asyncio.to_thread(
                self._repository.assert_conversation_mutable, conversation_id
            )
            if model is not None:
                catalog = await self.model_catalog(conversation_id)
                if not catalog.complete:
                    raise InvariantError(
                        "Codex model catalog is incomplete; model changes are disabled"
                    )
                descriptor = _find_model(catalog, model)
                conversation = await asyncio.to_thread(
                    self._repository.get_conversation, conversation_id
                )
                if (
                    conversation.reasoning_effort_override is not None
                    and conversation.reasoning_effort_override
                    not in descriptor.supported_reasoning_efforts
                ):
                    raise InvariantError(
                        "clear or change the current reasoning effort before "
                        f"selecting {descriptor.model}"
                    )
                if (
                    conversation.service_tier_override is not None
                    and conversation.service_tier_override
                    not in {tier.id for tier in descriptor.service_tiers}
                ):
                    raise InvariantError(
                        "clear or change the current service tier before "
                        f"selecting {descriptor.model}"
                    )
                if (
                    conversation.personality_override is not None
                    and not descriptor.supports_personality
                ):
                    raise InvariantError(
                        "clear the current personality before selecting "
                        f"{descriptor.model}"
                    )
                model = descriptor.model
            return await asyncio.to_thread(
                self._repository.update_conversation_preferences,
                conversation_id,
                model_override=model,
                command_interaction_id=interaction_id,
            )

    async def set_reasoning_effort(
        self,
        conversation_id: str,
        effort: str | None,
        *,
        interaction_id: str | None = None,
    ) -> ConversationRecord:
        async with self._locks.hold(conversation_id):
            await asyncio.to_thread(
                self._repository.assert_conversation_mutable, conversation_id
            )
            if effort is not None:
                conversation = await asyncio.to_thread(
                    self._repository.get_conversation, conversation_id
                )
                catalog = await self.model_catalog(conversation_id)
                descriptor = _effective_model(catalog, conversation.model_override)
                if effort not in descriptor.supported_reasoning_efforts:
                    supported = ", ".join(descriptor.supported_reasoning_efforts)
                    raise InvariantError(
                        f"reasoning effort {effort} is not supported by "
                        f"{descriptor.model}; choose: {supported}"
                    )
            return await asyncio.to_thread(
                self._repository.update_conversation_preferences,
                conversation_id,
                reasoning_effort_override=effort,
                command_interaction_id=interaction_id,
            )

    async def set_reasoning_summary(
        self,
        conversation_id: str,
        summary: str | None,
        *,
        interaction_id: str | None = None,
    ) -> ConversationRecord:
        if summary is not None and summary not in {"auto", "concise", "detailed", "none"}:
            raise InvariantError(
                "reasoning summary must be auto, concise, detailed, none, or default"
            )
        async with self._locks.hold(conversation_id):
            _conversation, project, _config = await self._context(
                conversation_id, reject_active_schedules=False
            )
            runtime, _lease = await self._runtimes.ensure(project)
            await _require_capabilities(runtime, "turn.reasoning_summary")
            return await asyncio.to_thread(
                self._repository.update_conversation_preferences,
                conversation_id,
                reasoning_summary_override=summary,
                command_interaction_id=interaction_id,
            )

    async def set_personality(
        self,
        conversation_id: str,
        personality: str | None,
        *,
        interaction_id: str | None = None,
    ) -> ConversationRecord:
        if personality is not None and personality not in {
            "none",
            "friendly",
            "pragmatic",
        }:
            raise InvariantError(
                "personality must be none, friendly, pragmatic, or default"
            )
        async with self._locks.hold(conversation_id):
            conversation, project, _config = await self._context(
                conversation_id, reject_active_schedules=False
            )
            runtime, _lease = await self._runtimes.ensure(project)
            await _require_capabilities(runtime, "turn.personality")
            catalog = await runtime.list_models()
            descriptor = _effective_model(catalog, conversation.model_override)
            if personality is not None and not descriptor.supports_personality:
                raise InvariantError(
                    f"model {descriptor.model} does not support personality"
                )
            return await asyncio.to_thread(
                self._repository.update_conversation_preferences,
                conversation_id,
                personality_override=personality,
                command_interaction_id=interaction_id,
            )

    async def set_service_tier(
        self,
        conversation_id: str,
        service_tier: str | None,
        *,
        interaction_id: str | None = None,
    ) -> ConversationRecord:
        async with self._locks.hold(conversation_id):
            conversation, project, _config = await self._context(
                conversation_id, reject_active_schedules=False
            )
            runtime, _lease = await self._runtimes.ensure(project)
            await _require_capabilities(runtime, "turn.service_tier")
            if service_tier is not None:
                catalog = await runtime.list_models()
                if not catalog.complete:
                    raise InvariantError(
                        "Codex model catalog is incomplete; service-tier changes are disabled"
                    )
                descriptor = _effective_model(catalog, conversation.model_override)
                tiers = {tier.id for tier in descriptor.service_tiers}
                if service_tier not in tiers:
                    supported = ", ".join(sorted(tiers)) or "none"
                    raise InvariantError(
                        f"service tier {service_tier} is not supported by "
                        f"{descriptor.model}; choose: {supported}"
                    )
            return await asyncio.to_thread(
                self._repository.update_conversation_preferences,
                conversation_id,
                service_tier_override=service_tier,
                command_interaction_id=interaction_id,
            )

    async def set_web_search(
        self,
        conversation_id: str,
        mode: str,
        *,
        interaction_id: str | None = None,
    ) -> ConversationRecord:
        web_search = WebSearchMode.DISABLED if mode == "off" else WebSearchMode(mode)
        if web_search is WebSearchMode.PROVIDER_DEFAULT_UNCONTROLLED:
            raise InvariantError("uncontrolled web search cannot be selected explicitly")
        async with self._locks.hold(conversation_id):
            conversation, project, current_config = await self._context(
                conversation_id, reject_active_schedules=False
            )
            runtime, _lease = await self._runtimes.ensure(project)
            await _require_capabilities(runtime, "web_search.config")
            config = replace(current_config, web_search_mode=web_search)
            revision = await asyncio.to_thread(
                self._repository.get_active_revision, conversation_id
            )
            if revision is None:
                return await asyncio.to_thread(
                    self._repository.update_conversation_preferences,
                    conversation_id,
                    web_search_mode=web_search.value,
                    command_interaction_id=interaction_id,
                )
            await self._begin_provider_effect(
                conversation.id,
                interaction_id=interaction_id,
                effect_kind="websearch_set",
                effect_correlation_id=revision.id,
            )
            provider_succeeded = False
            try:
                identity = await runtime.resume_thread(
                    thread_id=revision.provider_thread_id,
                    cwd=project.root_path,
                    config=config,
                )
                provider_succeeded = True
                await self._validate_identity(conversation_id, revision, identity)
                await self._activate(
                    conversation=conversation,
                    identity=identity,
                    config=config,
                    operation="web_search_update",
                    update_web_search_only=True,
                    complete_provider_effect=True,
                )
                updated = await asyncio.to_thread(
                    self._repository.get_conversation, conversation_id
                )
            except BaseException as exc:
                await self._finish_provider_effect_failure(
                    conversation,
                    operation="web_search_update",
                    error=exc,
                    provider_succeeded=provider_succeeded,
                )
                raise
            return updated

    async def _context(
        self,
        conversation_id: str,
        *,
        reject_active_schedules: bool,
    ) -> tuple[ConversationRecord, ProjectRecord, ThreadConfig]:
        await asyncio.to_thread(
            self._repository.assert_conversation_mutable,
            conversation_id,
            reject_active_schedules=reject_active_schedules,
        )
        conversation = await asyncio.to_thread(
            self._repository.get_conversation, conversation_id
        )
        if conversation.state is ConversationState.BLOCKED:
            raise InvariantError("Conversation is blocked pending operator recovery")
        project = await asyncio.to_thread(
            self._repository.get_project, conversation.project_id
        )
        config = await asyncio.to_thread(
            self._repository.effective_thread_config, conversation_id
        )
        return conversation, project, config

    async def _begin_provider_effect(
        self,
        conversation_id: str,
        *,
        interaction_id: str | None,
        effect_kind: str,
        effect_correlation_id: str | None,
    ) -> None:
        if interaction_id is None:
            await asyncio.to_thread(
                self._repository.set_provider_barrier,
                conversation_id,
                "unknown_effect",
            )
            return
        await asyncio.to_thread(
            self._repository.begin_provider_barrier_effect,
            conversation_id=conversation_id,
            interaction_id=interaction_id,
            kind="unknown_effect",
            effect_kind=effect_kind,
            effect_correlation_id=effect_correlation_id,
        )

    async def _finish_provider_effect_failure(
        self,
        conversation: ConversationRecord,
        *,
        operation: str,
        error: BaseException,
        provider_succeeded: bool,
    ) -> None:
        outcome_unknown = (
            provider_succeeded
            or isinstance(error, (ProviderOutcomeUnknown, asyncio.CancelledError))
        )
        if not outcome_unknown:
            deterministic_error = isinstance(error, (InvariantError, AdapterError))
            code = getattr(error, "code", "provider_rejected")
            message = (
                str(error)[:512]
                if deterministic_error
                else type(error).__name__
            )
            try:
                await asyncio.to_thread(
                    self._repository.resolve_provider_barrier_effect,
                    conversation.id,
                    state="rejected" if deterministic_error else "failed",
                    code=code,
                    message=message,
                )
            except Exception as cleanup_error:
                error.add_note(
                    "provider barrier deterministic completion failed: "
                    f"{type(cleanup_error).__name__}"
                )
            return
        failures: list[Exception] = []
        try:
            await asyncio.to_thread(
                self._repository.mark_provider_barrier_outcome_unknown,
                conversation.id,
                code="provider_effect_outcome_unknown",
                message=(
                    "The provider mutation may have completed, but codexD could "
                    "not commit a deterministic local outcome."
                ),
                block_conversation=True,
            )
        except Exception as exc:
            failures.append(exc)
        try:
            await asyncio.to_thread(
                self._repository.record_incident,
                severity="critical",
                code="provider_effect_outcome_unknown",
                summary="Provider mutation outcome could not be committed safely",
                project_id=conversation.project_id,
                conversation_id=conversation.id,
                details={"operation": operation},
            )
        except Exception as exc:
            failures.append(exc)
        for failure in failures:
            error.add_note(
                "provider outcome fencing also failed: "
                f"{type(failure).__name__}"
            )

    async def _activate(
        self,
        *,
        conversation: ConversationRecord,
        identity: ThreadIdentity,
        config: ThreadConfig,
        operation: str,
        parent_revision_id: str | None = None,
        restore_conversation_config: bool = False,
        update_web_search_only: bool = False,
        complete_provider_effect: bool = False,
    ) -> ThreadRevisionRecord:
        try:
            return await asyncio.to_thread(
                self._repository.activate_thread_revision,
                conversation_id=conversation.id,
                identity=identity,
                config=config,
                parent_revision_id=parent_revision_id,
                restore_conversation_config=restore_conversation_config,
                update_web_search_only=update_web_search_only,
                complete_provider_effect=complete_provider_effect,
            )
        except Exception as commit_error:
            try:
                await self._record_commit_failure(
                    conversation,
                    f"{operation}_commit_failed",
                    details={
                        "provider_thread_hash": sha256_text(
                            identity.thread_id
                        )[:16]
                    },
                )
            except Exception as incident_error:
                raise ExceptionGroup(
                    "session commit and incident persistence failed",
                    (commit_error, incident_error),
                ) from commit_error
            raise

    async def _record_commit_failure(
        self,
        conversation: ConversationRecord,
        code: str,
        *,
        details: Mapping[str, str | None],
    ) -> None:
        await asyncio.to_thread(
            self._repository.record_incident,
            severity="critical",
            code=code,
            summary="Provider session mutation succeeded but local commit failed",
            project_id=conversation.project_id,
            conversation_id=conversation.id,
            details=details,
        )
        await asyncio.to_thread(
            self._repository.block_conversation,
            conversation.id,
            reason=code,
        )

    async def _validate_identity(
        self,
        conversation_id: str,
        revision: ThreadRevisionRecord,
        identity: ThreadIdentity,
    ) -> None:
        if (
            identity.thread_id == revision.provider_thread_id
            and identity.provider_session_id == revision.provider_session_id
            and identity.requested_thread_id == revision.provider_thread_id
        ):
            return
        conversation = await asyncio.to_thread(
            self._repository.get_conversation,
            conversation_id,
        )
        await asyncio.to_thread(
            self._repository.record_incident,
            severity="critical",
            code="provider_thread_identity_mismatch",
            summary="Provider returned an unexpected Thread identity",
            project_id=conversation.project_id,
            conversation_id=conversation_id,
            details={
                "expected_thread_hash": sha256_text(
                    revision.provider_thread_id
                )[:16],
                "actual_thread_hash": sha256_text(identity.thread_id)[:16],
                "expected_session_hash": sha256_text(
                    revision.provider_session_id
                )[:16],
                "actual_session_hash": sha256_text(
                    identity.provider_session_id
                )[:16],
            },
        )
        await asyncio.to_thread(
            self._repository.block_conversation,
            conversation_id,
            reason="provider_thread_identity_mismatch",
        )
        raise InvariantError("provider resumed a different thread identity")


def _decode_thread_config(raw_json: str) -> ThreadConfig:
    raw = json.loads(raw_json)
    if not isinstance(raw, Mapping):
        raise InvariantError("stored thread config is not an object")

    def optional_string(key: str) -> str | None:
        value = raw.get(key)
        if value is None:
            return None
        if not isinstance(value, str):
            raise InvariantError(f"stored thread config {key} is invalid")
        return value

    try:
        approval = ApprovalPolicy(str(raw["approval_mode"]))
        sandbox = SandboxProfile(str(raw["sandbox"]))
        web_search = WebSearchMode(str(raw["web_search_mode"]))
    except (KeyError, ValueError) as exc:
        raise InvariantError("stored thread config is invalid") from exc
    return ThreadConfig(
        model=optional_string("model"),
        personality=optional_string("personality"),
        sandbox=sandbox,
        approval_mode=approval,
        service_tier=optional_string("service_tier"),
        web_search_mode=web_search,
    )


def _find_model(
    catalog: ModelCatalogSnapshot, requested: str
) -> ModelDescriptor:
    matches = tuple(
        model
        for model in catalog.models
        if requested in {model.id, model.model}
    )
    if not matches:
        completeness = "complete" if catalog.complete else "incomplete"
        raise InvariantError(
            f"model is not present in the {completeness} Codex catalog: {requested}"
        )
    if len(matches) > 1 and len({model.model for model in matches}) > 1:
        raise InvariantError(f"model identifier is ambiguous: {requested}")
    return matches[0]


def _effective_model(
    catalog: ModelCatalogSnapshot, override: str | None
) -> ModelDescriptor:
    if override is not None:
        return _find_model(catalog, override)
    for model in catalog.models:
        if model.is_default:
            return model
    raise InvariantError("Codex model catalog does not identify a default model")


async def _require_capabilities(runtime: CodexRuntime, *names: str) -> None:
    manifest = await runtime.capabilities()
    unavailable = tuple(name for name in names if manifest.optional.get(name) is not True)
    if unavailable:
        raise InvariantError(
            f"optional Codex capabilities are unavailable: {', '.join(unavailable)}"
        )


def _normalize_thread_name(name: str) -> str:
    normalized = name.strip()
    if not normalized:
        raise InvariantError("Thread name may not be empty")
    if len(normalized) > 100:
        raise InvariantError("Thread name may not exceed 100 characters")
    if not normalized.isprintable():
        raise InvariantError("Thread name contains control characters")
    if any(token in normalized for token in ("@everyone", "@here", "<@", "<#")):
        raise InvariantError("Thread name may not contain Discord mentions")
    return normalized
