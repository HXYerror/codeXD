from __future__ import annotations

import asyncio
import unicodedata
from dataclasses import dataclass

from codexd.domain.conversations import (
    SandboxProfile,
    ThreadConfig,
    ThreadIdentity,
    TurnConfig,
    WebSearchMode,
)
from codexd.domain.ids import sha256_text
from codexd.errors import CodexDError, ConflictError
from codexd.runtime.errors import RuntimeUnavailable
from codexd.runtime.port import CodexRuntime, SideQueryIdentity
from codexd.runtime.supervisor import RuntimeSupervisor
from codexd.storage.records import (
    ConversationRecord,
    ProjectRecord,
    RuntimeLeaseRecord,
    SideQueryRecord,
)
from codexd.storage.repository import Repository
from codexd.storage.side_queries import SideQueryRepository

_MAX_QUESTION_CHARS = 4_000
_MAX_QUESTION_BYTES = 16 * 1024
_MAX_ANSWER_BYTES = 256 * 1024
_VISIBLE_PHASES = frozenset({None, "commentary", "final_answer"})


class SideQueryError(CodexDError):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


@dataclass(frozen=True)
class _ActiveSideQuery:
    record_id: str
    runtime: CodexRuntime
    identity: SideQueryIdentity
    task: asyncio.Task[object]


class SideQueryCoordinator:
    def __init__(
        self,
        *,
        repository: Repository,
        side_repository: SideQueryRepository,
        runtimes: RuntimeSupervisor,
        boot_id: str,
        timeout_seconds: float = 600.0,
        max_concurrency: int = 4,
    ) -> None:
        if timeout_seconds <= 0 or max_concurrency < 1:
            raise ValueError("Side Query limits must be positive")
        self._repository = repository
        self._side_repository = side_repository
        self._runtimes = runtimes
        self._boot_id = boot_id
        self._timeout_seconds = timeout_seconds
        self._max_concurrency = max_concurrency
        self._active: dict[tuple[str, int], _ActiveSideQuery | None] = {}
        self._tasks: dict[tuple[str, int], asyncio.Task[object]] = {}
        self._lock = asyncio.Lock()
        self._closing = False

    async def ask(
        self,
        *,
        interaction_id: str,
        conversation_id: str,
        requested_by_user_id: int,
        question: str,
    ) -> str:
        normalized = _question(question)
        question_size = len(normalized.encode("utf-8"))
        try:
            record, created = await asyncio.to_thread(
                self._side_repository.accept,
                interaction_id=interaction_id,
                conversation_id=conversation_id,
                requested_by_user_id=requested_by_user_id,
                question_hash=sha256_text(normalized),
                question_size=question_size,
                boot_id=self._boot_id,
            )
        except ConflictError as exc:
            if "btw_already_running" in str(exc):
                raise SideQueryError(
                    "btw_already_running",
                    "This user already has an active Side Query.",
                ) from exc
            raise
        if not created or record.state != "accepted":
            raise SideQueryError(
                "btw_already_processed",
                f"This Side Query is already {record.state}; submit a new command.",
            )
        key = (conversation_id, requested_by_user_id)
        current_task = asyncio.current_task()
        assert current_task is not None
        async with self._lock:
            if self._closing:
                await self._finish_failed(record.id, "daemon_stopping")
                raise SideQueryError("daemon_stopping", "codexD is shutting down.")
            if key in self._active:
                await self._finish_failed(record.id, "btw_already_running")
                raise SideQueryError(
                    "btw_already_running",
                    "This user already has an active Side Query.",
                )
            if len(self._active) >= self._max_concurrency:
                await self._finish_failed(record.id, "btw_capacity")
                raise SideQueryError(
                    "btw_capacity",
                    "The temporary Side Query capacity is currently full.",
                )
            self._active[key] = None
            self._tasks[key] = current_task

        started: _ActiveSideQuery | None = None
        terminal_recorded = False
        project: ProjectRecord | None = None
        lease: RuntimeLeaseRecord | None = None
        try:
            conversation = await asyncio.to_thread(
                self._repository.get_conversation,
                conversation_id,
            )
            if conversation.state.value != "active":
                raise SideQueryError(
                    "btw_conversation_unavailable",
                    f"This Conversation is {conversation.state.value}.",
                )
            if conversation.provider_barrier_kind is not None:
                raise SideQueryError(
                    "btw_provider_barrier",
                    "The provider Thread is not stable enough for a Side Query.",
                )
            revision = await asyncio.to_thread(
                self._repository.get_active_revision,
                conversation_id,
            )
            if revision is None:
                raise SideQueryError(
                    "btw_session_uninitialized",
                    "Send a normal message first to start the Codex Session.",
                )
            project = await asyncio.to_thread(
                self._repository.get_project,
                conversation.project_id,
            )
            runtime, lease = await self._runtimes.ensure(project)
            current_revision = await asyncio.to_thread(
                self._repository.get_active_revision,
                conversation_id,
            )
            if (
                current_revision is None
                or current_revision.id != revision.id
                or current_revision.provider_thread_id != revision.provider_thread_id
                or current_revision.provider_session_id != revision.provider_session_id
            ):
                raise SideQueryError(
                    "btw_thread_changed",
                    "The active Codex Thread changed before the Side Query started.",
                )
            thread_config, turn_config = _effective_configs(
                conversation,
                project,
            )
            provider = await runtime.start_side_query(
                local_query_id=record.id,
                source_thread=ThreadIdentity(
                    thread_id=revision.provider_thread_id,
                    requested_thread_id=revision.provider_thread_id,
                    provider_session_id=revision.provider_session_id,
                    forked_from_thread_id=revision.provider_forked_from_thread_id,
                    parent_thread_id=revision.provider_parent_thread_id,
                    provider_version=revision.provider_version,
                    dynamic_tools_enabled=revision.dynamic_tools_enabled,
                ),
                question=normalized,
                cwd=project.root_path,
                thread_config=thread_config,
                turn_config=turn_config,
            )
            if provider.identity.runtime_generation != lease.generation:
                await runtime.close_side_query(provider.identity)
                raise SideQueryError(
                    "btw_runtime_changed",
                    "The Codex runtime changed while starting the Side Query.",
                )
            started = _ActiveSideQuery(
                record.id,
                runtime,
                provider.identity,
                current_task,
            )
            async with self._lock:
                self._active[key] = started
            await asyncio.to_thread(self._side_repository.mark_running, record.id)
            try:
                async with asyncio.timeout(self._timeout_seconds):
                    answer = await _collect_answer(provider.stream)
            except TimeoutError as exc:
                await _interrupt_safely(started)
                await self._close_safely(started)
                started = None
                await self._finish_failed(record.id, "btw_timeout")
                terminal_recorded = True
                raise SideQueryError(
                    "btw_timeout",
                    "The temporary Side Query exceeded its time limit.",
                ) from exc
            await self._close_safely(started, raise_error=True)
            started = None
            answer_size = len(answer.encode("utf-8"))
            await asyncio.to_thread(
                self._side_repository.finish,
                record.id,
                state="completed",
                terminal_code="provider_completed",
                answer_hash=sha256_text(answer),
                answer_size=answer_size,
            )
            terminal_recorded = True
            return answer
        except asyncio.CancelledError:
            if started is not None:
                await asyncio.shield(_interrupt_safely(started))
                await asyncio.shield(self._close_safely(started))
            if not terminal_recorded:
                interrupt_code = (
                    "daemon_shutdown" if self._closing else "btw_cancelled"
                )
                await asyncio.shield(
                    self._finish_interrupted(record.id, interrupt_code)
                )
            raise
        except RuntimeUnavailable as exc:
            if started is not None:
                await self._close_safely(started)
            if project is not None and lease is not None:
                await self._runtimes.report_failure(
                    project,
                    expected_lease_id=lease.id,
                    expected_generation=lease.generation,
                    failure_code=exc.failure.code,
                )
            if not terminal_recorded:
                await self._finish_failed(record.id, exc.failure.code)
            raise SideQueryError(
                exc.failure.code,
                "The Codex runtime failed during the Side Query.",
            ) from exc
        except SideQueryError as exc:
            if started is not None:
                await _interrupt_safely(started)
                await self._close_safely(started)
            if not terminal_recorded:
                await self._finish_failed(record.id, exc.code)
            raise
        except Exception as exc:
            if started is not None:
                await _interrupt_safely(started)
                await self._close_safely(started)
            if not terminal_recorded:
                await self._finish_failed(
                    record.id,
                    getattr(exc, "code", "btw_failed"),
                )
            raise SideQueryError(
                getattr(exc, "code", "btw_failed"),
                "The temporary Side Query failed; the main task was unchanged.",
            ) from exc
        finally:
            async with self._lock:
                self._active.pop(key, None)
                self._tasks.pop(key, None)

    async def close(self) -> None:
        async with self._lock:
            self._closing = True
            active = tuple(item for item in self._active.values() if item is not None)
            tasks = tuple(self._tasks.values())
        for item in active:
            await _interrupt_safely(item)
        for task in tasks:
            task.cancel()
        await asyncio.gather(
            *tasks,
            return_exceptions=True,
        )
        for item in active:
            await self._close_safely(item)
            record = await asyncio.to_thread(self._side_repository.get, item.record_id)
            if record.state in {"accepted", "running"}:
                await self._finish_interrupted(item.record_id, "daemon_shutdown")

    async def _close_safely(
        self,
        active: _ActiveSideQuery,
        *,
        raise_error: bool = False,
    ) -> None:
        try:
            await active.runtime.close_side_query(active.identity)
        except Exception as exc:
            if raise_error:
                raise SideQueryError(
                    "btw_cleanup_failed",
                    "The Side Query answer could not be safely detached.",
                ) from exc

    async def _finish_failed(self, query_id: str, code: str) -> SideQueryRecord:
        return await asyncio.to_thread(
            self._side_repository.finish,
            query_id,
            state="failed",
            terminal_code=code,
            error_code=code,
        )

    async def _finish_interrupted(self, query_id: str, code: str) -> SideQueryRecord:
        return await asyncio.to_thread(
            self._side_repository.finish,
            query_id,
            state="interrupted",
            terminal_code=code,
            error_code=code,
        )


async def _interrupt_safely(active: _ActiveSideQuery) -> None:
    try:
        await active.runtime.interrupt_side_query(active.identity)
    except Exception:
        return


async def _collect_answer(stream: object) -> str:
    completed: list[tuple[str | None, str]] = []
    deltas: list[str] = []
    terminal_kind: str | None = None
    async for event in stream:  # type: ignore[attr-defined]
        if event.kind == "assistant.text.delta":
            text = event.payload.get("text")
            if isinstance(text, str):
                deltas.append(text)
        elif event.kind == "assistant.text.completed":
            text = event.payload.get("text")
            phase = event.payload.get("phase")
            if (
                isinstance(text, str)
                and text.strip()
                and phase in _VISIBLE_PHASES
            ):
                completed.append((phase if isinstance(phase, str) else None, text))
        elif event.kind in {
            "turn.completed",
            "turn.failed",
            "turn.interrupted",
            "turn.terminal_unparseable",
        }:
            terminal_kind = event.kind
    if terminal_kind != "turn.completed":
        raise SideQueryError(
            "btw_provider_failed",
            "Codex did not complete the temporary Side Query.",
        )
    final = [text for phase, text in completed if phase == "final_answer"]
    compatible = [text for phase, text in completed if phase is None]
    commentary = [text for phase, text in completed if phase == "commentary"]
    answer = (
        final[-1]
        if final
        else compatible[-1]
        if compatible
        else "".join(deltas).strip()
    )
    if commentary and answer:
        answer = "\n\n".join((*commentary, answer))
    answer = answer.strip()
    if not answer:
        raise SideQueryError("btw_empty_answer", "Codex returned no visible Side answer.")
    if len(answer.encode("utf-8")) > _MAX_ANSWER_BYTES:
        raise SideQueryError(
            "btw_answer_too_large",
            "The temporary answer exceeded the ephemeral response limit.",
        )
    return answer


def _question(value: str) -> str:
    question = value.strip()
    if not question:
        raise SideQueryError("btw_empty_question", "A Side Query question is required.")
    if any(unicodedata.category(character) == "Cc" for character in question):
        raise SideQueryError(
            "btw_control_character",
            "Side Query questions may not contain control characters.",
        )
    if len(question) > _MAX_QUESTION_CHARS or len(question.encode("utf-8")) > _MAX_QUESTION_BYTES:
        raise SideQueryError(
            "btw_question_too_large",
            "Side Query questions may not exceed 4,000 characters or 16 KiB.",
        )
    return question


def _effective_configs(
    conversation: ConversationRecord,
    project: ProjectRecord,
) -> tuple[ThreadConfig, TurnConfig]:
    thread = ThreadConfig(
        model=conversation.model_override or project.default_model,
        personality=(
            conversation.personality_override
            or project.default_personality
        ),
        sandbox=SandboxProfile.READ_ONLY,
        service_tier=(
            conversation.service_tier_override
            or project.default_service_tier
        ),
        web_search_mode=WebSearchMode(conversation.web_search_mode),
    )
    turn = TurnConfig(
        cwd=project.root_path,
        sandbox=SandboxProfile.READ_ONLY,
        model=thread.model,
        reasoning_effort=(
            conversation.reasoning_effort_override
            or project.default_reasoning_effort
        ),
        reasoning_summary=(
            conversation.reasoning_summary_override
            or project.default_reasoning_summary
        ),
        personality=thread.personality,
        service_tier=thread.service_tier,
    )
    return thread, turn
