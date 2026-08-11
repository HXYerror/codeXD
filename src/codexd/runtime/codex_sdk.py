from __future__ import annotations

import asyncio
import hashlib
import hmac
import importlib
import importlib.metadata
import inspect
import logging
import os
import re
import stat
import tomllib
from collections.abc import AsyncIterator, Callable
from contextlib import suppress
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import openai_codex
from openai_codex import (
    ApprovalMode,
    AsyncCodex,
    CodexConfig,
    CodexError,
    InputItem,
    InternalRpcError,
    InvalidParamsError,
    InvalidRequestError,
    JsonRpcError,
    LocalImageInput,
    MethodNotFoundError,
    ParseError,
    RetryLimitExceededError,
    Sandbox,
    ServerBusyError,
    SkillInput,
    TextInput,
    TransportClosedError,
    is_retryable_error,
)
from openai_codex.api import AsyncThread, AsyncTurnHandle
from openai_codex.client import CodexClient
from openai_codex.generated.v2_all import (
    DynamicToolSpec,
    ReasoningSummaryPartAddedNotification,
    TurnCompletedNotification,
    TurnStartedNotification,
)
from openai_codex.models import (
    JsonObject,
    Notification,
    UnknownNotification,
)
from openai_codex.types import Personality, ReasoningEffort, ReasoningSummary, TurnStatus

from codexd.domain.capabilities import (
    CapabilityManifest,
    CompatibilityInfo,
    EventCapability,
)
from codexd.domain.conversations import (
    ApprovalPolicy,
    SandboxProfile,
    ThreadConfig,
    ThreadIdentity,
    ThreadProviderState,
    ThreadSnapshot,
    TurnConfig,
    WebSearchMode,
)
from codexd.domain.events import NormalizedEvent
from codexd.domain.ids import canonical_json, sha256_text, utc_now_ms
from codexd.domain.models import (
    AccountStatus,
    ModelCatalogSnapshot,
    ModelDescriptor,
    ServiceTierDescriptor,
)
from codexd.domain.turns import TurnFile, TurnIdentity, TurnInput
from codexd.errors import AttachmentIntegrityError, InvariantError, NotFoundError
from codexd.runtime.app_server import (
    DynamicAsyncCodex,
    DynamicAsyncThread,
    DynamicAsyncTurnHandle,
)
from codexd.runtime.errors import (
    AdapterError,
    AdapterFailure,
    AdapterInvariantError,
    InterruptFailed,
    ProviderOutcomeUnknown,
    ProviderRateLimited,
    ProviderRejected,
    RuntimeUnavailable,
    UnsupportedCapability,
    file_input_unsupported,
)
from codexd.runtime.port import (
    CompactStartResult,
    DynamicToolHandler,
    RuntimeSlotConfig,
    StartedTurn,
    TurnStream,
)
from codexd.security import private_files
from codexd.security.redaction import redact_diff, redact_text, redact_value

SDK_DECLARED_RANGE = ">=0.144.4,<0.145"
_MAX_DELTA = 16 * 1024
_THREAD_COMPACT_START_TIMEOUT_SECONDS = 30.0
_SUBAGENT_DETAIL_TIMEOUT_SECONDS = 2.0
_CANCELLED_STARTUP_CLEANUP_TIMEOUT_SECONDS = 30.0
_FILE_INPUT_RUNTIME_RETIRE_TIMEOUT_SECONDS = 30.0
_APP_SERVER_FORCE_WAIT_TIMEOUT_SECONDS = 2.0
_NEW_THREAD_PERSISTENCE_NAME = "codexD session"
_ARCHIVE_DIRECT_HANDLE_CONTRACT_VERIFIED = False
_MIN_SDK_VERSION = (0, 144, 4)
_MAX_SDK_VERSION = (0, 145, 0)
_MENTION_INPUT_VERIFIED_SDK_VERSIONS = frozenset({"0.144.4"})
_DYNAMIC_TOOL_VERIFIED_VERSION_PAIRS = frozenset({("0.144.4", "0.144.4")})
try:
    _FCNTL: Any | None = importlib.import_module("fcntl")
except ImportError:
    _FCNTL = None
logger = logging.getLogger(__name__)
_PENDING_STARTUP_CLEANUPS: set[asyncio.Task[None]] = set()
_COMPAT_UNKNOWN_NOTIFICATION_METHODS = frozenset(
    {
        "hook/completed",
        "hook/started",
        "item/autoApprovalReview/completed",
        "item/autoApprovalReview/started",
        "item/commandExecution/terminalInteraction",
        "item/fileChange/patchUpdated",
        "item/mcpToolCall/progress",
        "item/reasoning/summaryPartAdded",
        "model/rerouted",
        "model/safetyBuffering/updated",
        "model/verification",
        "thread/compacted",
        "thread/goal/cleared",
        "thread/goal/updated",
        "turn/moderationMetadata",
    }
)


class _FileInputLeaseError(OSError):
    pass


class _FileInputLeases:
    def __init__(self, descriptors: tuple[int, ...] = ()) -> None:
        self._descriptors = descriptors

    @property
    def descriptors(self) -> tuple[int, ...]:
        return self._descriptors

    def close(self) -> None:
        descriptors, self._descriptors = self._descriptors, ()
        for descriptor in descriptors:
            if _FCNTL is not None:
                with suppress(OSError):
                    _FCNTL.flock(descriptor, _FCNTL.LOCK_UN)
            with suppress(OSError):
                os.close(descriptor)


def _schedule_cancelled_startup_cleanup(
    client: AsyncCodex | DynamicAsyncCodex,
    enter_task: asyncio.Task[Any],
) -> None:
    cleanup = asyncio.create_task(
        _finish_cancelled_startup(client, enter_task),
        name="codexd-sdk-cancelled-startup-cleanup",
    )
    _PENDING_STARTUP_CLEANUPS.add(cleanup)
    cleanup.add_done_callback(_PENDING_STARTUP_CLEANUPS.discard)


async def _wait_for_cancelled_startup_cleanups() -> None:
    while pending := tuple(_PENDING_STARTUP_CLEANUPS):
        await asyncio.wait(pending)


async def _finish_cancelled_startup(
    client: AsyncCodex | DynamicAsyncCodex,
    enter_task: asyncio.Task[Any],
) -> None:
    process = _app_server_process(client)
    try:
        async with asyncio.timeout(_CANCELLED_STARTUP_CLEANUP_TIMEOUT_SECONDS):
            await client.close()
            await asyncio.shield(enter_task)
            await client.close()
    except asyncio.CancelledError:
        _force_terminate_cancelled_startup_process(client, process=process)
        await _cancel_startup_entry(enter_task)
        raise
    except TimeoutError:
        logger.error("Cancelled Codex runtime startup cleanup exceeded its deadline")
        _force_terminate_cancelled_startup_process(client, process=process)
        await _cancel_startup_entry(enter_task)
    except Exception:
        logger.exception("Cancelled Codex runtime startup cleanup failed")
        _force_terminate_cancelled_startup_process(client, process=process)
        await _cancel_startup_entry(enter_task)


async def _cancel_startup_entry(enter_task: asyncio.Task[Any]) -> None:
    enter_task.cancel()
    try:
        await enter_task
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception("Cancelled Codex runtime startup task failed during cleanup")


def _app_server_process(client: AsyncCodex | DynamicAsyncCodex) -> Any | None:
    process = getattr(client, "process", None)
    if process is not None:
        return process
    async_client = getattr(client, "_client", None)
    sync_client = getattr(async_client, "_sync", None)
    return getattr(sync_client, "_proc", None)


def _force_terminate_app_server(
    client: AsyncCodex | DynamicAsyncCodex,
    *,
    process: Any | None = None,
) -> bool:
    """Synchronously stop the SDK process captured before ``client.close``.

    The SDK clears its private process field at the beginning of close, so a
    close failure can otherwise make the still-running process unreachable.
    """

    process = process if process is not None else _app_server_process(client)
    if process is None:
        return True
    try:
        process.kill()
    except ProcessLookupError:
        return True
    except Exception:
        logger.exception("Could not force-terminate Codex app-server process")
        return False
    if _confirm_app_server_exit(process):
        return True
    logger.error("Could not confirm Codex app-server process termination")
    return False


def _confirm_app_server_exit(process: Any | None) -> bool:
    if process is None:
        return True
    poll = getattr(process, "poll", None)
    if callable(poll):
        try:
            if poll() is not None:
                return True
        except ProcessLookupError:
            return True
        except Exception:
            return False
    wait = getattr(process, "wait", None)
    if not callable(wait):
        return False
    try:
        wait(timeout=_APP_SERVER_FORCE_WAIT_TIMEOUT_SECONDS)
    except ProcessLookupError:
        return True
    except Exception:
        return False
    return True


def _force_terminate_cancelled_startup_process(
    client: AsyncCodex | DynamicAsyncCodex,
    *,
    process: Any | None = None,
) -> None:
    _force_terminate_app_server(client, process=process)


_UNKNOWN_OUTCOME_MUTATIONS = frozenset(
    {
        "thread.start",
        "thread.resume",
        "thread.fork",
        "thread.archive",
        "thread.unarchive",
        "thread.set_name",
        "thread.compact",
        "turn.steer",
        "turn.interrupt",
    }
)


class CodexSDKRuntime:
    def __init__(
        self,
        *,
        client: AsyncCodex | DynamicAsyncCodex,
        slot: RuntimeSlotConfig,
        generation: int,
        manifest: CapabilityManifest,
    ) -> None:
        self.generation = generation
        self._client = client
        self._slot = slot
        self._manifest = manifest
        self._threads: dict[str, AsyncThread | DynamicAsyncThread] = {}
        self._turn_handles: dict[
            str, AsyncTurnHandle | DynamicAsyncTurnHandle
        ] = {}
        self._file_input_leases: dict[str, _FileInputLeases] = {}
        self._pending_file_input_leases: set[_FileInputLeases] = set()
        self._file_lease_release_blocked = False
        self._file_lease_process: Any | None = None
        self._subagent_details: dict[str, dict[str, str]] = {}
        self._closed = False
        self._close_lock = asyncio.Lock()

    @classmethod
    async def create(
        cls,
        *,
        slot: RuntimeSlotConfig,
        generation: int,
        dynamic_tools: tuple[JsonObject, ...] = (),
        dynamic_tool_handler: DynamicToolHandler | None = None,
    ) -> CodexSDKRuntime:
        await _wait_for_cancelled_startup_cleanups()
        _verify_public_contract()
        dynamic_enabled = dynamic_tool_handler is not None
        if dynamic_enabled and not dynamic_tools:
            raise InvariantError("dynamic tool handler requires one or more tool specs")
        config = _sdk_config(
            slot,
            generation=generation,
            experimental_api=dynamic_enabled,
        )
        client: AsyncCodex | DynamicAsyncCodex
        if dynamic_tool_handler is None:
            client = AsyncCodex(config)
        else:
            client = DynamicAsyncCodex(
                config,
                generation=generation,
                dynamic_tools=dynamic_tools,
                dynamic_tool_handler=dynamic_tool_handler,
            )
        enter_task = asyncio.create_task(
            client.__aenter__(),
            name=f"codexd-sdk-enter-{generation}",
        )
        entered = False
        try:
            await asyncio.shield(enter_task)
            entered = True
            runtime_version = _initialized_runtime_version(client)
            manifest = (
                capability_manifest(
                    runtime_version=runtime_version,
                    schedule_tool_enabled=True,
                    publish_image_enabled=True,
                )
                if dynamic_enabled
                else capability_manifest(runtime_version=runtime_version)
            )
            if (
                dynamic_enabled
                and manifest.optional.get("codexd.schedule_create_tool") is not True
            ):
                raise AdapterInvariantError(
                    AdapterFailure(
                        code="dynamic_tool_contract_unavailable",
                        provider_exception="VersionMismatch",
                        message=(
                            "The installed SDK/runtime pair has no verified dynamic "
                            "tool call contract"
                        ),
                        retryable=False,
                        runtime_generation=generation,
                    )
                )
            manifest.assert_required()
        except asyncio.CancelledError:
            _schedule_cancelled_startup_cleanup(client, enter_task)
            raise
        except CodexError as exc:
            if entered:
                await _close_after_failed_create(client, exc)
            raise _adapter_error(
                exc,
                operation="runtime.initialize",
                generation=generation,
            ) from exc
        except BaseException as exc:
            if entered:
                await _close_after_failed_create(client, exc)
            raise
        return cls(client=client, slot=slot, generation=generation, manifest=manifest)

    async def capabilities(self) -> CapabilityManifest:
        return self._manifest

    async def list_models(self) -> ModelCatalogSnapshot:
        self._ensure_open()
        try:
            response = await self._client.models(include_hidden=False)
        except CodexError as exc:
            raise _adapter_error(
                exc,
                operation="model.catalog",
                generation=self.generation,
            ) from exc
        models: list[ModelDescriptor] = []
        for model in response.data:
            upgrade: dict[str, object] | None = (
                cast(
                    dict[str, object],
                    model.upgrade_info.model_dump(
                        mode="json", by_alias=False, exclude_none=True
                    ),
                )
                if model.upgrade_info
                else ({"model": model.upgrade} if model.upgrade else None)
            )
            models.append(
                ModelDescriptor(
                    id=model.id,
                    model=model.model,
                    is_default=model.is_default,
                    input_modalities=tuple(
                        _enum_value(value) for value in model.input_modalities or []
                    ),
                    supported_reasoning_efforts=tuple(
                        _enum_value(option.reasoning_effort)
                        for option in model.supported_reasoning_efforts
                    ),
                    default_reasoning_effort=_enum_value(model.default_reasoning_effort),
                    supports_personality=bool(model.supports_personality),
                    service_tiers=tuple(
                        ServiceTierDescriptor(tier.id, tier.name, tier.description)
                        for tier in model.service_tiers or []
                    ),
                    default_service_tier=model.default_service_tier,
                    upgrade=upgrade,
                )
            )
        return ModelCatalogSnapshot(
            models=tuple(models),
            complete=response.next_cursor is None,
            next_cursor=response.next_cursor,
        )

    async def account_status(self) -> AccountStatus:
        self._ensure_open()
        try:
            response = await self._client.account(refresh_token=False)
        except CodexError as exc:
            raise _adapter_error(
                exc,
                operation="account.read",
                generation=self.generation,
            ) from exc
        account_type: str | None = None
        plan_type: str | None = None
        if response.account is not None:
            root = response.account.root
            account_type = str(root.type)
            plan = getattr(root, "plan_type", None)
            plan_type = _enum_value(plan) if plan is not None else None
        configured_requires_auth = _configured_provider_requires_openai_auth(
            self._slot
        )
        return AccountStatus(
            auth_required=(
                response.requires_openai_auth
                and configured_requires_auth is not False
            ),
            account_type=account_type,
            plan_type=plan_type,
            observed_at=utc_now_ms(),
        )

    async def start_thread(self, *, cwd: Path, config: ThreadConfig) -> ThreadIdentity:
        self._ensure_open()
        try:
            thread = await self._client.thread_start(
                approval_mode=_approval(config.approval_mode),
                config=_thread_wire_config(config.web_search_mode),
                cwd=str(cwd),
                ephemeral=False,
                model=config.model,
                personality=_personality(config.personality),
                sandbox=_sandbox(config.sandbox),
                service_tier=config.service_tier,
            )
        except CodexError as exc:
            raise _adapter_error(
                exc,
                operation="thread.start",
                generation=self.generation,
            ) from exc
        try:
            await thread.set_name(_NEW_THREAD_PERSISTENCE_NAME)
        except CodexError as exc:
            raise _provider_outcome_unknown(
                exc,
                operation="thread.start",
                generation=self.generation,
                thread_id=thread.id,
                message=(
                    "Codex created a Thread but failed to persist its initial "
                    "runtime handle"
                ),
            ) from exc
        self._threads[thread.id] = thread
        identity = await self._identity_after_mutation(
            thread,
            requested_thread_id=None,
            operation="thread.start",
        )
        return replace(
            identity,
            dynamic_tools_enabled=isinstance(self._client, DynamicAsyncCodex),
        )

    async def resume_thread(
        self, *, thread_id: str, cwd: Path, config: ThreadConfig
    ) -> ThreadIdentity:
        self._ensure_open()
        try:
            thread = await self._client.thread_resume(
                thread_id,
                approval_mode=_approval(config.approval_mode),
                config=_thread_wire_config(config.web_search_mode),
                cwd=str(cwd),
                model=config.model,
                personality=_personality(config.personality),
                sandbox=_sandbox(config.sandbox),
                service_tier=config.service_tier,
            )
            self._threads[thread.id] = thread
            return await self._identity_after_mutation(
                thread,
                requested_thread_id=thread_id,
                operation="thread.resume",
            )
        except CodexError as exc:
            raise _adapter_error(
                exc,
                operation="thread.resume",
                generation=self.generation,
                thread_id=thread_id,
            ) from exc

    async def fork_thread(
        self, *, thread_id: str, cwd: Path, config: ThreadConfig
    ) -> ThreadIdentity:
        self._ensure_open()
        try:
            thread = await self._client.thread_fork(
                thread_id,
                approval_mode=_approval(config.approval_mode),
                config=_thread_wire_config(config.web_search_mode),
                cwd=str(cwd),
                ephemeral=False,
                model=config.model,
                sandbox=_sandbox(config.sandbox),
                service_tier=config.service_tier,
            )
        except CodexError as exc:
            raise _adapter_error(
                exc,
                operation="thread.fork",
                generation=self.generation,
                thread_id=thread_id,
            ) from exc
        self._threads[thread.id] = thread
        return await self._identity_after_mutation(
            thread,
            requested_thread_id=None,
            operation="thread.fork",
        )

    async def read_thread(self, thread_id: str) -> ThreadSnapshot:
        thread = self._thread(thread_id)
        try:
            response = await thread.read(include_turns=False)
        except CodexError as exc:
            raise _adapter_error(
                exc,
                operation="thread.read",
                generation=self.generation,
                thread_id=thread_id,
            ) from exc
        identity = _thread_identity(
            response.thread,
            requested_thread_id=thread_id,
            sdk_version=self._manifest.sdk_version,
        )
        status_root = response.thread.status.root
        status_type = str(status_root.type)
        try:
            state = ThreadProviderState(status_type)
        except ValueError:
            state = ThreadProviderState.UNKNOWN
        flags = tuple(_enum_value(flag) for flag in getattr(status_root, "active_flags", []))
        return ThreadSnapshot(identity=identity, state=state, active_flags=flags)

    async def set_thread_name(self, thread_id: str, name: str) -> None:
        if not name.strip():
            raise InvariantError("thread name may not be empty")
        try:
            await self._thread(thread_id).set_name(name.strip())
        except CodexError as exc:
            raise _adapter_error(
                exc,
                operation="thread.set_name",
                generation=self.generation,
                thread_id=thread_id,
            ) from exc

    async def compact_thread(self, thread_id: str) -> CompactStartResult:
        compact_task = asyncio.create_task(
            self._thread(thread_id).compact(),
            name=f"codex-compact:{thread_id}",
        )
        try:
            done, _ = await asyncio.wait(
                {compact_task},
                timeout=_THREAD_COMPACT_START_TIMEOUT_SECONDS,
            )
        except BaseException:
            _cancel_in_background(compact_task)
            raise
        if compact_task not in done:
            _cancel_in_background(compact_task)
            exc = TimeoutError()
            raise _provider_outcome_unknown(
                exc,
                operation="thread.compact",
                generation=self.generation,
                thread_id=thread_id,
                message=(
                    "Codex compaction acknowledgement timed out; "
                    "the provider outcome is unknown"
                ),
            ) from exc
        try:
            await compact_task
        except CodexError as exc:
            raise _adapter_error(
                exc,
                operation="thread.compact",
                generation=self.generation,
                thread_id=thread_id,
            ) from exc
        return CompactStartResult(accepted=True)

    async def start_turn(
        self,
        *,
        local_turn_id: str,
        thread: ThreadIdentity,
        input: TurnInput,
        config: TurnConfig,
    ) -> StartedTurn:
        mention_input = _resolve_mention_input_constructor(self._manifest.sdk_version)
        if input.files and (
            self._manifest.optional.get("mention.input") is not True
            or mention_input is None
            or not _file_input_leasing_supported()
        ):
            raise file_input_unsupported(
                generation=self.generation,
                thread_id=thread.thread_id,
                turn_id=local_turn_id,
            )
        handle_thread = self._thread(thread.thread_id)
        wire_input: list[InputItem] = []
        if input.text:
            wire_input.append(TextInput(input.text))
        wire_input.extend(
            SkillInput(skill.name, str(skill.canonical_path))
            for skill in input.skill_inputs
        )
        attachment_input: list[tuple[int, InputItem]] = [
            (image.ordinal, LocalImageInput(str(image.canonical_path)))
            for image in input.images
        ]
        leases = _acquire_file_input_leases(input.files)
        provider_start_attempted = False
        try:
            if input.files:
                assert mention_input is not None
                attachment_input.extend(
                    (
                        file.ordinal,
                        mention_input(file.display_name, str(file.canonical_path)),
                    )
                    for file in input.files
                )
            wire_input.extend(
                item
                for _ordinal, item in sorted(
                    attachment_input,
                    key=lambda entry: entry[0],
                )
            )
            if leases.descriptors:
                self._pending_file_input_leases.add(leases)
                self._remember_app_server_process()
            provider_start_attempted = True
            handle: AsyncTurnHandle | DynamicAsyncTurnHandle
            if isinstance(handle_thread, DynamicAsyncThread):
                handle = await handle_thread.turn(
                    wire_input,
                    local_turn_id=local_turn_id,
                    approval_mode=_approval(config.approval_mode),
                    cwd=str(config.cwd),
                    effort=_effort(config.reasoning_effort),
                    model=config.model,
                    output_schema=(
                        dict(config.output_schema)
                        if config.output_schema
                        else None
                    ),
                    personality=_personality(config.personality),
                    sandbox=_sandbox(config.sandbox),
                    service_tier=config.service_tier,
                    summary=_summary(config.reasoning_summary),
                )
            else:
                handle = await handle_thread.turn(
                    wire_input,
                    approval_mode=_approval(config.approval_mode),
                    cwd=str(config.cwd),
                    effort=_effort(config.reasoning_effort),
                    model=config.model,
                    output_schema=(
                        dict(config.output_schema)
                        if config.output_schema
                        else None
                    ),
                    personality=_personality(config.personality),
                    sandbox=_sandbox(config.sandbox),
                    service_tier=config.service_tier,
                    summary=_summary(config.reasoning_summary),
                )
        except BaseException as exc:
            if input.files and provider_start_attempted:
                close_error = await self._retire_after_uncertain_file_start()
                if close_error is not None:
                    exc.add_note(
                        "Codex app-server close failed during uncertain file Turn cleanup; "
                        "synchronous force-termination was attempted before lease release"
                    )
                if isinstance(exc, asyncio.CancelledError):
                    raise
                raise _uncertain_file_turn_start(
                    exc,
                    generation=self.generation,
                    thread_id=thread.thread_id,
                    turn_id=local_turn_id,
                ) from exc
            self._pending_file_input_leases.discard(leases)
            leases.close()
            if isinstance(exc, CodexError):
                raise _adapter_error(
                    exc,
                    operation="turn.start",
                    generation=self.generation,
                    thread_id=thread.thread_id,
                    turn_id=local_turn_id,
                ) from exc
            raise
        if input.files and self._closed:
            self._pending_file_input_leases.discard(leases)
            leases.close()
            closed_error = RuntimeError(
                "Codex runtime closed while starting a file Turn"
            )
            raise _uncertain_file_turn_start(
                closed_error,
                generation=self.generation,
                thread_id=thread.thread_id,
                turn_id=local_turn_id,
            ) from closed_error
        if handle.id in self._file_input_leases:
            if input.files:
                duplicate_error = InvariantError(
                    "Codex Turn handle already owns a file input lease"
                )
                await self._retire_after_uncertain_file_start()
                raise _uncertain_file_turn_start(
                    duplicate_error,
                    generation=self.generation,
                    thread_id=thread.thread_id,
                    turn_id=local_turn_id,
                ) from duplicate_error
            raise InvariantError("Codex Turn handle already owns a file input lease")
        self._pending_file_input_leases.discard(leases)
        if leases.descriptors:
            self._file_input_leases[handle.id] = leases
        self._turn_handles[handle.id] = handle
        identity = TurnIdentity(local_turn_id, handle.id, self.generation)

        async def iterator() -> AsyncIterator[NormalizedEvent]:
            terminal_seen = False
            try:
                async for notification in handle.stream():
                    _assert_notification_route(
                        notification,
                        expected_thread_id=thread.thread_id,
                        expected_turn_id=handle.id,
                        generation=self.generation,
                    )
                    observed_image_path = _provider_image_path(notification)
                    if (
                        observed_image_path is not None
                        and isinstance(self._client, DynamicAsyncCodex)
                    ):
                        self._client.observe_image_path(
                            handle.id,
                            observed_image_path,
                        )
                    unknown_terminal = (
                        notification.method == "turn/completed"
                        and isinstance(notification.payload, UnknownNotification)
                    )
                    agent_thread_id = _subagent_thread_id(notification)
                    event = _normalize_notification(notification, cwd=config.cwd)
                    if agent_thread_id is not None:
                        detail = await self._subagent_detail(
                            agent_thread_id=agent_thread_id,
                            provider_session_id=thread.provider_session_id,
                            cwd=config.cwd,
                        )
                        if detail:
                            event = replace(event, payload={**event.payload, **detail})
                    terminal_event = event.kind in {
                        "turn.completed",
                        "turn.failed",
                        "turn.interrupted",
                    }
                    provider_terminal = (
                        unknown_terminal
                        or event.kind == "turn.terminal_unparseable"
                        or terminal_event
                    )
                    if provider_terminal:
                        terminal_seen = True
                        self._turn_handles.pop(handle.id, None)
                        self._release_file_input_lease(handle.id)
                    yield event
                    if unknown_terminal or event.kind == "turn.terminal_unparseable":
                        raise _protocol_incompatible(
                            generation=self.generation,
                            thread_id=thread.thread_id,
                            turn_id=handle.id,
                        )
                    if terminal_event:
                        break
            except CodexError as exc:
                raise _adapter_error(
                    exc,
                    operation="turn.stream",
                    generation=self.generation,
                    thread_id=thread.thread_id,
                    turn_id=handle.id,
                ) from exc
            if not terminal_seen:
                failure = AdapterFailure(
                    code="stream_ended_unexpectedly",
                    provider_exception="StreamEnded",
                    message="Codex Turn stream ended without a terminal event",
                    retryable=False,
                    runtime_generation=self.generation,
                    thread_id=thread.thread_id,
                    turn_id=handle.id,
                )
                raise RuntimeUnavailable(failure)

        return StartedTurn(identity=identity, stream=TurnStream(iterator))

    async def _subagent_detail(
        self,
        *,
        agent_thread_id: str,
        provider_session_id: str,
        cwd: Path,
    ) -> dict[str, str]:
        cached = self._subagent_details.get(agent_thread_id)
        if cached is not None:
            return cached
        try:
            child_thread: AsyncThread | DynamicAsyncThread
            child_thread = (
                DynamicAsyncThread(self._client, agent_thread_id)
                if isinstance(self._client, DynamicAsyncCodex)
                else AsyncThread(self._client, agent_thread_id)
            )
            response = await asyncio.wait_for(
                child_thread.read(include_turns=False),
                timeout=_SUBAGENT_DETAIL_TIMEOUT_SECONDS,
            )
        except (CodexError, TimeoutError) as exc:
            logger.warning(
                "Subagent activity detail could not be read",
                extra={
                    "stable_code": "subagent_detail_unavailable",
                    "exception_type": type(exc).__name__,
                },
            )
            detail: dict[str, str] = {}
        else:
            child = response.thread
            if (
                child.id != agent_thread_id
                or child.session_id != provider_session_id
                or child.parent_thread_id is None
            ):
                logger.warning(
                    "Subagent activity detail failed identity validation",
                    extra={"stable_code": "subagent_detail_identity_mismatch"},
                )
                detail = {}
            else:
                role = _safe_subagent_detail(child.agent_role, cwd=cwd, limit=128)
                summary = _safe_subagent_detail(
                    child.preview or child.name,
                    cwd=cwd,
                    limit=512,
                )
                detail = {}
                if role:
                    detail["agent_role"] = role
                if summary:
                    detail["activity_summary"] = summary
        self._subagent_details[agent_thread_id] = detail
        return detail

    async def steer(self, turn: TurnIdentity, text: str) -> None:
        handle = self._turn_handle(turn)
        try:
            response = await handle.steer(TextInput(text))
            if response.turn_id != handle.id:
                raise _notification_route_mismatch(
                    generation=self.generation,
                    thread_id=handle.thread_id,
                    turn_id=handle.id,
                )
        except CodexError as exc:
            raise _adapter_error(
                exc,
                operation="turn.steer",
                generation=self.generation,
                thread_id=handle.thread_id,
                turn_id=handle.id,
            ) from exc

    async def interrupt(self, turn: TurnIdentity) -> None:
        handle = self._turn_handle(turn)
        try:
            await handle.interrupt()
        except CodexError as exc:
            raise _adapter_error(
                exc,
                operation="turn.interrupt",
                generation=self.generation,
                thread_id=handle.thread_id,
                turn_id=handle.id,
            ) from exc

    async def archive_thread(self, thread_id: str) -> None:
        try:
            await self._client.thread_archive(thread_id)
        except CodexError as exc:
            raise _adapter_error(
                exc,
                operation="thread.archive",
                generation=self.generation,
                thread_id=thread_id,
            ) from exc

    async def unarchive_thread(self, thread_id: str) -> ThreadIdentity:
        try:
            thread = await self._client.thread_unarchive(thread_id)
        except CodexError as exc:
            raise _adapter_error(
                exc,
                operation="thread.unarchive",
                generation=self.generation,
                thread_id=thread_id,
            ) from exc
        self._threads[thread.id] = thread
        return await self._identity_after_mutation(
            thread,
            requested_thread_id=thread_id,
            operation="thread.unarchive",
        )

    async def close(self) -> None:
        async with self._close_lock:
            if self._closed:
                return
            process = self._remember_app_server_process()
            self._file_lease_release_blocked = True
            try:
                await self._client.close()
            except BaseException:
                if _force_terminate_app_server(
                    self._client,
                    process=process,
                ):
                    self._finalize_closed_runtime()
                raise
            if not _confirm_app_server_exit(process) and not _force_terminate_app_server(
                self._client,
                process=process,
            ):
                raise OSError("Codex app-server termination could not be confirmed")
            self._finalize_closed_runtime()

    async def _retire_after_uncertain_file_start(self) -> BaseException | None:
        async with self._close_lock:
            if self._closed:
                self._release_all_file_input_leases()
                return None
            process = self._remember_app_server_process()
            close_error: BaseException | None = None
            termination_confirmed = False
            self._file_lease_release_blocked = True
            try:
                async with asyncio.timeout(_FILE_INPUT_RUNTIME_RETIRE_TIMEOUT_SECONDS):
                    await self._client.close()
                termination_confirmed = True
            except BaseException as exc:
                close_error = exc
                termination_confirmed = _force_terminate_app_server(
                    self._client,
                    process=process,
                )
            else:
                termination_confirmed = _confirm_app_server_exit(process)
                if not termination_confirmed:
                    termination_confirmed = _force_terminate_app_server(
                        self._client,
                        process=process,
                    )
                if not termination_confirmed:
                    close_error = OSError(
                        "Codex app-server termination could not be confirmed"
                    )
            if termination_confirmed:
                self._finalize_closed_runtime()
            return close_error

    def _remember_app_server_process(self) -> Any | None:
        if self._file_lease_process is None:
            self._file_lease_process = _app_server_process(self._client)
        return self._file_lease_process

    def _finalize_closed_runtime(self) -> None:
        self._release_all_file_input_leases()
        self._file_lease_process = None
        self._closed = True
        self._turn_handles.clear()
        self._threads.clear()

    async def _identity(
        self,
        thread: AsyncThread | DynamicAsyncThread,
        *,
        requested_thread_id: str | None,
    ) -> ThreadIdentity:
        response = await thread.read(include_turns=False)
        return _thread_identity(
            response.thread,
            requested_thread_id=requested_thread_id,
            sdk_version=self._manifest.sdk_version,
        )

    async def _identity_after_mutation(
        self,
        thread: AsyncThread | DynamicAsyncThread,
        *,
        requested_thread_id: str | None,
        operation: str,
    ) -> ThreadIdentity:
        first_error: CodexError | None = None
        for _attempt in range(2):
            try:
                return await self._identity(
                    thread,
                    requested_thread_id=requested_thread_id,
                )
            except CodexError as exc:
                if first_error is None:
                    first_error = exc
                    continue
                exc.add_note(
                    f"initial identity read also failed ({type(first_error).__name__})"
                )
                raise _provider_outcome_unknown(
                    exc,
                    operation=operation,
                    generation=self.generation,
                    thread_id=thread.id,
                ) from exc
        raise AssertionError("identity reconciliation loop did not return")

    def _thread(self, thread_id: str) -> AsyncThread | DynamicAsyncThread:
        self._ensure_open()
        try:
            return self._threads[thread_id]
        except KeyError as exc:
            raise NotFoundError(
                f"thread {thread_id} is not loaded in runtime generation {self.generation}"
            ) from exc

    def _turn_handle(
        self, turn: TurnIdentity
    ) -> AsyncTurnHandle | DynamicAsyncTurnHandle:
        self._ensure_open()
        if turn.runtime_generation != self.generation or not turn.provider_turn_id:
            raise InvariantError("Turn handle belongs to another runtime generation")
        try:
            return self._turn_handles[turn.provider_turn_id]
        except KeyError as exc:
            raise NotFoundError("active Codex Turn handle not found") from exc

    def _release_file_input_lease(self, provider_turn_id: str) -> None:
        if self._file_lease_release_blocked:
            return
        lease = self._file_input_leases.pop(provider_turn_id, None)
        if lease is not None:
            lease.close()
        if not self._file_input_leases and not self._pending_file_input_leases:
            self._file_lease_process = None

    def _release_all_file_input_leases(self) -> None:
        leases = (
            *self._file_input_leases.values(),
            *self._pending_file_input_leases,
        )
        self._file_input_leases.clear()
        self._pending_file_input_leases.clear()
        for lease in leases:
            lease.close()
        self._file_lease_release_blocked = False

    def _ensure_open(self) -> None:
        if self._closed:
            raise InvariantError("Codex runtime is closed")
        if self._file_lease_release_blocked:
            raise InvariantError("Codex runtime termination is unconfirmed")


def _sdk_config(
    slot: RuntimeSlotConfig,
    *,
    generation: int,
    experimental_api: bool = False,
) -> CodexConfig:
    return CodexConfig(
        codex_bin=str(slot.codex_bin) if slot.codex_bin is not None else None,
        launch_args_override=None,
        config_overrides=(),
        cwd=str(slot.cwd),
        env=_sdk_environment(slot, generation=generation),
        experimental_api=experimental_api,
    )


def _sdk_environment(
    slot: RuntimeSlotConfig,
    *,
    generation: int,
) -> dict[str, str]:
    environment = dict(slot.environment)
    if slot.codex_home is None:
        return environment
    expected = slot.codex_home.expanduser().resolve()
    configured = environment.get("CODEX_HOME")
    if configured is not None and Path(configured).expanduser().resolve() != expected:
        failure = AdapterFailure(
            code="codex_home_conflict",
            provider_exception="ConfigurationConflict",
            message="Runtime slot CODEX_HOME conflicts with the validated environment",
            retryable=False,
            runtime_generation=generation,
        )
        raise AdapterInvariantError(failure)
    environment["CODEX_HOME"] = str(expected)
    return environment


def _configured_provider_requires_openai_auth(
    slot: RuntimeSlotConfig,
) -> bool | None:
    codex_home = slot.codex_home
    if codex_home is None:
        configured_home = slot.environment.get("CODEX_HOME")
        if configured_home:
            codex_home = Path(configured_home)
        else:
            home = slot.environment.get("HOME")
            if not home:
                return None
            codex_home = Path(home) / ".codex"
    try:
        config = tomllib.loads(
            (codex_home / "config.toml").read_text(encoding="utf-8")
        )
    except (OSError, tomllib.TOMLDecodeError):
        return None
    provider_name = config.get("model_provider")
    providers = config.get("model_providers")
    if not isinstance(provider_name, str) or not isinstance(providers, dict):
        return None
    provider = providers.get(provider_name)
    if not isinstance(provider, dict):
        return None
    requires_auth = provider.get("requires_openai_auth")
    return requires_auth if isinstance(requires_auth, bool) else None


async def _close_after_failed_create(
    client: AsyncCodex | DynamicAsyncCodex,
    original: BaseException,
) -> None:
    try:
        await client.close()
    except Exception as close_error:
        original.add_note(
            f"Codex client cleanup also failed ({type(close_error).__name__})"
        )


def _parse_stable_version(value: str) -> tuple[int, int, int]:
    match = re.fullmatch(r"(\d+)\.(\d+)\.(\d+)", value)
    if match is None:
        failure = AdapterFailure(
            code="sdk_version_out_of_range",
            provider_exception="VersionMismatch",
            message=f"openai-codex {value} is outside {SDK_DECLARED_RANGE}",
            retryable=False,
            runtime_generation=0,
        )
        raise AdapterInvariantError(failure)
    return (
        int(match.group(1)),
        int(match.group(2)),
        int(match.group(3)),
    )


def _assert_runtime_semantic_version(value: str) -> None:
    if re.fullmatch(r"\d+\.\d+\.\d+", value) is not None:
        return
    failure = AdapterFailure(
        code="runtime_version_invalid",
        provider_exception="VersionMismatch",
        message="Initialized Codex runtime did not report a stable semantic version",
        retryable=False,
        runtime_generation=0,
    )
    raise AdapterInvariantError(failure)


def _cancel_in_background(task: asyncio.Task[Any]) -> None:
    task.cancel()

    def consume_result(completed: asyncio.Task[Any]) -> None:
        with suppress(asyncio.CancelledError, Exception):
            completed.result()

    task.add_done_callback(consume_result)


def _assert_notification_route(
    notification: Notification,
    *,
    expected_thread_id: str,
    expected_turn_id: str,
    generation: int,
) -> None:
    payload = notification.payload
    if isinstance(payload, UnknownNotification):
        if notification.method not in _COMPAT_UNKNOWN_NOTIFICATION_METHODS:
            return
        thread_id = payload.params.get("threadId")
        turn_id = payload.params.get("turnId")
    else:
        thread_id = getattr(payload, "thread_id", None)
        turn = getattr(payload, "turn", None)
        turn_id = getattr(turn, "id", None) or getattr(payload, "turn_id", None)
    lifecycle = notification.method in {"turn/started", "turn/completed"}
    if (
        (thread_id is not None and thread_id != expected_thread_id)
        or (turn_id is not None and turn_id != expected_turn_id)
        or (lifecycle and (thread_id is None or turn_id is None))
    ):
        raise _notification_route_mismatch(
            generation=generation,
            thread_id=expected_thread_id,
            turn_id=expected_turn_id,
        )


def _subagent_thread_id(notification: Notification) -> str | None:
    if notification.method not in {"item/started", "item/completed"}:
        return None
    item_wrapper = getattr(notification.payload, "item", None)
    item = getattr(item_wrapper, "root", None)
    if getattr(item, "type", None) != "subAgentActivity":
        return None
    value = getattr(item, "agent_thread_id", None)
    return value if isinstance(value, str) and value else None


def _provider_image_path(notification: Notification) -> str | None:
    if notification.method not in {"item/started", "item/completed"}:
        return None
    item_wrapper = getattr(notification.payload, "item", None)
    item = getattr(item_wrapper, "root", None)
    item_type = getattr(item, "type", None)
    if item_type == "imageView":
        path = getattr(getattr(item, "path", None), "root", None)
    elif item_type == "imageGeneration":
        path = getattr(getattr(item, "saved_path", None), "root", None)
    else:
        return None
    return path if isinstance(path, str) and path else None


def _notification_route_mismatch(
    *,
    generation: int,
    thread_id: str,
    turn_id: str,
) -> AdapterInvariantError:
    return AdapterInvariantError(
        AdapterFailure(
            code="runtime_notification_route_mismatch",
            provider_exception="NotificationRouteMismatch",
            message="Codex notification did not match its Turn handle",
            retryable=False,
            runtime_generation=generation,
            thread_id=thread_id,
            turn_id=turn_id,
        )
    )


def _protocol_incompatible(
    *,
    generation: int,
    thread_id: str,
    turn_id: str,
) -> RuntimeUnavailable:
    return RuntimeUnavailable(
        AdapterFailure(
            code="runtime_protocol_incompatible",
            provider_exception="UnknownTerminalNotification",
            message="Codex terminal notification schema is incompatible",
            retryable=False,
            runtime_generation=generation,
            thread_id=thread_id,
            turn_id=turn_id,
        )
    )


def _callable_accepts(
    owner: type[Any],
    name: str,
    parameters: set[str],
) -> bool:
    candidate = getattr(owner, name, None)
    return callable(candidate) and parameters <= set(inspect.signature(candidate).parameters)


def _resolve_mention_input_constructor(
    sdk_version: str,
) -> Callable[[str, str], InputItem] | None:
    if sdk_version not in _MENTION_INPUT_VERIFIED_SDK_VERSIONS:
        return None
    try:
        candidate = getattr(openai_codex, "MentionInput", None)
    except Exception:
        return None
    if not callable(candidate):
        return None
    try:
        parameters = tuple(inspect.signature(candidate).parameters.values())
        probe = candidate("contract-name", "contract-path")
    except Exception:
        return None
    if not (
        tuple(parameter.name for parameter in parameters) == ("name", "path")
        and all(
            parameter.kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
            and parameter.default is inspect.Parameter.empty
            for parameter in parameters
        )
        and getattr(probe, "name", None) == "contract-name"
        and getattr(probe, "path", None) == "contract-path"
    ):
        return None
    return cast(Callable[[str, str], InputItem], candidate)


def _mention_input_contract_supported(sdk_version: str) -> bool:
    return _resolve_mention_input_constructor(sdk_version) is not None


def _file_input_supported(sdk_version: str) -> bool:
    return _mention_input_contract_supported(sdk_version) and _file_input_leasing_supported()


def _file_input_leasing_supported() -> bool:
    if not private_files.private_file_security_supported():
        return False
    if os.name == "nt":
        return True
    return bool(
        os.name == "posix"
        and _FCNTL is not None
        and getattr(os, "O_NOFOLLOW", 0)
        and getattr(os, "O_DIRECTORY", 0)
        and os.open in os.supports_dir_fd
    )


def _acquire_file_input_leases(files: tuple[TurnFile, ...]) -> _FileInputLeases:
    if not files:
        return _FileInputLeases()
    descriptors: list[int] = []
    current: TurnFile | None = None
    try:
        for current in files:
            descriptors.append(_acquire_file_input_lease(current))
    except _FileInputLeaseError:
        _FileInputLeases(tuple(descriptors)).close()
        attachment_id = current.attachment_id if current is not None else "unknown"
        raise AttachmentIntegrityError(
            "ordinary file attachment failed final integrity validation: "
            f"{attachment_id}"
        ) from None
    return _FileInputLeases(tuple(descriptors))


def _acquire_file_input_lease(file: TurnFile) -> int:
    if not _file_input_leasing_supported():
        raise _FileInputLeaseError("secure file leasing is unavailable")
    path = file.canonical_path
    descriptor = -1
    try:
        _validate_file_lease_path(path)
        descriptor = _open_file_lease_descriptor(path)
        if os.name != "nt":
            assert _FCNTL is not None
            _FCNTL.flock(descriptor, _FCNTL.LOCK_SH | _FCNTL.LOCK_NB)
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise _FileInputLeaseError("leased target is not a regular file")
        private_files.validate_private_file_descriptor(descriptor)
        if before.st_size != file.size_bytes:
            raise _FileInputLeaseError("leased file size changed")

        os.lseek(descriptor, 0, os.SEEK_SET)
        digest = hashlib.sha256()
        observed_size = 0
        while observed_size <= file.size_bytes:
            chunk = os.read(
                descriptor,
                min(1024 * 1024, file.size_bytes + 1 - observed_size),
            )
            if not chunk:
                break
            observed_size += len(chunk)
            digest.update(chunk)
        after = os.fstat(descriptor)
        if (
            before.st_dev != after.st_dev
            or before.st_ino != after.st_ino
            or before.st_size != after.st_size
            or observed_size != after.st_size
        ):
            raise _FileInputLeaseError("leased file changed while hashing")
        if observed_size != file.size_bytes or not hmac.compare_digest(
            digest.hexdigest(), file.sha256
        ):
            raise _FileInputLeaseError("leased file content changed")

        _validate_file_lease_path(path)
        named = path.lstat()
        if (
            stat.S_ISLNK(named.st_mode)
            or not stat.S_ISREG(named.st_mode)
            or named.st_dev != after.st_dev
            or named.st_ino != after.st_ino
            or named.st_size != after.st_size
        ):
            raise _FileInputLeaseError("leased file no longer matches its named path")
        private_files.validate_private_file(path)
        return descriptor
    except (OSError, ValueError):
        if descriptor >= 0:
            _FileInputLeases((descriptor,)).close()
        raise _FileInputLeaseError("file lease validation failed") from None


def _validate_file_lease_path(path: Path) -> None:
    if not path.is_absolute() or ".." in path.parts or path.name in {"", ".", ".."}:
        raise _FileInputLeaseError("file lease path is invalid")
    if path.parent.name != "input" or path.parent.parent.name != "attachments":
        raise _FileInputLeaseError("file lease path is outside private attachment storage")

    current = Path(path.anchor)
    for part in path.parts[1:-1]:
        current /= part
        private_files.validate_directory_no_reparse(current)
        metadata = current.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise _FileInputLeaseError("file lease path contains an unsafe parent")

    for directory in (path.parent.parent.parent, path.parent.parent, path.parent):
        private_files.validate_private_directory(directory)


def _open_file_lease_descriptor(path: Path) -> int:
    if os.name == "nt":
        return private_files.open_file_no_reparse(
            path,
            require_private=True,
            deny_write_delete=True,
        )
    nofollow = getattr(os, "O_NOFOLLOW", 0)
    directory_flags = (
        os.O_RDONLY
        | getattr(os, "O_DIRECTORY", 0)
        | nofollow
        | getattr(os, "O_CLOEXEC", 0)
    )
    directory_descriptor = os.open(path.anchor, directory_flags)
    try:
        for part in path.parts[1:-1]:
            child = os.open(
                part,
                directory_flags,
                dir_fd=directory_descriptor,
            )
            os.close(directory_descriptor)
            directory_descriptor = child
        return os.open(
            path.name,
            os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0),
            dir_fd=directory_descriptor,
        )
    finally:
        os.close(directory_descriptor)


def _verify_public_contract() -> None:
    required_parameters: dict[Any, set[str]] = {
        CodexConfig: {"cwd", "env", "experimental_api"},
        AsyncCodex.thread_start: {
            "approval_mode",
            "config",
            "cwd",
            "ephemeral",
            "model",
            "personality",
            "sandbox",
            "service_tier",
        },
        AsyncCodex.thread_resume: {
            "thread_id",
            "approval_mode",
            "config",
            "cwd",
            "model",
            "personality",
            "sandbox",
            "service_tier",
        },
        AsyncThread.turn: {
            "input",
            "approval_mode",
            "cwd",
            "effort",
            "model",
            "output_schema",
            "personality",
            "sandbox",
            "service_tier",
            "summary",
        },
    }
    missing: list[str] = []
    for callable_object, names in required_parameters.items():
        actual = set(inspect.signature(callable_object).parameters)
        absent = names - actual
        if absent:
            missing.append(f"{callable_object.__qualname__}: {sorted(absent)}")
    required_methods = {
        AsyncCodex: {
            "account",
            "close",
            "models",
            "thread_resume",
            "thread_start",
        },
        AsyncThread: {"read", "turn"},
        AsyncTurnHandle: {"stream", "interrupt", "steer"},
    }
    for owner, names in required_methods.items():
        absent = {name for name in names if not callable(getattr(owner, name, None))}
        if absent:
            missing.append(f"{owner.__qualname__}: {sorted(absent)}")
    required_fields = {
        TurnStartedNotification: {"thread_id", "turn"},
        TurnCompletedNotification: {"thread_id", "turn"},
    }
    for model, names in required_fields.items():
        actual = set(model.model_fields)
        absent = names - actual
        if absent:
            missing.append(f"{model.__qualname__}: {sorted(absent)}")
    if missing:
        failure = AdapterFailure(
            code="unsupported_sdk_contract",
            provider_exception="SignatureMismatch",
            message="; ".join(missing),
            retryable=False,
            runtime_generation=0,
        )
        raise AdapterInvariantError(failure)


def _dynamic_tool_contract_supported(
    sdk_version: str,
    runtime_version: str,
) -> bool:
    if (sdk_version, runtime_version) not in _DYNAMIC_TOOL_VERIFIED_VERSION_PAIRS:
        return False
    try:
        constructor_parameters = set(inspect.signature(CodexClient).parameters)
        thread_start_parameters = set(
            inspect.signature(CodexClient.thread_start).parameters
        )
        contract_spec: JsonObject = {
            "type": "namespace",
            "name": "codexd",
            "description": "codexD contract probe",
            "tools": [
                {
                    "type": "function",
                    "name": "probe",
                    "description": "codexD contract probe",
                    "inputSchema": {
                        "type": "object",
                        "additionalProperties": False,
                    },
                }
            ],
        }
        validated_spec = DynamicToolSpec.model_validate(contract_spec).model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        )
    except (AttributeError, TypeError, ValueError):
        return False
    return (
        {"config", "approval_handler"} <= constructor_parameters
        and "params" in thread_start_parameters
        and callable(getattr(CodexClient, "next_turn_notification", None))
        and callable(getattr(CodexClient, "register_turn_notifications", None))
        and validated_spec == contract_spec
    )


def capability_manifest(
    *,
    runtime_version: str | None = None,
    schedule_tool_enabled: bool = False,
    publish_image_enabled: bool = False,
) -> CapabilityManifest:
    _verify_public_contract()
    sdk_version = importlib.metadata.version("openai-codex")
    effective_runtime_version = runtime_version or importlib.metadata.version(
        "openai-codex-cli-bin"
    )
    _assert_runtime_semantic_version(effective_runtime_version)
    parsed_version = _parse_stable_version(sdk_version)
    if not (_MIN_SDK_VERSION <= parsed_version < _MAX_SDK_VERSION):
        failure = AdapterFailure(
            code="sdk_version_out_of_range",
            provider_exception="VersionMismatch",
            message=f"openai-codex {sdk_version} is outside {SDK_DECLARED_RANGE}",
            retryable=False,
            runtime_generation=0,
        )
        raise AdapterInvariantError(failure)
    if sdk_version == "0.144.4" and effective_runtime_version == "0.144.4":
        matrix_tier = "recommended"
    elif sdk_version == effective_runtime_version:
        matrix_tier = "compatible_patch"
    else:
        matrix_tier = "runtime_override"
    dynamic_tool_call = _dynamic_tool_contract_supported(
        sdk_version,
        effective_runtime_version,
    )
    return CapabilityManifest(
        adapter="openai_codex",
        sdk_version=sdk_version,
        runtime_version=effective_runtime_version,
        compatibility=CompatibilityInfo(
            declared_range=SDK_DECLARED_RANGE,
            matrix_tier=matrix_tier,
            handshake="passed",
        ),
        image_input_modes=("local_path",),
        required={
            "thread.start": True,
            "thread.resume": True,
            "thread.read": True,
            "turn.stream": True,
            "turn.interrupt": True,
            "turn.steer": True,
            "turn.image_input": True,
            "turn.model_override": True,
            "turn.reasoning_effort": True,
            "model.catalog": True,
            "event.turn_lifecycle": True,
            "thread.identity": True,
            "sandbox.configure": True,
            "approval.configure": True,
            "runtime.close": True,
        },
        optional={
            # Current SDK/CLI pairs return an unarchived handle that can be read
            # but cannot directly start a Turn, so the design contract is not met.
            "thread.archive": _ARCHIVE_DIRECT_HANDLE_CONTRACT_VERIFIED,
            "thread.unarchive": _ARCHIVE_DIRECT_HANDLE_CONTRACT_VERIFIED,
            "thread.fork": _callable_accepts(
                AsyncCodex,
                "thread_fork",
                {
                    "thread_id",
                    "approval_mode",
                    "config",
                    "cwd",
                    "ephemeral",
                    "model",
                    "sandbox",
                    "service_tier",
                },
            ),
            "thread.set_name": _callable_accepts(
                AsyncThread,
                "set_name",
                {"name"},
            ),
            "thread.compact": _callable_accepts(AsyncThread, "compact", set()),
            "turn.output_schema": True,
            "turn.personality": True,
            "turn.reasoning_summary": True,
            "turn.service_tier": True,
            "usage.notification": EventCapability.SUPPORTED_NOT_OBSERVED,
            "item.command_file_diff_plan": EventCapability.SUPPORTED,
            "turn.diff.updated": EventCapability.SUPPORTED,
            "web_search.item": EventCapability.SUPPORTED,
            "web_search.config": True,
            "skill.input": True,
            "mention.input": _file_input_supported(sdk_version),
            "mcp.item": EventCapability.SUPPORTED_NOT_OBSERVED,
            "dynamic_tool.item": EventCapability.SUPPORTED_NOT_OBSERVED,
            "dynamic_tool.call": dynamic_tool_call,
            "codexd.schedule_create_tool": (
                dynamic_tool_call and schedule_tool_enabled
            ),
            "codexd.publish_image_tool": (
                dynamic_tool_call and publish_image_enabled
            ),
            "collab.item": EventCapability.SUPPORTED_NOT_OBSERVED,
            "image_generation.item": EventCapability.SUPPORTED_NOT_OBSERVED,
            "account.read": True,
            "account.auth": True,
        },
    )


def _capability_manifest() -> CapabilityManifest:
    return capability_manifest(
        schedule_tool_enabled=True,
        publish_image_enabled=True,
    )


def _initialized_runtime_version(
    client: AsyncCodex | DynamicAsyncCodex,
) -> str:
    server_info = client.metadata.serverInfo
    if server_info is None or server_info.version is None or not server_info.version.strip():
        return importlib.metadata.version("openai-codex-cli-bin")
    match = re.search(r"(?<!\d)(\d+\.\d+\.\d+)(?!\d)", server_info.version)
    return match.group(1) if match is not None else server_info.version.strip()


def _thread_identity(
    thread: Any,
    *,
    requested_thread_id: str | None,
    sdk_version: str,
) -> ThreadIdentity:
    return ThreadIdentity(
        thread_id=thread.id,
        requested_thread_id=requested_thread_id,
        provider_session_id=thread.session_id,
        forked_from_thread_id=thread.forked_from_id,
        parent_thread_id=thread.parent_thread_id,
        provider_version=thread.cli_version or sdk_version,
    )


def _normalize_notification(notification: Notification, *, cwd: Path) -> NormalizedEvent:
    event = _normalize_notification_unredacted(notification, cwd=cwd)
    payload = (
        dict(event.payload)
        if event.kind == "usage.updated"
        else cast(
            dict[str, Any],
            redact_value(dict(event.payload), project_root=cwd),
        )
    )
    return NormalizedEvent(
        kind=event.kind,
        payload=payload,
        provider_event_id=event.provider_event_id,
        occurred_at=event.occurred_at,
        raw_type=event.raw_type,
        raw_hash=event.raw_hash,
        raw_size=event.raw_size,
        schema_version=event.schema_version,
    )


def _normalize_routed_notification(
    method: str,
    payload: Any,
    *,
    cwd: Path,
) -> NormalizedEvent | None:
    if method == "item/commandExecution/terminalInteraction":
        item_id = str(_payload_field(payload, "item_id", "itemId") or "")
        process_id = str(_payload_field(payload, "process_id", "processId") or "")
        stdin = str(_payload_field(payload, "stdin") or "")
        return NormalizedEvent(
            "terminal.interaction",
            {
                "item_id": item_id,
                "process_id_hash": sha256_text(process_id) if process_id else None,
                "stdin_hash": sha256_text(stdin) if stdin else None,
                "stdin_size": len(stdin.encode()),
            },
            provider_event_id=(
                f"terminal:{item_id}:{sha256_text(stdin)}" if item_id else None
            ),
            raw_type=method,
        )
    if method == "item/fileChange/patchUpdated":
        item_id = str(_payload_field(payload, "item_id", "itemId") or "")
        raw_changes = _payload_field(payload, "changes")
        changes = (
            [
                _redact_change(_safe_model_dump(change), cwd)
                for change in raw_changes
            ]
            if isinstance(raw_changes, list)
            else []
        )
        digest = sha256_text(canonical_json(changes))
        return NormalizedEvent(
            "file_change.patch.updated",
            {"item_id": item_id, "changes": changes},
            provider_event_id=f"file-patch:{item_id}:{digest}",
            raw_type=method,
        )
    if method == "item/mcpToolCall/progress":
        item_id = str(_payload_field(payload, "item_id", "itemId") or "")
        message = str(_payload_field(payload, "message") or "")
        safe_message = _bounded(redact_text(message, project_root=cwd), 2048)
        return NormalizedEvent(
            "mcp.progress",
            {"item_id": item_id, "message": safe_message},
            provider_event_id=(
                f"mcp-progress:{item_id}:{sha256_text(message)}" if item_id else None
            ),
            raw_type=method,
        )
    if method in {"hook/started", "hook/completed"}:
        run = _payload_field(payload, "run")
        run_id = str(_payload_field(run, "id") or "")
        run_hash = _opaque_hash(run_id) if run_id else None
        suffix = "started" if method.endswith("started") else "completed"
        return NormalizedEvent(
            f"hook.{suffix}",
            {
                "item_id": run_hash,
                "run_hash": run_hash,
                "event_name": _enum_value(
                    _payload_field(run, "event_name", "eventName")
                ),
                "execution_mode": _enum_value(
                    _payload_field(run, "execution_mode", "executionMode")
                ),
                "handler_type": _enum_value(
                    _payload_field(run, "handler_type", "handlerType")
                ),
                "scope": _enum_value(_payload_field(run, "scope")),
                "status": _enum_value(_payload_field(run, "status")),
                "entry_count": len(_payload_field(run, "entries") or ()),
                "duration_ms": _payload_field(run, "duration_ms", "durationMs"),
            },
            provider_event_id=f"hook:{run_id}:{suffix}" if run_id else None,
            raw_type=method,
        )
    if method in {
        "item/autoApprovalReview/started",
        "item/autoApprovalReview/completed",
    }:
        review = _payload_field(payload, "review")
        action = _payload_field(payload, "action")
        action_root = _payload_field(action, "root") or action
        review_id = str(_payload_field(payload, "review_id", "reviewId") or "")
        review_hash = _opaque_hash(review_id) if review_id else None
        suffix = "started" if method.endswith("started") else "completed"
        return NormalizedEvent(
            f"approval_review.{suffix}",
            {
                "item_id": review_hash,
                "review_hash": review_hash,
                "target_item_id": _payload_field(
                    payload,
                    "target_item_id",
                    "targetItemId",
                ),
                "risk_level": _enum_value(
                    _payload_field(review, "risk_level", "riskLevel")
                ),
                "status": _enum_value(_payload_field(review, "status")),
                "action_type": str(
                    _payload_field(action_root, "type")
                    or type(action_root).__name__
                ),
                "decision_source": _enum_value(
                    _payload_field(payload, "decision_source", "decisionSource")
                ),
            },
            provider_event_id=(
                f"approval-review:{review_id}:{suffix}" if review_id else None
            ),
            raw_type=method,
        )
    if method == "model/rerouted":
        from_model = str(_payload_field(payload, "from_model", "fromModel") or "")
        to_model = str(_payload_field(payload, "to_model", "toModel") or "")
        reason = _payload_field(payload, "reason")
        return NormalizedEvent(
            "model.rerouted",
            {
                "from_model": _bounded(from_model, 256),
                "to_model": _bounded(to_model, 256),
                "reason": _enum_value(_payload_field(reason, "root") or reason),
            },
            provider_event_id=f"model-rerouted:{from_model}:{to_model}",
            raw_type=method,
        )
    if method == "model/safetyBuffering/updated":
        safety = {
            "model": _bounded(str(_payload_field(payload, "model") or ""), 256),
            "faster_model": _optional_bounded(
                _payload_field(payload, "faster_model", "fasterModel"),
                256,
            ),
            "show_buffering_ui": bool(
                _payload_field(payload, "show_buffering_ui", "showBufferingUi")
            ),
            "reasons": _bounded_string_list(_payload_field(payload, "reasons")),
            "use_cases": _bounded_string_list(
                _payload_field(payload, "use_cases", "useCases")
            ),
        }
        return NormalizedEvent(
            "model.safety",
            safety,
            provider_event_id=(
                f"model-safety:{sha256_text(canonical_json(safety))}"
            ),
            raw_type=method,
        )
    if method == "model/verification":
        raw_verifications = _payload_field(payload, "verifications")
        verifications = [
            _enum_value(_payload_field(value, "root") or value)
            for value in (
                raw_verifications if isinstance(raw_verifications, list) else []
            )
        ]
        return NormalizedEvent(
            "model.verification",
            {"verifications": verifications},
            provider_event_id=(
                f"model-verification:{sha256_text(canonical_json(verifications))}"
            ),
            raw_type=method,
        )
    if method == "turn/moderationMetadata":
        metadata = _safe_model_dump(_payload_field(payload, "metadata"))
        serialized = canonical_json(metadata)
        return NormalizedEvent(
            "turn.moderation",
            {
                "metadata_hash": sha256_text(serialized),
                "metadata_size": len(serialized.encode()),
            },
            provider_event_id=f"turn-moderation:{sha256_text(serialized)}",
            raw_type=method,
        )
    if method == "thread/compacted":
        turn_id = str(_payload_field(payload, "turn_id", "turnId") or "")
        return NormalizedEvent(
            "context_compaction.completed",
            {"provider_turn_id": turn_id},
            provider_event_id=f"context-compacted:{turn_id}" if turn_id else None,
            raw_type=method,
        )
    if method == "thread/goal/updated":
        goal = _payload_field(payload, "goal")
        objective = str(_payload_field(goal, "objective") or "")
        goal_safe = {
            "status": _enum_value(_payload_field(goal, "status")),
            "objective_hash": sha256_text(objective) if objective else None,
            "objective_size": len(objective.encode()),
            "time_used_seconds": _payload_field(
                goal,
                "time_used_seconds",
                "timeUsedSeconds",
            ),
            "token_budget": _payload_field(goal, "token_budget", "tokenBudget"),
            "tokens_used": _payload_field(goal, "tokens_used", "tokensUsed"),
        }
        return NormalizedEvent(
            "thread_goal.updated",
            goal_safe,
            provider_event_id=(
                f"thread-goal:{sha256_text(canonical_json(goal_safe))}"
            ),
            raw_type=method,
        )
    if method == "thread/goal/cleared":
        thread_id = str(_payload_field(payload, "thread_id", "threadId") or "")
        return NormalizedEvent(
            "thread_goal.cleared",
            {},
            provider_event_id=f"thread-goal-cleared:{thread_id}",
            raw_type=method,
        )
    return None


def _payload_field(value: Any, *names: str) -> Any:
    if isinstance(value, UnknownNotification):
        value = value.params
    if isinstance(value, dict):
        for name in names:
            if name in value:
                return value[name]
        return None
    for name in names:
        if hasattr(value, name):
            return getattr(value, name)
    return None


def _safe_model_dump(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    dump = getattr(value, "model_dump", None)
    if callable(dump):
        result = dump(mode="json", by_alias=False, exclude_none=True)
        return dict(result) if isinstance(result, dict) else {}
    return {}


def _bounded_string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [_bounded(str(item), 256) for item in value[:32]]


def _normalize_notification_unredacted(
    notification: Notification,
    *,
    cwd: Path,
) -> NormalizedEvent:
    method = notification.method
    # The SDK models method/payload correlation at runtime, but its type is an
    # unnarrowed union. The method dispatch below is the adapter's discriminator.
    payload: Any = notification.payload
    routed = _normalize_routed_notification(method, payload, cwd=cwd)
    if routed is not None:
        return routed
    if isinstance(payload, UnknownNotification):
        raw = payload.params
        if method == "item/reasoning/summaryPartAdded":
            item_id = raw.get("itemId")
            summary_index = raw.get("summaryIndex")
            if (
                isinstance(item_id, str)
                and isinstance(summary_index, int)
                and not isinstance(summary_index, bool)
            ):
                return _reasoning_summary_part_added(
                    item_id=item_id,
                    summary_index=summary_index,
                    method=method,
                )
        return NormalizedEvent.unknown(method=method, raw_payload=raw)

    if method == "item/reasoning/summaryPartAdded" and isinstance(
        payload, ReasoningSummaryPartAddedNotification
    ):
        return _reasoning_summary_part_added(
            item_id=payload.item_id,
            summary_index=payload.summary_index,
            method=method,
        )
    if method == "turn/started":
        return NormalizedEvent(
            "turn.started",
            {
                "provider_turn_id": payload.turn.id,
                "status": _enum_value(payload.turn.status),
            },
            provider_event_id=f"turn:{payload.turn.id}:started",
            raw_type=method,
        )
    if method == "turn/completed":
        status = _enum_value(payload.turn.status)
        kind = {
            TurnStatus.completed.value: "turn.completed",
            TurnStatus.failed.value: "turn.failed",
            TurnStatus.interrupted.value: "turn.interrupted",
        }.get(status, "turn.terminal_unparseable")
        error = payload.turn.error
        return NormalizedEvent(
            kind,
            {
                "provider_turn_id": payload.turn.id,
                "status": status,
                "error": (
                    _bounded(redact_text(error.message))
                    if error
                    else None
                ),
            },
            provider_event_id=f"turn:{payload.turn.id}:completed",
            raw_type=method,
        )
    if method in {"item/started", "item/completed"}:
        return _normalize_item(method, payload.item, cwd=cwd)
    if method == "item/agentMessage/delta":
        return NormalizedEvent(
            "assistant.text.delta",
            {
                "item_id": payload.item_id,
                "text": redact_text(payload.delta, project_root=cwd),
            },
            raw_type=method,
        )
    if method == "item/plan/delta":
        return NormalizedEvent(
            "plan.delta",
            {
                "item_id": payload.item_id,
                "text": redact_text(payload.delta, project_root=cwd),
            },
            raw_type=method,
        )
    if method == "item/reasoning/summaryTextDelta":
        return NormalizedEvent(
            "reasoning.summary",
            {
                "item_id": payload.item_id,
                "summary_index": payload.summary_index,
                "text": _bounded(payload.delta),
            },
            raw_type=method,
        )
    if method == "item/reasoning/textDelta":
        text = payload.delta
        return NormalizedEvent(
            "reasoning.hidden_delta_discarded",
            {
                "item_id": payload.item_id,
                "raw_hash": sha256_text(text),
                "raw_size": len(text.encode()),
            },
            raw_type=method,
            raw_hash=sha256_text(text),
            raw_size=len(text.encode()),
        )
    if method == "item/commandExecution/outputDelta":
        return NormalizedEvent(
            "command.output.delta",
            {"item_id": payload.item_id, "text": _bounded(payload.delta)},
            raw_type=method,
        )
    if method == "item/fileChange/outputDelta":
        return NormalizedEvent(
            "file_change.output.delta",
            {"item_id": payload.item_id, "text": _bounded(payload.delta)},
            raw_type=method,
        )
    if method == "turn/diff/updated":
        return NormalizedEvent(
            "diff.updated", {"diff": payload.diff}, raw_type=method
        )
    if method == "turn/plan/updated":
        return NormalizedEvent(
            "plan.updated",
            {
                "explanation": _bounded(payload.explanation or ""),
                "steps": [
                    step.model_dump(mode="json", by_alias=False, exclude_none=True)
                    for step in payload.plan
                ],
            },
            raw_type=method,
        )
    if method == "thread/tokenUsage/updated":
        return NormalizedEvent(
            "usage.updated",
            payload.token_usage.model_dump(mode="json", by_alias=False, exclude_none=True),
            raw_type=method,
        )
    if method == "error":
        return NormalizedEvent(
            "provider.error",
            {
                "message": _bounded(redact_text(payload.error.message, project_root=cwd)),
                "will_retry": payload.will_retry,
            },
            raw_type=method,
        )
    safe = payload.model_dump(mode="json", by_alias=False, exclude_none=True)
    serialized = canonical_json(safe)
    return NormalizedEvent(
        "provider.unknown",
        {"method": method, "raw_hash": sha256_text(serialized), "raw_size": len(serialized)},
        raw_type=method,
        raw_hash=sha256_text(serialized),
        raw_size=len(serialized.encode()),
    )


def _normalize_item(method: str, item_wrapper: Any, *, cwd: Path) -> NormalizedEvent:
    item = item_wrapper.root
    item_type = str(item.type)
    completed = method == "item/completed"
    suffix = "completed" if completed else "started"
    item_id = item.id
    if item_type == "agentMessage":
        return NormalizedEvent(
            f"assistant.text.{suffix}",
            {
                "item_id": item_id,
                "text": item.text if completed else "",
                "phase": _enum_value(item.phase) if item.phase is not None else None,
            },
            provider_event_id=f"item:{item_id}:{suffix}",
            raw_type=method,
        )
    if item_type == "plan":
        return NormalizedEvent(
            f"plan.{suffix}",
            {"item_id": item_id, "text": item.text if completed else ""},
            provider_event_id=f"item:{item_id}:{suffix}",
            raw_type=method,
        )
    if item_type == "reasoning":
        summary = tuple(_bounded(part) for part in (item.summary or ()))
        hidden_content = item.content or ()
        content = canonical_json(hidden_content)
        return NormalizedEvent(
            f"reasoning.{suffix}",
            {
                "item_id": item_id,
                "summary": summary,
                "hidden_content_hash": sha256_text(content),
                "hidden_content_size": len(content.encode()),
            },
            provider_event_id=f"item:{item_id}:{suffix}",
            raw_type=method,
        )
    if item_type == "commandExecution":
        return NormalizedEvent(
            f"command.{suffix}",
            {
                "item_id": item_id,
                "command": _bounded(
                    redact_text(item.command, project_root=cwd),
                    1024,
                ),
                "cwd": "<project>",
                "status": _enum_value(item.status),
                "exit_code": item.exit_code if completed else None,
                "duration_ms": item.duration_ms if completed else None,
                "output": (
                    _bounded(redact_text(item.aggregated_output, project_root=cwd))
                    if completed and item.aggregated_output
                    else None
                ),
            },
            provider_event_id=f"item:{item_id}:{suffix}",
            raw_type=method,
        )
    if item_type == "fileChange":
        changes = [
            change.model_dump(mode="json", by_alias=False, exclude_none=True)
            for change in item.changes
        ]
        return NormalizedEvent(
            f"file_change.{suffix}",
            {
                "item_id": item_id,
                "status": _enum_value(item.status),
                "changes": [_redact_change(change, cwd) for change in changes],
            },
            provider_event_id=f"item:{item_id}:{suffix}",
            raw_type=method,
        )
    if item_type == "mcpToolCall":
        return NormalizedEvent(
            f"mcp.{suffix}",
            {
                "item_id": item_id,
                "server": _bounded(item.server, 256),
                "tool": _bounded(item.tool, 256),
                "status": _enum_value(item.status),
                "error": (
                    _bounded(redact_text(str(item.error), project_root=cwd))
                    if completed and item.error
                    else None
                ),
            },
            provider_event_id=f"item:{item_id}:{suffix}",
            raw_type=method,
        )
    if item_type == "dynamicToolCall":
        return NormalizedEvent(
            f"dynamic_tool.{suffix}",
            {
                "item_id": item_id,
                "namespace": _optional_bounded(item.namespace, 256),
                "tool": _bounded(item.tool, 256),
                "status": _enum_value(item.status),
                "success": item.success if completed else None,
            },
            provider_event_id=f"item:{item_id}:{suffix}",
            raw_type=method,
        )
    if item_type == "collabAgentToolCall":
        prompt = item.prompt or ""
        return NormalizedEvent(
            f"collaboration.{suffix}",
            {
                "item_id": item_id,
                "operation": _enum_value(item.tool),
                "status": _enum_value(item.status),
                "receiver_count": len(item.receiver_thread_ids),
                "receiver_thread_hashes": [
                    _opaque_hash(thread_id)
                    for thread_id in item.receiver_thread_ids
                ],
                "sender_thread_hash": _opaque_hash(item.sender_thread_id),
                "agents": [
                    {
                        "thread_hash": _opaque_hash(thread_id),
                        "status": _enum_value(agent.status),
                        "message": (
                            _bounded(redact_text(agent.message, project_root=cwd), 2048)
                            if agent.message
                            else None
                        ),
                    }
                    for thread_id, agent in sorted(item.agents_states.items())
                ],
                "prompt_hash": sha256_text(prompt) if prompt else None,
                "prompt_size": len(prompt.encode()),
                "model": item.model,
                "reasoning_effort": (
                    _enum_value(item.reasoning_effort) if item.reasoning_effort else None
                ),
            },
            provider_event_id=f"item:{item_id}:{suffix}",
            raw_type=method,
        )
    if item_type == "subAgentActivity":
        agent_path = item.agent_path or ""
        return NormalizedEvent(
            f"collaboration.{suffix}",
            {
                "item_id": item_id,
                "operation": "activity",
                "activity_kind": _enum_value(item.kind),
                "agent_thread_hash": _opaque_hash(item.agent_thread_id),
                "agent_path_hash": sha256_text(agent_path) if agent_path else None,
                "agent_path_size": len(agent_path.encode()),
            },
            provider_event_id=f"item:{item_id}:{suffix}",
            raw_type=method,
        )
    if item_type == "webSearch":
        return NormalizedEvent(
            f"web_search.{suffix}",
            {
                "item_id": item_id,
                "query": _bounded(item.query, 2048),
                "action": (
                    item.action.model_dump(mode="json", by_alias=False, exclude_none=True)
                    if item.action
                    else None
                ),
            },
            provider_event_id=f"item:{item_id}:{suffix}",
            raw_type=method,
        )
    if item_type == "imageView":
        return NormalizedEvent(
            f"image_view.{suffix}",
            {
                "item_id": item_id,
                "path": _redact_path(item.path.root, cwd),
            },
            provider_event_id=f"item:{item_id}:{suffix}",
            raw_type=method,
        )
    if item_type == "imageGeneration":
        result = item.result or ""
        revised_prompt = item.revised_prompt or ""
        saved_path = (
            str(item.saved_path.root)
            if item.saved_path is not None
            else ""
        )
        return NormalizedEvent(
            f"image_generation.{suffix}",
            {
                "item_id": item_id,
                "status": item.status,
                "result_hash": sha256_text(result) if result else None,
                "result_size": len(result.encode()),
                "revised_prompt_hash": (
                    sha256_text(revised_prompt) if revised_prompt else None
                ),
                "revised_prompt_size": len(revised_prompt.encode()),
                "saved_path": _redact_path(saved_path, cwd) if saved_path else None,
                "has_saved_path": bool(saved_path),
            },
            provider_event_id=f"item:{item_id}:{suffix}",
            raw_type=method,
        )
    if item_type == "sleep":
        return NormalizedEvent(
            f"sleep.{suffix}",
            {
                "item_id": item_id,
                "duration_ms": item.duration_ms,
            },
            provider_event_id=f"item:{item_id}:{suffix}",
            raw_type=method,
        )
    if item_type in {"enteredReviewMode", "exitedReviewMode"}:
        review = item.review or ""
        action = "entered" if item_type == "enteredReviewMode" else "exited"
        return NormalizedEvent(
            f"review_mode.{action}",
            {
                "item_id": item_id,
                "review_hash": sha256_text(review) if review else None,
                "review_size": len(review.encode()),
                "lifecycle": suffix,
            },
            provider_event_id=f"item:{item_id}:{suffix}",
            raw_type=method,
        )
    if item_type == "contextCompaction":
        return NormalizedEvent(
            f"context_compaction.{suffix}",
            {"item_id": item_id},
            provider_event_id=f"item:{item_id}:{suffix}",
            raw_type=method,
        )
    if item_type == "hookPrompt":
        fragments = [_safe_model_dump(fragment) for fragment in item.fragments]
        serialized = canonical_json(fragments)
        return NormalizedEvent(
            f"provider_input.{suffix}",
            {
                "item_id": item_id,
                "type": item_type,
                "fragment_count": len(fragments),
                "fragment_hash": sha256_text(serialized),
                "fragment_size": len(serialized.encode()),
            },
            provider_event_id=f"item:{item_id}:{suffix}",
            raw_type=method,
        )
    if item_type == "userMessage":
        input_content = [_safe_model_dump(value) for value in item.content]
        serialized = canonical_json(input_content)
        return NormalizedEvent(
            f"provider_input.{suffix}",
            {
                "item_id": item_id,
                "type": item_type,
                "content_count": len(input_content),
                "content_hash": sha256_text(serialized),
                "content_size": len(serialized.encode()),
            },
            provider_event_id=f"item:{item_id}:{suffix}",
            raw_type=method,
        )
    safe = item.model_dump(mode="json", by_alias=False, exclude_none=True)
    serialized = canonical_json(safe)
    return NormalizedEvent(
        "provider.item",
        {
            "item_id": item_id,
            "type": item_type,
            "lifecycle": suffix,
            "raw_hash": sha256_text(serialized),
            "raw_size": len(serialized.encode()),
        },
        provider_event_id=f"item:{item_id}:{suffix}",
        raw_type=method,
        raw_hash=sha256_text(serialized),
        raw_size=len(serialized.encode()),
    )


def _reasoning_summary_part_added(
    *,
    item_id: str,
    summary_index: int,
    method: str,
) -> NormalizedEvent:
    return NormalizedEvent(
        "reasoning.summary_part.added",
        {
            "item_id": item_id,
            "summary_index": summary_index,
        },
        provider_event_id=f"item:{item_id}:summary:{summary_index}:added",
        raw_type=method,
    )


def _thread_wire_config(mode: WebSearchMode) -> JsonObject | None:
    if mode is WebSearchMode.PROVIDER_DEFAULT_UNCONTROLLED:
        return None
    return {"web_search": mode.value}


def _sandbox(profile: SandboxProfile) -> Sandbox:
    return {
        SandboxProfile.FULL_ACCESS: Sandbox.full_access,
        SandboxProfile.WORKSPACE_WRITE: Sandbox.workspace_write,
        SandboxProfile.READ_ONLY: Sandbox.read_only,
    }[profile]


def _approval(policy: ApprovalPolicy) -> ApprovalMode:
    if policy is not ApprovalPolicy.AUTO_REVIEW:
        raise InvariantError(f"unsupported approval policy: {policy}")
    return ApprovalMode.auto_review


def _effort(value: str | None) -> ReasoningEffort | None:
    return ReasoningEffort(value) if value else None


def _personality(value: str | None) -> Personality | None:
    return Personality(value) if value else None


def _summary(value: str | None) -> ReasoningSummary | None:
    return ReasoningSummary.model_validate(value) if value else None


def _enum_value(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw)


def _bounded(value: str, limit: int = _MAX_DELTA) -> str:
    if len(value) <= limit:
        return value
    return value[:limit] + "…"


def _safe_subagent_detail(
    value: str | None,
    *,
    cwd: Path,
    limit: int,
) -> str | None:
    if not value:
        return None
    normalized = " ".join(redact_text(value, project_root=cwd).split())
    return _bounded(normalized, limit) if normalized else None


def _optional_bounded(value: str | None, limit: int = _MAX_DELTA) -> str | None:
    return _bounded(value, limit) if value is not None else None


def _redact_path(value: str, cwd: Path) -> str:
    header = redact_diff(f"--- {value}\n", project_root=cwd)
    return header.removeprefix("--- ").rstrip("\n")


def _redact_change(change: dict[str, Any], cwd: Path) -> dict[str, Any]:
    result = dict(change)
    if isinstance(result.get("path"), str):
        result["path"] = _redact_path(result["path"], cwd)
    if isinstance(result.get("diff"), str):
        result["diff"] = _bounded(
            redact_text(result["diff"], project_root=cwd),
            256 * 1024,
        )
    return result


def _opaque_hash(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _adapter_error(
    exc: CodexError,
    *,
    operation: str,
    generation: int,
    thread_id: str | None = None,
    turn_id: str | None = None,
) -> AdapterError:
    chain = f"{type(exc).__name__}:{exc}"
    error_type: type[AdapterError]
    if isinstance(exc, TransportClosedError):
        if operation in _UNKNOWN_OUTCOME_MUTATIONS:
            code = "provider_effect_outcome_unknown"
            error_type = ProviderOutcomeUnknown
        else:
            code = "runtime_unavailable"
            error_type = RuntimeUnavailable
    elif isinstance(exc, ParseError):
        code = "runtime_protocol_incompatible"
        error_type = RuntimeUnavailable
    elif isinstance(exc, InternalRpcError):
        code = "runtime_internal_error"
        error_type = RuntimeUnavailable
    elif isinstance(exc, (ServerBusyError, RetryLimitExceededError)) or is_retryable_error(
        exc
    ):
        code = "provider_rate_limited"
        error_type = ProviderRateLimited
    elif isinstance(exc, MethodNotFoundError):
        code = "unsupported_capability"
        error_type = UnsupportedCapability
    elif operation == "turn.interrupt":
        code = "interrupt_failed"
        error_type = InterruptFailed
    elif isinstance(exc, (InvalidRequestError, InvalidParamsError, JsonRpcError)):
        code = "provider_rejected"
        error_type = ProviderRejected
    else:
        code = "provider_error"
        error_type = AdapterError
    message = (
        "Codex provider mutation outcome is unknown after transport loss"
        if error_type is ProviderOutcomeUnknown
        else f"Codex provider request failed ({type(exc).__name__})"
    )
    failure = AdapterFailure(
        code=code,
        provider_exception=type(exc).__name__,
        message=message,
        retryable=error_type is ProviderRateLimited,
        runtime_generation=generation,
        thread_id=thread_id,
        turn_id=turn_id,
        cause_chain_hash=hashlib.sha256(chain.encode()).hexdigest(),
    )
    return error_type(failure)


def _provider_outcome_unknown(
    exc: BaseException,
    *,
    operation: str,
    generation: int,
    thread_id: str,
    message: str = (
        "Codex provider mutation succeeded but its Thread identity "
        "could not be reconciled"
    ),
) -> ProviderOutcomeUnknown:
    chain = f"{operation}:{type(exc).__name__}:{exc}"
    return ProviderOutcomeUnknown(
        AdapterFailure(
            code="provider_effect_outcome_unknown",
            provider_exception=type(exc).__name__,
            message=message,
            retryable=False,
            runtime_generation=generation,
            thread_id=thread_id,
            cause_chain_hash=hashlib.sha256(chain.encode()).hexdigest(),
        )
    )


def _uncertain_file_turn_start(
    exc: BaseException,
    *,
    generation: int,
    thread_id: str,
    turn_id: str,
) -> RuntimeUnavailable:
    chain = f"turn.start:{type(exc).__name__}:{exc}"
    return RuntimeUnavailable(
        AdapterFailure(
            code="provider_effect_outcome_unknown",
            provider_exception=type(exc).__name__,
            message=(
                "Codex file Turn start outcome is unknown; the runtime was retired"
            ),
            retryable=False,
            runtime_generation=generation,
            thread_id=thread_id,
            turn_id=turn_id,
            cause_chain_hash=hashlib.sha256(chain.encode()).hexdigest(),
        )
    )
