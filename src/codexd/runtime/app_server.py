from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import threading
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from typing import Any, cast

from openai_codex import (
    ApprovalMode,
    CodexConfig,
    ImageInput,
    InputItem,
    LocalImageInput,
    MentionInput,
    Sandbox,
    SkillInput,
    TextInput,
)
from openai_codex.client import CodexClient
from openai_codex.generated.v2_all import (
    ApprovalsReviewer,
    AskForApproval,
    AskForApprovalValue,
    DangerFullAccessSandboxPolicy,
    DynamicToolSpec,
    GetAccountParams,
    GetAccountResponse,
    ModelListResponse,
    ReadOnlySandboxPolicy,
    SandboxMode,
    SandboxPolicy,
    ThreadCompactStartResponse,
    ThreadForkParams,
    ThreadReadResponse,
    ThreadResumeParams,
    ThreadSetNameResponse,
    ThreadStartParams,
    TurnCompletedNotification,
    TurnInterruptResponse,
    TurnStartParams,
    TurnSteerResponse,
    WorkspaceWriteSandboxPolicy,
)
from openai_codex.models import InitializeResponse, JsonObject, JsonValue, Notification
from openai_codex.types import Personality, ReasoningEffort, ReasoningSummary

from codexd.domain.ids import canonical_json
from codexd.runtime.port import DynamicToolCall, DynamicToolHandler

logger = logging.getLogger(__name__)

_SERVER_REQUEST_TIMEOUT_SECONDS = 10.0
_TURN_ROUTE_WAIT_SECONDS = 1.0
_IMAGE_OBSERVATION_WAIT_SECONDS = 1.0
_MAX_TOOL_RESPONSE_BYTES = 32 * 1024
_APPROVAL_METHODS = frozenset(
    {
        "item/commandExecution/requestApproval",
        "item/fileChange/requestApproval",
    }
)


class DynamicAsyncCodex:
    """Version-gated async facade over the public low-level SDK client.

    The high-level SDK does not expose server-request handlers in 0.144.4.
    This compatibility facade injects the handler through ``CodexClient``'s
    public constructor and otherwise calls only public typed client methods.
    """

    def __init__(
        self,
        config: CodexConfig,
        *,
        generation: int,
        dynamic_tools: Sequence[JsonObject],
        dynamic_tool_handler: DynamicToolHandler,
    ) -> None:
        self._generation = generation
        self._dynamic_tools = tuple(
            _validated_tool_spec(item) for item in dynamic_tools
        )
        self._dynamic_tool_handler = dynamic_tool_handler
        self._loop: asyncio.AbstractEventLoop | None = None
        self._metadata: InitializeResponse | None = None
        self._route_lock = threading.Lock()
        self._route_condition = threading.Condition(self._route_lock)
        self._pending_turns: dict[str, str] = {}
        self._turn_routes: dict[str, tuple[str, str]] = {}
        self._observed_image_paths: dict[str, set[str]] = {}
        self._sync_client = CodexClient(
            config=config,
            approval_handler=self._handle_server_request,
        )

    async def __aenter__(self) -> DynamicAsyncCodex:
        self._loop = asyncio.get_running_loop()
        await asyncio.to_thread(self._sync_client.start)
        try:
            self._metadata = await asyncio.to_thread(self._sync_client.initialize)
        except BaseException:
            await asyncio.to_thread(self._sync_client.close)
            self._loop = None
            raise
        return self

    @property
    def metadata(self) -> InitializeResponse:
        if self._metadata is None:
            raise RuntimeError(
                "Codex app-server compatibility facade is not initialized"
            )
        return self._metadata

    @property
    def process(self) -> Any | None:
        # The public client does not expose its child process. Reading the
        # handle is limited to the existing force-termination safety path; no
        # SDK private state is ever modified.
        return getattr(self._sync_client, "_proc", None)

    async def close(self) -> None:
        await asyncio.to_thread(self._sync_client.close)
        with self._route_lock:
            self._pending_turns.clear()
            self._turn_routes.clear()
            self._observed_image_paths.clear()
        self._metadata = None
        self._loop = None

    async def models(self, *, include_hidden: bool = False) -> ModelListResponse:
        return await asyncio.to_thread(
            self._sync_client.model_list,
            include_hidden,
        )

    async def account(self, *, refresh_token: bool = False) -> GetAccountResponse:
        return await asyncio.to_thread(
            self._sync_client.account_read,
            GetAccountParams(refresh_token=refresh_token),
        )

    async def thread_start(
        self,
        *,
        approval_mode: ApprovalMode,
        config: JsonObject | None,
        cwd: str | None,
        ephemeral: bool | None,
        model: str | None,
        personality: Personality | None,
        sandbox: Sandbox | None,
        service_tier: str | None,
    ) -> DynamicAsyncThread:
        approval_policy, reviewer = _approval_settings(approval_mode)
        params = ThreadStartParams(
            approval_policy=approval_policy,
            approvals_reviewer=reviewer,
            config=config,
            cwd=cwd,
            ephemeral=ephemeral,
            model=model,
            personality=personality,
            sandbox=_sandbox_mode(sandbox),
            service_tier=service_tier,
        )
        payload = _model_payload(params)
        payload["dynamicTools"] = list(self._dynamic_tools)
        response = await asyncio.to_thread(self._sync_client.thread_start, payload)
        return DynamicAsyncThread(self, response.thread.id)

    async def thread_resume(
        self,
        thread_id: str,
        *,
        approval_mode: ApprovalMode | None,
        config: JsonObject | None,
        cwd: str | None,
        model: str | None,
        personality: Personality | None,
        sandbox: Sandbox | None,
        service_tier: str | None,
    ) -> DynamicAsyncThread:
        approval_policy, reviewer = _approval_settings(approval_mode)
        params = ThreadResumeParams(
            thread_id=thread_id,
            approval_policy=approval_policy,
            approvals_reviewer=reviewer,
            config=config,
            cwd=cwd,
            model=model,
            personality=personality,
            sandbox=_sandbox_mode(sandbox),
            service_tier=service_tier,
        )
        response = await asyncio.to_thread(
            self._sync_client.thread_resume,
            thread_id,
            params,
        )
        return DynamicAsyncThread(self, response.thread.id)

    async def thread_fork(
        self,
        thread_id: str,
        *,
        approval_mode: ApprovalMode | None,
        config: JsonObject | None,
        cwd: str | None,
        ephemeral: bool | None,
        model: str | None,
        sandbox: Sandbox | None,
        service_tier: str | None,
    ) -> DynamicAsyncThread:
        approval_policy, reviewer = _approval_settings(approval_mode)
        params = ThreadForkParams(
            thread_id=thread_id,
            approval_policy=approval_policy,
            approvals_reviewer=reviewer,
            config=config,
            cwd=cwd,
            ephemeral=ephemeral,
            model=model,
            sandbox=_sandbox_mode(sandbox),
            service_tier=service_tier,
        )
        response = await asyncio.to_thread(
            self._sync_client.thread_fork,
            thread_id,
            params,
        )
        return DynamicAsyncThread(self, response.thread.id)

    async def thread_archive(self, thread_id: str) -> None:
        await asyncio.to_thread(self._sync_client.thread_archive, thread_id)

    async def thread_unarchive(self, thread_id: str) -> DynamicAsyncThread:
        response = await asyncio.to_thread(
            self._sync_client.thread_unarchive,
            thread_id,
        )
        return DynamicAsyncThread(self, response.thread.id)

    def _start_turn_sync(
        self,
        *,
        local_turn_id: str,
        thread_id: str,
        wire_input: list[JsonObject],
        params: TurnStartParams,
    ) -> str:
        with self._route_condition:
            if thread_id in self._pending_turns:
                raise RuntimeError("a Codex Thread already has a pending local Turn")
            self._pending_turns[thread_id] = local_turn_id
        try:
            response = self._sync_client.turn_start(thread_id, wire_input, params)
            provider_turn_id = response.turn.id
            with self._route_condition:
                existing = self._turn_routes.get(provider_turn_id)
                expected = (thread_id, local_turn_id)
                if existing is not None and existing != expected:
                    raise RuntimeError("dynamic tool Turn route changed during start")
                self._turn_routes[provider_turn_id] = expected
                self._route_condition.notify_all()
            return provider_turn_id
        finally:
            with self._route_condition:
                if self._pending_turns.get(thread_id) == local_turn_id:
                    self._pending_turns.pop(thread_id, None)
                self._route_condition.notify_all()

    def _resolve_local_turn(self, thread_id: str, turn_id: str) -> str | None:
        with self._route_condition:
            route = self._turn_routes.get(turn_id)
            if route is not None:
                return route[1] if route[0] == thread_id else None
            if thread_id not in self._pending_turns:
                return None
            self._route_condition.wait_for(
                lambda: (
                    turn_id in self._turn_routes
                    or thread_id not in self._pending_turns
                ),
                timeout=_TURN_ROUTE_WAIT_SECONDS,
            )
            route = self._turn_routes.get(turn_id)
            return (
                route[1]
                if route is not None and route[0] == thread_id
                else None
            )

    def _forget_turn(self, provider_turn_id: str) -> None:
        with self._route_lock:
            self._turn_routes.pop(provider_turn_id, None)
            self._observed_image_paths.pop(provider_turn_id, None)

    def observe_image_path(self, provider_turn_id: str, path: str) -> None:
        if not path or len(path) > 4096 or "\x00" in path:
            return
        with self._route_condition:
            if provider_turn_id not in self._turn_routes:
                return
            self._observed_image_paths.setdefault(provider_turn_id, set()).add(path)
            self._route_condition.notify_all()

    def _observed_paths_for_call(
        self,
        provider_turn_id: str,
        arguments: object,
    ) -> tuple[str, ...]:
        source_path = (
            arguments.get("source_path")
            if isinstance(arguments, dict)
            else None
        )
        with self._route_condition:
            if isinstance(source_path, str) and source_path:
                self._route_condition.wait_for(
                    lambda: source_path
                    in self._observed_image_paths.get(provider_turn_id, set()),
                    timeout=_IMAGE_OBSERVATION_WAIT_SECONDS,
                )
            return tuple(
                sorted(self._observed_image_paths.get(provider_turn_id, set()))
            )

    def _handle_server_request(
        self,
        method: str,
        params: JsonObject | None,
    ) -> JsonObject:
        if method in _APPROVAL_METHODS:
            return {"decision": "accept"}
        if method != "item/tool/call":
            return {}
        try:
            call = self._dynamic_call(params)
        except (TypeError, ValueError):
            return _tool_failure("invalid_call_identity")
        loop = self._loop
        if loop is None or loop.is_closed():
            return _tool_failure("runtime_unavailable")
        future = asyncio.run_coroutine_threadsafe(
            self._invoke_dynamic_tool(call),
            loop,
        )
        try:
            response = future.result(timeout=_SERVER_REQUEST_TIMEOUT_SECONDS)
            return _validated_tool_response(response)
        except concurrent.futures.TimeoutError:
            future.cancel()
            logger.warning(
                "Dynamic tool handler timed out",
                extra={"stable_code": "dynamic_tool_handler_timeout"},
            )
            return _tool_failure("handler_timeout")
        except BaseException as exc:
            logger.exception(
                "Dynamic tool handler failed",
                exc_info=(type(exc), exc, exc.__traceback__),
                extra={"stable_code": "dynamic_tool_handler_failed"},
            )
            return _tool_failure("internal_error")

    async def _invoke_dynamic_tool(
        self,
        call: DynamicToolCall,
    ) -> dict[str, object]:
        return await self._dynamic_tool_handler(call)

    def _dynamic_call(self, params: JsonObject | None) -> DynamicToolCall:
        if params is None:
            raise ValueError("dynamic tool params are missing")
        thread_id = _bounded_identity(params.get("threadId"))
        turn_id = _bounded_identity(params.get("turnId"))
        call_id = _bounded_identity(params.get("callId"))
        tool = _bounded_identity(params.get("tool"))
        namespace_value = params.get("namespace")
        if namespace_value is not None and not isinstance(namespace_value, str):
            raise TypeError("dynamic tool namespace is invalid")
        namespace = namespace_value if namespace_value else None
        local_turn_id = self._resolve_local_turn(thread_id, turn_id)
        if local_turn_id is None:
            raise ValueError("dynamic tool call has no active local Turn")
        arguments = cast(object, params.get("arguments"))
        return DynamicToolCall(
            runtime_generation=self._generation,
            local_turn_id=local_turn_id,
            provider_thread_id=thread_id,
            provider_turn_id=turn_id,
            provider_call_id=call_id,
            namespace=namespace,
            tool=tool,
            arguments=arguments,
            observed_image_paths=self._observed_paths_for_call(
                turn_id,
                arguments,
            ),
        )


@dataclass(slots=True)
class DynamicAsyncThread:
    codex: DynamicAsyncCodex
    id: str

    async def read(self, *, include_turns: bool = False) -> ThreadReadResponse:
        return await asyncio.to_thread(
            self.codex._sync_client.thread_read,
            self.id,
            include_turns,
        )

    async def set_name(self, name: str) -> ThreadSetNameResponse:
        return await asyncio.to_thread(
            self.codex._sync_client.thread_set_name,
            self.id,
            name,
        )

    async def compact(self) -> ThreadCompactStartResponse:
        return await asyncio.to_thread(
            self.codex._sync_client.thread_compact,
            self.id,
        )

    async def turn(
        self,
        input: Sequence[InputItem],
        *,
        local_turn_id: str,
        approval_mode: ApprovalMode | None,
        cwd: str | None,
        effort: ReasoningEffort | None,
        model: str | None,
        output_schema: JsonObject | None,
        personality: Personality | None,
        sandbox: Sandbox | None,
        service_tier: str | None,
        summary: ReasoningSummary | None,
    ) -> DynamicAsyncTurnHandle:
        wire_input = _wire_input(input)
        approval_policy, reviewer = _approval_settings(approval_mode)
        params = TurnStartParams(
            thread_id=self.id,
            input=cast(Any, wire_input),
            approval_policy=approval_policy,
            approvals_reviewer=reviewer,
            cwd=cwd,
            effort=effort,
            model=model,
            output_schema=output_schema,
            personality=personality,
            sandbox_policy=_sandbox_policy(sandbox),
            service_tier=service_tier,
            summary=summary,
        )
        provider_turn_id = await asyncio.to_thread(
            self.codex._start_turn_sync,
            local_turn_id=local_turn_id,
            thread_id=self.id,
            wire_input=wire_input,
            params=params,
        )
        return DynamicAsyncTurnHandle(self.codex, self.id, provider_turn_id)


@dataclass(slots=True)
class DynamicAsyncTurnHandle:
    codex: DynamicAsyncCodex
    thread_id: str
    id: str

    async def steer(self, input: InputItem) -> TurnSteerResponse:
        return await asyncio.to_thread(
            self.codex._sync_client.turn_steer,
            self.thread_id,
            self.id,
            _wire_input((input,)),
        )

    async def interrupt(self) -> TurnInterruptResponse:
        return await asyncio.to_thread(
            self.codex._sync_client.turn_interrupt,
            self.thread_id,
            self.id,
        )

    async def stream(self) -> AsyncIterator[Notification]:
        self.codex._sync_client.register_turn_notifications(self.id)
        try:
            while True:
                event = await asyncio.to_thread(
                    self.codex._sync_client.next_turn_notification,
                    self.id,
                )
                yield event
                if (
                    event.method == "turn/completed"
                    and isinstance(event.payload, TurnCompletedNotification)
                    and event.payload.turn.id == self.id
                ):
                    break
        finally:
            self.codex._sync_client.unregister_turn_notifications(self.id)
            self.codex._forget_turn(self.id)


def _validated_tool_spec(value: JsonObject) -> JsonObject:
    model = DynamicToolSpec.model_validate(value)
    return cast(
        JsonObject,
        model.model_dump(mode="json", by_alias=True, exclude_none=True),
    )


def _model_payload(value: Any) -> JsonObject:
    return cast(
        JsonObject,
        value.model_dump(mode="json", by_alias=True, exclude_none=True),
    )


def _approval_settings(
    mode: ApprovalMode | None,
) -> tuple[AskForApproval | None, ApprovalsReviewer | None]:
    if mode is None:
        return None, None
    if mode is ApprovalMode.auto_review:
        return (
            AskForApproval(root=AskForApprovalValue.on_request),
            ApprovalsReviewer.auto_review,
        )
    if mode is ApprovalMode.deny_all:
        return AskForApproval(root=AskForApprovalValue.never), None
    raise ValueError("unsupported Codex approval mode")


def _sandbox_mode(value: Sandbox | None) -> SandboxMode | None:
    return {
        Sandbox.read_only: SandboxMode.read_only,
        Sandbox.workspace_write: SandboxMode.workspace_write,
        Sandbox.full_access: SandboxMode.danger_full_access,
        None: None,
    }[value]


def _sandbox_policy(value: Sandbox | None) -> SandboxPolicy | None:
    if value is Sandbox.read_only:
        return SandboxPolicy(root=ReadOnlySandboxPolicy(type="readOnly"))
    if value is Sandbox.workspace_write:
        return SandboxPolicy(root=WorkspaceWriteSandboxPolicy(type="workspaceWrite"))
    if value is Sandbox.full_access:
        return SandboxPolicy(
            root=DangerFullAccessSandboxPolicy(type="dangerFullAccess")
        )
    if value is None:
        return None
    raise ValueError("unsupported Codex sandbox")


def _wire_input(items: Sequence[InputItem]) -> list[JsonObject]:
    wire: list[JsonObject] = []
    for item in items:
        if isinstance(item, TextInput):
            wire.append({"type": "text", "text": item.text})
        elif isinstance(item, ImageInput):
            wire.append({"type": "image", "url": item.url})
        elif isinstance(item, LocalImageInput):
            wire.append({"type": "localImage", "path": item.path})
        elif isinstance(item, SkillInput):
            wire.append({"type": "skill", "name": item.name, "path": item.path})
        elif isinstance(item, MentionInput):
            wire.append({"type": "mention", "name": item.name, "path": item.path})
        else:
            raise TypeError(f"unsupported Codex input item: {type(item).__name__}")
    return wire


def _bounded_identity(value: JsonValue | None) -> str:
    if not isinstance(value, str) or not value or len(value) > 256:
        raise ValueError("dynamic tool identity is invalid")
    return value


def _validated_tool_response(response: dict[str, object]) -> JsonObject:
    success = response.get("success")
    content_items = response.get("contentItems")
    if not isinstance(success, bool) or not isinstance(content_items, list):
        raise ValueError("dynamic tool response shape is invalid")
    for item in content_items:
        if not isinstance(item, dict):
            raise ValueError("dynamic tool content item is invalid")
        kind = item.get("type")
        if kind == "inputText":
            if set(item) != {"type", "text"} or not isinstance(item.get("text"), str):
                raise ValueError("dynamic tool text result is invalid")
        elif kind == "inputImage":
            if set(item) != {"type", "imageUrl"} or not isinstance(
                item.get("imageUrl"), str
            ):
                raise ValueError("dynamic tool image result is invalid")
        else:
            raise ValueError("dynamic tool content type is invalid")
    encoded = json.dumps(response, ensure_ascii=False, separators=(",", ":")).encode()
    if len(encoded) > _MAX_TOOL_RESPONSE_BYTES:
        raise ValueError("dynamic tool response is too large")
    return cast(JsonObject, response)


def _tool_failure(code: str) -> JsonObject:
    return {
        "success": False,
        "contentItems": [
            {
                "type": "inputText",
                "text": canonical_json(
                    {
                        "status": "error",
                        "code": code,
                        "confirmation_required": False,
                    }
                ),
            }
        ],
    }
