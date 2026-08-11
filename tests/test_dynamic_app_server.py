from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any

import pytest
from openai_codex import ApprovalMode, CodexConfig, Sandbox

from codexd.application.dynamic_tools import CODEXD_DYNAMIC_TOOLS
from codexd.runtime import app_server, codex_sdk
from codexd.runtime.app_server import DynamicAsyncCodex
from codexd.runtime.port import DynamicToolCall


class _LowLevelClient:
    def __init__(
        self,
        config: CodexConfig,
        approval_handler: Any,
    ) -> None:
        self.config = config
        self.approval_handler = approval_handler
        self.thread_start_params: dict[str, object] | None = None

    def thread_start(self, params: dict[str, object]) -> object:
        self.thread_start_params = params
        return SimpleNamespace(thread=SimpleNamespace(id="created-thread"))


@pytest.mark.asyncio
async def test_low_level_facade_registers_exact_dynamic_tool_schema(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clients: list[_LowLevelClient] = []

    class CaptureClient(_LowLevelClient):
        def __init__(self, config: CodexConfig, approval_handler: Any) -> None:
            super().__init__(config, approval_handler)
            clients.append(self)

    monkeypatch.setattr(app_server, "CodexClient", CaptureClient)

    async def handler(_call: DynamicToolCall) -> dict[str, object]:
        return {"success": True, "contentItems": []}

    facade = DynamicAsyncCodex(
        CodexConfig(experimental_api=True),
        generation=7,
        dynamic_tools=CODEXD_DYNAMIC_TOOLS,
        dynamic_tool_handler=handler,
    )
    thread = await facade.thread_start(
        approval_mode=ApprovalMode.auto_review,
        config={"web_search": "cached"},
        cwd="/project",
        ephemeral=False,
        model="gpt-5",
        personality=None,
        sandbox=Sandbox.full_access,
        service_tier=None,
    )

    assert thread.id == "created-thread"
    params = clients[0].thread_start_params
    assert params is not None
    assert params["dynamicTools"] == list(CODEXD_DYNAMIC_TOOLS)
    assert params["approvalPolicy"] == "on-request"
    assert params["approvalsReviewer"] == "auto_review"
    assert params["sandbox"] == "danger-full-access"
    assert clients[0].config.experimental_api is True


@pytest.mark.asyncio
async def test_low_level_facade_routes_tool_request_without_breaking_approvals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clients: list[_LowLevelClient] = []

    class CaptureClient(_LowLevelClient):
        def __init__(self, config: CodexConfig, approval_handler: Any) -> None:
            super().__init__(config, approval_handler)
            clients.append(self)

    monkeypatch.setattr(app_server, "CodexClient", CaptureClient)
    seen: list[DynamicToolCall] = []

    async def handler(call: DynamicToolCall) -> dict[str, object]:
        seen.append(call)
        return {
            "success": True,
            "contentItems": [
                {
                    "type": "inputText",
                    "text": json.dumps({"status": "confirmation_required"}),
                }
            ],
        }

    facade = DynamicAsyncCodex(
        CodexConfig(experimental_api=True),
        generation=9,
        dynamic_tools=CODEXD_DYNAMIC_TOOLS,
        dynamic_tool_handler=handler,
    )
    facade._loop = asyncio.get_running_loop()
    facade._turn_routes["provider-turn"] = ("provider-thread", "local-turn")
    facade.observe_image_path("provider-turn", "/tmp/generated.png")
    approval_handler = clients[0].approval_handler

    assert approval_handler(
        "item/commandExecution/requestApproval", {"itemId": "command"}
    ) == {"decision": "accept"}
    assert approval_handler("item/fileChange/requestApproval", {"itemId": "patch"}) == {
        "decision": "accept"
    }
    assert approval_handler("unknown/request", {}) == {}

    response = await asyncio.to_thread(
        approval_handler,
        "item/tool/call",
        {
            "threadId": "provider-thread",
            "turnId": "provider-turn",
            "callId": "provider-call",
            "namespace": "codexd",
            "tool": "schedule_create",
            "arguments": {"name": "daily"},
        },
    )

    assert response["success"] is True
    assert len(seen) == 1
    assert seen[0] == DynamicToolCall(
        runtime_generation=9,
        local_turn_id="local-turn",
        provider_thread_id="provider-thread",
        provider_turn_id="provider-turn",
        provider_call_id="provider-call",
        namespace="codexd",
        tool="schedule_create",
        arguments={"name": "daily"},
        observed_image_paths=("/tmp/generated.png",),
    )
    assert facade._turn_routes["provider-turn"] == (
        "provider-thread",
        "local-turn",
    )


@pytest.mark.asyncio
async def test_low_level_facade_fails_closed_for_unroutable_tool_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clients: list[_LowLevelClient] = []

    class CaptureClient(_LowLevelClient):
        def __init__(self, config: CodexConfig, approval_handler: Any) -> None:
            super().__init__(config, approval_handler)
            clients.append(self)

    monkeypatch.setattr(app_server, "CodexClient", CaptureClient)

    async def handler(_call: DynamicToolCall) -> dict[str, object]:
        raise AssertionError("unroutable calls must not reach the application")

    facade = DynamicAsyncCodex(
        CodexConfig(experimental_api=True),
        generation=1,
        dynamic_tools=CODEXD_DYNAMIC_TOOLS,
        dynamic_tool_handler=handler,
    )
    facade._loop = asyncio.get_running_loop()
    response = await asyncio.to_thread(
        clients[0].approval_handler,
        "item/tool/call",
        {
            "threadId": "unknown-thread",
            "turnId": "unknown-turn",
            "callId": "unknown-call",
            "namespace": "codexd",
            "tool": "schedule_create",
            "arguments": {},
        },
    )

    assert response["success"] is False
    item = response["contentItems"][0]
    assert json.loads(item["text"])["code"] == "invalid_call_identity"


def test_dynamic_tool_capability_is_exact_version_and_product_gated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    supported = codex_sdk.capability_manifest(schedule_tool_enabled=True)
    assert supported.optional["dynamic_tool.call"] is True
    assert supported.optional["codexd.schedule_create_tool"] is True
    assert supported.optional["codexd.publish_image_tool"] is False
    publish_supported = codex_sdk.capability_manifest(
        schedule_tool_enabled=True,
        publish_image_enabled=True,
    )
    assert publish_supported.optional["codexd.publish_image_tool"] is True

    def version(_distribution: str) -> str:
        return "0.144.5"

    monkeypatch.setattr(codex_sdk.importlib.metadata, "version", version)
    unverified = codex_sdk.capability_manifest(schedule_tool_enabled=True)
    assert unverified.optional["dynamic_tool.call"] is False
    assert unverified.optional["codexd.schedule_create_tool"] is False
    assert unverified.optional["codexd.publish_image_tool"] is False


def test_missing_low_level_handler_degrades_only_dynamic_tool(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class IncompatibleLowLevelClient:
        pass

    monkeypatch.setattr(codex_sdk, "CodexClient", IncompatibleLowLevelClient)

    manifest = codex_sdk.capability_manifest(schedule_tool_enabled=True)

    manifest.assert_required()
    assert manifest.optional["dynamic_tool.call"] is False
    assert manifest.optional["codexd.schedule_create_tool"] is False
    assert manifest.optional["codexd.publish_image_tool"] is False
