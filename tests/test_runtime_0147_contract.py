"""Real app-server contracts using an isolated, deterministic local provider.

No account credentials, billable requests, or Discord writes. The local SSE
provider tests runtime protocol behavior, not production model quality.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import importlib.metadata
import io
import json
import os
import secrets
import shlex
import sys
import threading
import zipfile
from contextlib import AsyncExitStack
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
from conftest import StorageContext
from openai_codex import ApprovalMode, CodexConfig, LocalImageInput, Sandbox, TextInput
from PIL import Image

from codexd.application.attachment_materializer import AttachmentMaterializer
from codexd.application.dynamic_tools import CODEXD_DYNAMIC_TOOLS
from codexd.domain.conversations import SandboxProfile, ThreadConfig, ThreadIdentity, TurnConfig
from codexd.domain.turns import TurnFile, TurnInput, TurnSource
from codexd.runtime.app_server import DynamicAsyncCodex
from codexd.runtime.codex_sdk import (
    CodexSDKRuntime,
    _initialized_runtime_version,
    capability_manifest,
)
from codexd.runtime.port import DynamicToolCall, RuntimeSlotConfig
from codexd.security import private_files


@pytest.mark.asyncio
@pytest.mark.parametrize("archive", [False, True])
async def test_real_0147_dynamic_tools_and_thread_lifecycle(
    tmp_path: Path,
    storage_context: StorageContext,
    archive: bool,
) -> None:
    assert importlib.metadata.version("openai-codex") == "0.147.0"
    assert importlib.metadata.version("openai-codex-cli-bin") == "0.147.0"
    requests: list[dict[str, Any]] = []
    calls: list[DynamicToolCall] = []
    errors: list[str] = []
    marker = "CONTRACT_" + secrets.token_hex(16)
    content = marker.encode()
    if archive:
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w") as zipped:
            zipped.writestr("marker.txt", content)
        content = buffer.getvalue()
    data_root = storage_context.store.path.parent
    for folder in (data_root, data_root / "attachments", data_root / "attachments" / "input"):
        private_files.ensure_private_directory(folder)
    source_path = data_root / "attachments" / "input" / ("source.zip" if archive else "source.txt")
    source_path.write_bytes(content)
    private_files.secure_private_file(source_path)
    source = TurnFile(
        attachment_id="fixture",
        ordinal=0,
        canonical_path=source_path.resolve(),
        display_name=source_path.name,
        reported_media_type="application/zip" if archive else "text/plain",
        sha256=hashlib.sha256(content).hexdigest(),
        size_bytes=len(content),
        retention_until=9_999_999_999_999,
    )
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(files=(source,)),
        input_message_id="contract-file",
    )
    materialized = AttachmentMaterializer(
        store=storage_context.store,
        data_root=storage_context.store.path.parent,
    ).materialize(turn_id=turn.id, files=(source,))
    image_path = tmp_path / "image.png"
    Image.new("RGB", (16, 16), "red").save(image_path)
    release_response = threading.Event()
    hold_response = threading.Event()
    response_waiting = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *_args: object) -> None:
            pass

        def do_POST(self) -> None:
            try:
                request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
                requests.append(request)
                index = len(requests)
                if hold_response.is_set():
                    response_waiting.set()
                    release_response.wait(timeout=10)
                if index <= 2:
                    tool = "schedule_create" if index == 1 else "publish_image"
                    arguments = (
                        {
                            "name": "contract",
                            "kind": "cron",
                            "expression": "0 9 * * *",
                            "timezone": "UTC",
                            "misfire_policy": "skip",
                            "prompt": "test",
                        }
                        if index == 1
                        else {
                            "source_path": str(tmp_path / "image.png"),
                            "display_name": "contract.png",
                            "description": "test",
                        }
                    )
                    item = {
                        "type": "function_call",
                        "id": f"fc_{index}",
                        "call_id": f"call_{index}",
                        "name": tool,
                        "namespace": "codexd",
                        "arguments": json.dumps(arguments),
                        "status": "completed",
                    }
                elif index == 3:
                    # The actual runtime executes a real read against daemon materialization.
                    root = str(storage_context.store.path.parent / "attachments" / "materialized")
                    script = (
                        "from pathlib import Path; "
                        f"print(''.join(p.read_text() for p in Path({root!r}).rglob('*') "
                        "if p.is_file() and p.suffix == '.txt'))"
                    )
                    encoded = base64.b64encode(script.encode()).decode()
                    portable_script = f"import base64;exec(base64.b64decode('{encoded}'))"
                    command = (
                        f'& "{sys.executable}" -c "{portable_script}"'
                        if os.name == "nt"
                        else f"{shlex.quote(sys.executable)} -c {shlex.quote(script)}"
                    )
                    available = {
                        tool.get("name") for tool in request.get("tools", [])
                        if tool.get("type") == "function"
                    }
                    command_tool = (
                        "exec_command" if "exec_command" in available else "shell_command"
                    )
                    arguments = (
                        {"cmd": command} if command_tool == "exec_command"
                        else {"command": command}
                    )
                    item = {
                        "type": "function_call",
                        "id": "read-file",
                        "call_id": "read-file",
                        "name": command_tool,
                        "arguments": json.dumps(arguments),
                        "status": "completed",
                    }
                else:
                    item = {
                        "type": "message",
                        "id": f"msg_{index}",
                        "role": "assistant",
                        "status": "completed",
                        "content": [
                            {"type": "output_text", "text": "CONTRACT_OK", "annotations": []}
                        ],
                    }
                events = [
                    {"type": "response.created", "response": {"id": f"r_{index}", "output": []}},
                    {"type": "response.output_item.added", "output_index": 0, "item": item},
                    {"type": "response.output_item.done", "output_index": 0, "item": item},
                    {
                        "type": "response.completed",
                        "response": {
                            "id": f"r_{index}",
                            "status": "completed",
                            "output": [item],
                            "usage": {"input_tokens": 10, "output_tokens": 5, "total_tokens": 15},
                        },
                    },
                ]
                body = "".join(
                    f"event: {event['type']}\ndata: {json.dumps(event)}\n\n" for event in events
                ).encode()
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
            except Exception as exc:
                errors.append(type(exc).__name__)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    home = tmp_path / "home"
    home.mkdir()
    overrides = (
        'model="contract-model"',
        'model_provider="contract"',
        'model_providers.contract.name="Local contract"',
        f'model_providers.contract.base_url="http://127.0.0.1:{server.server_port}/v1"',
        'model_providers.contract.wire_api="responses"',
        "model_providers.contract.requires_openai_auth=false",
        f"sqlite_home={json.dumps(str(tmp_path / 'sqlite'))}",
    )

    async def handler(call: DynamicToolCall) -> dict[str, object]:
        calls.append(call)
        return {
            "success": True,
            "contentItems": [{"type": "inputText", "text": "fixture accepted"}],
        }

    config = CodexConfig(
        cwd=str(tmp_path),
        env={"CODEX_HOME": str(home)},
        config_overrides=overrides,
        experimental_api=True,
    )
    try:
        client = DynamicAsyncCodex(
            config,
            generation=7,
            dynamic_tools=CODEXD_DYNAMIC_TOOLS,
            dynamic_tool_handler=handler,
        )
        async with asyncio.timeout(30), AsyncExitStack() as stack:
            stack.push_async_callback(client.close)
            await client.__aenter__()
            assert _initialized_runtime_version(client) == "0.147.0"
            assert client.metadata.serverInfo is not None
            effective = await client.read_config(str(tmp_path))
            assert effective.config.model == "contract-model"
            thread = await client.thread_start(
                approval_mode=ApprovalMode.deny_all,
                config=None,
                cwd=str(tmp_path),
                ephemeral=False,
                model=None,
                personality=None,
                sandbox=Sandbox.full_access,
                service_tier=None,
            )
            assert client.settings[thread.id].values["model"] == "contract-model"
            assert client.settings[thread.id].source in {"thread/start", "thread/settings/updated"}
            handle = await thread.turn(
                [
                    TextInput("Run the two contract tools and read the attachment."),
                    TextInput(materialized.context),
                    LocalImageInput(str(image_path)),
                ],
                local_turn_id="local-contract",
                approval_mode=ApprovalMode.deny_all,
                cwd=str(tmp_path),
                effort=None,
                model=None,
                output_schema=None,
                personality=None,
                sandbox=Sandbox.full_access,
                service_tier=None,
                summary=None,
            )
            events = [event async for event in handle.stream()]
            assert events[-1].payload.turn.status.value == "completed"
            assert client.settings[thread.id].source in {"thread/start", "thread/settings/updated"}
            if client.settings[thread.id].source == "thread/start":
                assert client.settings[thread.id].turn_id is None
                assert client.settings[thread.id].values["reasoning_summary"] is None
            assert [call.tool for call in calls] == ["schedule_create", "publish_image"], requests[
                0
            ].get("tools")
            assert all(
                call.local_turn_id == "local-contract"
                and call.provider_thread_id == thread.id
                and call.provider_turn_id == handle.id
                for call in calls
            )
            assert not errors
            assert len({call.provider_call_id for call in calls}) == 2
            assert marker not in json.dumps(requests[0])
            assert marker in json.dumps(requests[-1]), [
                item for item in requests[-1].get("input", [])
                if item.get("type") == "function_call_output"
            ]
            assert "input_image" in json.dumps(requests[0])
            resumed = await client.thread_resume(
                thread.id,
                approval_mode=ApprovalMode.deny_all,
                config=None,
                cwd=str(tmp_path),
                model=None,
                personality=None,
                sandbox=Sandbox.read_only,
                service_tier=None,
            )
            assert resumed.id == thread.id
            for _ in range(100):
                if client.settings[thread.id].source == "thread/settings/updated":
                    break
                await asyncio.sleep(0.01)
            assert client.settings[thread.id].values["model"] == "contract-model"
            assert client._settings_task is not None and not client._settings_task.done()
            # #48: simulate an already-completed Turn without settings notification.
            # The real execution-time resume must recover model evidence before image send.
            from dataclasses import replace
            from types import SimpleNamespace
            from unittest.mock import AsyncMock

            from test_runtime_turns import _model

            from codexd.application.turn_coordinator import TurnCoordinator
            from codexd.domain.models import ModelCatalogSnapshot

            client.settings[thread.id] = replace(client.settings[thread.id], fresh=False)
            main_snapshot = await thread.read()
            revision = storage_context.repository.activate_thread_revision(
                conversation_id=storage_context.conversation.id,
                identity=ThreadIdentity(
                    thread.id, None, main_snapshot.thread.session_id, None, None, "0.147.0"
                ),
                config=ThreadConfig(None, None, SandboxProfile.FULL_ACCESS),
            )
            runtime48 = CodexSDKRuntime(
                client=client,
                generation=7,
                slot=RuntimeSlotConfig(
                    "project", "fixture", tmp_path, home, {}, "fixture", "project_scoped"
                ),
                manifest=capability_manifest(),
            )
            runtime48._threads[thread.id] = thread
            runtime48.list_models = AsyncMock(
                return_value=ModelCatalogSnapshot(
                    (
                        _model("global-a", is_default=True, modalities=("text",)),
                        _model("contract-model", is_default=False, modalities=("text", "image")),
                    ),
                    True,
                    None,
                )
            )
            coordinator48 = SimpleNamespace(
                _repository=storage_context.repository, _validate_revision_identity=AsyncMock()
            )
            queued = storage_context.repository.get_turn(turn.id)
            descriptor = await TurnCoordinator._validate_turn_catalog(
                coordinator48,
                runtime48,
                turn=queued,
                turn_input=SimpleNamespace(images=(image_path,)),
            )
            assert descriptor.model == "contract-model"
            coordinator48._validate_revision_identity.assert_awaited_once()
            image_turn = await thread.turn(
                [LocalImageInput(str(image_path))],
                local_turn_id="image48",
                approval_mode=ApprovalMode.deny_all,
                cwd=str(tmp_path),
                effort=None,
                model=descriptor.model,
                output_schema=None,
                personality=None,
                sandbox=Sandbox.read_only,
                service_tier=None,
                summary=None,
            )
            image_events = [event async for event in image_turn.stream()]
            assert image_events[-1].payload.turn.status.value == "completed"
            assert requests[-1]["model"] == "contract-model"
            assert "input_image" in json.dumps(requests[-1])
            assert (
                storage_context.repository.get_active_revision(queued.conversation_id).id
                == revision.id
            )
            side = await client.thread_fork(
                thread.id,
                approval_mode=ApprovalMode.deny_all,
                config=None,
                cwd=str(tmp_path),
                ephemeral=True,
                model=None,
                sandbox=Sandbox.read_only,
                service_tier=None,
            )
            snapshot = await side.read()
            assert snapshot.thread.ephemeral is True
            assert snapshot.thread.id != thread.id
            # Ephemeral forks in 0.147 omit persisted ancestry metadata.
            assert snapshot.thread.forked_from_id is None
            hold_response.set()
            side_turn = await side.turn(
                [TextInput("temporary side question")],
                local_turn_id="side-contract",
                approval_mode=ApprovalMode.deny_all,
                cwd=str(tmp_path),
                effort=None,
                model=None,
                output_schema=None,
                personality=None,
                sandbox=Sandbox.read_only,
                service_tier=None,
                summary=None,
            )
            assert await asyncio.to_thread(response_waiting.wait, 5)
            await side_turn.steer(TextInput("additional side context"))
            await side_turn.interrupt()
            release_response.set()
            side_events = [event async for event in side_turn.stream()]
            assert side_events[-1].payload.turn.status.value == "interrupted"
            assert await client.thread_unsubscribe(side.id) == "unsubscribed"
            hold_response.clear()
            runtime = CodexSDKRuntime(
                client=client,
                generation=7,
                slot=RuntimeSlotConfig(
                    "project", "fixture", tmp_path, home, {}, "fixture", "project_scoped"
                ),
                manifest=capability_manifest(schedule_tool_enabled=True),
            )
            main_snapshot = await thread.read()
            query = await runtime.start_side_query(
                local_query_id="adapter-side",
                source_thread=ThreadIdentity(
                    thread.id,
                    None,
                    main_snapshot.thread.session_id,
                    None,
                    None,
                    "0.147.0",
                ),
                question="Read-only side question",
                cwd=tmp_path,
                thread_config=ThreadConfig(
                    model=None, personality=None, sandbox=SandboxProfile.READ_ONLY
                ),
                turn_config=TurnConfig(cwd=tmp_path, sandbox=SandboxProfile.READ_ONLY),
            )
            query_events = [event async for event in query.stream]
            assert any(event.kind == "turn.completed" for event in query_events)
            await runtime.close_side_query(query.identity)
            assert runtime._side_threads == {}
            await thread.compact()
            await asyncio.to_thread(client._sync_client.thread_archive, thread.id)
            restored = await asyncio.to_thread(client._sync_client.thread_unarchive, thread.id)
            assert restored.thread.id == thread.id
        from codexd.runtime.observing_client import ObservingAsyncCodex

        async with ObservingAsyncCodex(config) as public:
            observed_runtime = CodexSDKRuntime(
                client=public,
                generation=8,
                slot=RuntimeSlotConfig(
                    "project", "fixture", tmp_path, home, {}, "fixture", "project_scoped"
                ),
                manifest=capability_manifest(),
            )
            public_thread = await public.thread_start(
                model=None,
                cwd=str(tmp_path),
                ephemeral=True,
                sandbox=Sandbox.read_only,
            )
            assert observed_runtime._settings[public_thread.id].values["model"] == "contract-model"
            assert observed_runtime._settings[public_thread.id].source == "thread/start"
    finally:
        release_response.set()
        server.shutdown()
        server.server_close()
        worker.join(timeout=2)
