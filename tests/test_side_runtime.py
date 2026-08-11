from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest
from openai_codex import ApprovalMode, Sandbox

from codexd.domain.conversations import (
    SandboxProfile,
    ThreadConfig,
    ThreadIdentity,
    TurnConfig,
)
from codexd.runtime import codex_sdk
from codexd.runtime.codex_sdk import (
    CodexSDKRuntime,
    _capability_manifest,
    _configured_mcp_servers,
)
from codexd.runtime.port import RuntimeSlotConfig


class _SideHandle:
    id = "side-provider-turn"
    thread_id = "side-provider-thread"

    def __init__(self) -> None:
        self.interrupted = False

    async def interrupt(self) -> None:
        self.interrupted = True


class _SideThread:
    id = "side-provider-thread"

    def __init__(self, handle: _SideHandle) -> None:
        self.handle = handle
        self.turn_input: object | None = None
        self.turn_options: dict[str, object] | None = None

    async def read(self, *, include_turns: bool = False) -> object:
        assert include_turns is False
        return SimpleNamespace(
            thread=SimpleNamespace(
                id=self.id,
                session_id="main-provider-session",
                forked_from_id="main-provider-thread",
                parent_thread_id=None,
                cli_version="0.144.4",
                ephemeral=True,
            )
        )

    async def turn(self, input: object, **kwargs: object) -> _SideHandle:
        self.turn_input = input
        self.turn_options = kwargs
        return self.handle


class _SideClient:
    def __init__(self) -> None:
        self.handle = _SideHandle()
        self.thread = _SideThread(self.handle)
        self.fork_options: dict[str, object] | None = None
        self.unsubscribed: list[str] = []

    async def thread_fork(self, thread_id: str, **kwargs: object) -> _SideThread:
        assert thread_id == "main-provider-thread"
        self.fork_options = kwargs
        return self.thread

    async def thread_unsubscribe(self, thread_id: str) -> str:
        self.unsubscribed.append(thread_id)
        return "unsubscribed"


def test_side_query_detects_nested_mcp_configuration(tmp_path: Path) -> None:
    codex_home = tmp_path / "codex-home"
    codex_home.mkdir()
    (codex_home / "config.toml").write_text(
        '[profiles.work.mcp_servers.writer]\ncommand = "writer"\n',
        encoding="utf-8",
    )
    slot = RuntimeSlotConfig(
        scope_kind="project",
        project_id="project",
        cwd=tmp_path,
        codex_home=codex_home,
        environment={},
        environment_hash="environment",
        topology_contract="project_scoped",
    )

    assert _configured_mcp_servers(slot)


@pytest.mark.asyncio
async def test_runtime_side_query_is_ephemeral_read_only_and_unsubscribed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    codex_home = tmp_path / "codex-home"
    codex_home.mkdir()
    slot = RuntimeSlotConfig(
        scope_kind="project",
        project_id="project",
        cwd=tmp_path,
        codex_home=codex_home,
        environment={},
        environment_hash="environment",
        topology_contract="project_scoped",
    )
    client = _SideClient()
    monkeypatch.setattr(codex_sdk, "DynamicAsyncCodex", _SideClient)
    runtime = CodexSDKRuntime(
        client=cast(Any, client),
        slot=slot,
        generation=1,
        manifest=_capability_manifest(),
    )
    source = ThreadIdentity(
        thread_id="main-provider-thread",
        requested_thread_id="main-provider-thread",
        provider_session_id="main-provider-session",
        forked_from_thread_id=None,
        parent_thread_id=None,
        provider_version="0.144.4",
    )

    started = await runtime.start_side_query(
        local_query_id="query-id",
        source_thread=source,
        question="Why this approach?",
        cwd=tmp_path,
        thread_config=ThreadConfig(
            model="gpt-5",
            personality="pragmatic",
            sandbox=SandboxProfile.READ_ONLY,
            service_tier="fast",
        ),
        turn_config=TurnConfig(
            cwd=tmp_path,
            sandbox=SandboxProfile.READ_ONLY,
            model="gpt-5",
            reasoning_effort="high",
            reasoning_summary="concise",
            personality="pragmatic",
            service_tier="fast",
        ),
    )

    assert client.fork_options is not None
    assert client.fork_options["ephemeral"] is True
    assert client.fork_options["approval_mode"] is ApprovalMode.deny_all
    assert client.fork_options["sandbox"] is Sandbox.read_only
    assert "temporary read-only side question" in str(
        client.fork_options["developer_instructions"]
    )
    assert client.thread.turn_options is not None
    assert client.thread.turn_options["approval_mode"] is ApprovalMode.deny_all
    assert client.thread.turn_options["sandbox"] is Sandbox.read_only
    assert runtime._threads == {}
    assert started.identity.side_thread_id == "side-provider-thread"

    await runtime.interrupt_side_query(started.identity)
    await runtime.close_side_query(started.identity)

    assert client.handle.interrupted
    assert client.unsubscribed == ["side-provider-thread"]
    assert runtime._side_threads == {}
    assert runtime._side_turn_handles == {}
