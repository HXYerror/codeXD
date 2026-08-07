from __future__ import annotations

import asyncio
import hashlib
import importlib.metadata
import os
from collections.abc import AsyncIterator
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, Mock

import openai_codex
import pytest
from openai_codex import (
    CodexConfig,
    CodexError,
    InternalRpcError,
    InvalidParamsError,
    InvalidRequestError,
    JsonRpcError,
    LocalImageInput,
    MentionInput,
    MethodNotFoundError,
    ParseError,
    RetryLimitExceededError,
    ServerBusyError,
    SkillInput,
    TextInput,
    TransportClosedError,
)
from openai_codex.generated.v2_all import (
    ReasoningSummaryPartAddedNotification,
    Turn,
    TurnCompletedNotification,
)
from openai_codex.models import Notification, UnknownNotification
from openai_codex.types import TurnStatus

from codexd.domain.capabilities import CapabilityManifest
from codexd.domain.conversations import (
    ApprovalPolicy,
    SandboxProfile,
    ThreadConfig,
    ThreadIdentity,
    TurnConfig,
)
from codexd.domain.turns import TurnFile, TurnImage, TurnInput, TurnSkill
from codexd.errors import AttachmentIntegrityError, InvariantError
from codexd.runtime import codex_sdk
from codexd.runtime.codex_sdk import (
    CodexSDKRuntime,
    _adapter_error,
    _assert_notification_route,
    _capability_manifest,
    _initialized_runtime_version,
    _normalize_notification,
    _sdk_config,
    _sdk_environment,
    _verify_public_contract,
)
from codexd.runtime.errors import (
    AdapterError,
    AdapterInvariantError,
    FileInputUnsupported,
    InterruptFailed,
    ProviderOutcomeUnknown,
    ProviderRateLimited,
    ProviderRejected,
    RuntimeUnavailable,
    UnsupportedCapability,
)
from codexd.runtime.port import RuntimeSlotConfig
from codexd.security import private_files


def test_official_sdk_public_contract_and_required_manifest() -> None:
    _verify_public_contract()
    manifest = _capability_manifest()

    manifest.assert_required()
    assert manifest.adapter == "openai_codex"
    assert "local_path" in manifest.image_input_modes
    assert manifest.optional["mention.input"] is (
        manifest.sdk_version == "0.144.4"
        and codex_sdk._file_input_leasing_supported()
    )


@pytest.mark.asyncio
async def test_sdk_0144_4_public_mention_constructor_and_exact_wire_contract() -> None:
    assert openai_codex.MentionInput is MentionInput
    assert codex_sdk._mention_input_contract_supported("0.144.4")
    assert not codex_sdk._mention_input_contract_supported("0.144.5")

    class CaptureWireClient:
        def __init__(self) -> None:
            self.input: list[dict[str, object]] | None = None

        async def turn_start(
            self,
            thread_id: str,
            input: list[dict[str, object]],
            *,
            params: object,
        ) -> SimpleNamespace:
            del params
            assert thread_id == "thread"
            self.input = input
            return SimpleNamespace(turn=SimpleNamespace(id="turn"))

    class CaptureCodex:
        def __init__(self) -> None:
            self._client = CaptureWireClient()

        async def _ensure_initialized(self) -> None:
            return None

    client = CaptureCodex()
    mention = MentionInput("资料.md", "/validated/opaque.bin")
    handle = await openai_codex.AsyncThread(cast(Any, client), "thread").turn(mention)

    assert (mention.name, mention.path) == ("资料.md", "/validated/opaque.bin")
    assert handle.id == "turn"
    assert client._client.input == [
        {
            "type": "mention",
            "name": "资料.md",
            "path": "/validated/opaque.bin",
        }
    ]


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="secure file leasing requires POSIX")
async def test_runtime_maps_mixed_attachments_by_ordinal_without_prompt_downgrade(
    tmp_path: Path,
) -> None:
    capture = _InputCaptureThread()
    runtime = _runtime_for_input_capture(tmp_path, capture)
    skill = TurnSkill("review", tmp_path / "SKILL.md", "skill-hash")
    images = (
        _turn_image(tmp_path / "late.png", ordinal=3, attachment_id="late-image"),
        _turn_image(tmp_path / "middle.png", ordinal=1, attachment_id="middle-image"),
    )
    files = (
        _turn_file(tmp_path / "late.bin", ordinal=2, display_name="late.pdf"),
        _turn_file(tmp_path / "first.bin", ordinal=0, display_name="资料.txt"),
    )

    started = await runtime.start_turn(
        local_turn_id="local",
        thread=ThreadIdentity("thread", None, "session", None, None, "test"),
        input=TurnInput(
            text="inspect attachments",
            images=images,
            files=files,
            skill_inputs=(skill,),
        ),
        config=_turn_config(tmp_path),
    )

    assert capture.inputs == [
        [
            TextInput("inspect attachments"),
            SkillInput("review", str(tmp_path / "SKILL.md")),
            MentionInput("资料.txt", str(files[1].canonical_path)),
            LocalImageInput(str(tmp_path / "middle.png")),
            MentionInput("late.pdf", str(files[0].canonical_path)),
            LocalImageInput(str(tmp_path / "late.png")),
        ]
    ]
    with pytest.raises(RuntimeUnavailable):
        async for _event in started.stream:
            pass


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="secure file leasing requires POSIX")
async def test_runtime_starts_file_only_turn_with_one_mention(tmp_path: Path) -> None:
    capture = _InputCaptureThread()
    runtime = _runtime_for_input_capture(tmp_path, capture)
    file = _turn_file(tmp_path / "only.bin", ordinal=0, display_name="only.zip")

    started = await runtime.start_turn(
        local_turn_id="local",
        thread=ThreadIdentity("thread", None, "session", None, None, "test"),
        input=TurnInput(files=(file,)),
        config=_turn_config(tmp_path),
    )

    assert capture.inputs == [[MentionInput("only.zip", str(file.canonical_path))]]
    with pytest.raises(RuntimeUnavailable):
        async for _event in started.stream:
            pass


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="secure file leasing requires POSIX")
async def test_runtime_final_file_lease_rejects_named_path_replacement(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capture = _InputCaptureThread()
    runtime = _runtime_for_input_capture(tmp_path, capture)
    file = _turn_file(tmp_path / "leased.bin", ordinal=0, display_name="leased.txt")
    original_read = os.read
    replaced = False

    def replace_during_hash(descriptor: int, size: int) -> bytes:
        nonlocal replaced
        chunk = original_read(descriptor, size)
        if chunk and not replaced:
            replaced = True
            replacement = file.canonical_path.with_name("replacement.bin")
            replacement.write_bytes(file.canonical_path.read_bytes())
            replacement.chmod(0o600)
            os.replace(replacement, file.canonical_path)
        return chunk

    monkeypatch.setattr(codex_sdk.os, "read", replace_during_hash)

    with pytest.raises(AttachmentIntegrityError) as failure:
        await runtime.start_turn(
            local_turn_id="local",
            thread=ThreadIdentity("thread", None, "session", None, None, "test"),
            input=TurnInput(files=(file,)),
            config=_turn_config(tmp_path),
        )

    assert failure.value.code == "attachment_integrity_failed"
    assert file.attachment_id in str(failure.value)
    assert str(file.canonical_path) not in str(failure.value)
    assert capture.inputs == []


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="secure file leasing requires POSIX")
async def test_runtime_file_lease_rejects_symlinked_private_parent(
    tmp_path: Path,
) -> None:
    capture = _InputCaptureThread()
    runtime = _runtime_for_input_capture(tmp_path, capture)
    file = _turn_file(tmp_path / "leased.bin", ordinal=0, display_name="leased.txt")
    input_root = file.canonical_path.parent
    moved_root = input_root.with_name("moved-input")
    input_root.rename(moved_root)
    input_root.symlink_to(moved_root, target_is_directory=True)

    with pytest.raises(AttachmentIntegrityError):
        await runtime.start_turn(
            local_turn_id="local",
            thread=ThreadIdentity("thread", None, "session", None, None, "test"),
            input=TurnInput(files=(file,)),
            config=_turn_config(tmp_path),
        )

    assert capture.inputs == []


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="secure file leasing requires POSIX")
async def test_runtime_holds_file_lease_until_stream_finally(tmp_path: Path) -> None:
    capture = _InputCaptureThread()
    runtime = _runtime_for_input_capture(tmp_path, capture)
    file = _turn_file(tmp_path / "leased.bin", ordinal=0, display_name="leased.txt")

    started = await runtime.start_turn(
        local_turn_id="local",
        thread=ThreadIdentity("thread", None, "session", None, None, "test"),
        input=TurnInput(files=(file,)),
        config=_turn_config(tmp_path),
    )
    descriptor = runtime._file_input_leases["turn"].descriptors[0]
    os.fstat(descriptor)
    _assert_exclusive_lock(file.canonical_path, available=False)

    with pytest.raises(RuntimeUnavailable, match="terminal event"):
        async for _event in started.stream:
            pass

    assert runtime._file_input_leases == {}
    with pytest.raises(OSError):
        os.fstat(descriptor)
    _assert_exclusive_lock(file.canonical_path, available=True)


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="secure file leasing requires POSIX")
async def test_runtime_releases_file_lease_before_yielding_terminal_event(
    tmp_path: Path,
) -> None:
    notification = Notification(
        "turn/completed",
        TurnCompletedNotification(
            thread_id="thread",
            turn=Turn(id="turn", items=[], status=TurnStatus.completed),
        ),
    )
    runtime = _runtime_for_notifications(tmp_path, (notification,))
    file = _turn_file(tmp_path / "leased.bin", ordinal=0, display_name="leased.txt")
    started = await runtime.start_turn(
        local_turn_id="local",
        thread=ThreadIdentity("thread", None, "session", None, None, "test"),
        input=TurnInput(files=(file,)),
        config=_turn_config(tmp_path),
    )
    descriptor = runtime._file_input_leases["turn"].descriptors[0]
    stream = started.stream.__aiter__()

    event = await anext(stream)

    assert event.kind == "turn.completed"
    assert runtime._file_input_leases == {}
    with pytest.raises(OSError):
        os.fstat(descriptor)
    close = getattr(stream, "aclose", None)
    if callable(close):
        await close()


@pytest.mark.asyncio
@pytest.mark.skipif(os.name != "posix", reason="secure file leasing requires POSIX")
async def test_runtime_releases_file_lease_on_start_failure_and_close(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    file = _turn_file(tmp_path / "leased.bin", ordinal=0, display_name="leased.txt")
    acquired: list[int] = []
    original_acquire = codex_sdk._acquire_file_input_lease

    def record_acquire(value: TurnFile) -> int:
        descriptor = original_acquire(value)
        acquired.append(descriptor)
        return descriptor

    class FailingThread:
        async def turn(self, *_args: object, **_kwargs: object) -> object:
            raise CodexError("start failed")

    monkeypatch.setattr(codex_sdk, "_acquire_file_input_lease", record_acquire)
    failing = _runtime_for_input_capture(tmp_path, cast(Any, FailingThread()))
    with pytest.raises(AdapterError):
        await failing.start_turn(
            local_turn_id="failed",
            thread=ThreadIdentity("thread", None, "session", None, None, "test"),
            input=TurnInput(files=(file,)),
            config=_turn_config(tmp_path),
        )
    assert acquired
    with pytest.raises(OSError):
        os.fstat(acquired.pop())
    _assert_exclusive_lock(file.canonical_path, available=True)

    capture = _InputCaptureThread()
    runtime = _runtime_for_input_capture(tmp_path, capture)
    runtime._client = cast(Any, SimpleNamespace(close=AsyncMock()))
    await runtime.start_turn(
        local_turn_id="closing",
        thread=ThreadIdentity("thread", None, "session", None, None, "test"),
        input=TurnInput(files=(file,)),
        config=_turn_config(tmp_path),
    )
    descriptor = runtime._file_input_leases["turn"].descriptors[0]

    await runtime.close()

    with pytest.raises(OSError):
        os.fstat(descriptor)
    _assert_exclusive_lock(file.canonical_path, available=True)


@pytest.mark.asyncio
async def test_runtime_rejects_files_before_provider_when_mention_capability_is_missing(
    tmp_path: Path,
) -> None:
    capture = _InputCaptureThread()
    manifest = _capability_manifest()
    unsupported = replace(
        manifest,
        optional={**manifest.optional, "mention.input": False},
    )
    runtime = CodexSDKRuntime(
        client=cast(Any, object()),
        slot=_runtime_slot(tmp_path),
        generation=7,
        manifest=unsupported,
    )
    runtime._threads["thread"] = cast(Any, capture)
    file = _turn_file(
        tmp_path / "private-location.bin",
        ordinal=0,
        display_name="safe.txt",
    )

    with pytest.raises(FileInputUnsupported) as error:
        await runtime.start_turn(
            local_turn_id="local",
            thread=ThreadIdentity("thread", None, "session", None, None, "test"),
            input=TurnInput(files=(file,)),
            config=_turn_config(tmp_path),
        )

    assert error.value.code == "file_input_unsupported"
    assert error.value.failure.code == "file_input_unsupported"
    assert not error.value.failure.retryable
    assert str(file.canonical_path) not in error.value.failure.message
    assert capture.inputs == []


@pytest.mark.asyncio
async def test_cancelled_runtime_startup_retains_cleanup_ownership() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    closed = asyncio.Event()
    client = Mock()

    async def close() -> None:
        closed.set()
        release.set()

    client.close = AsyncMock(side_effect=close)

    async def enter() -> None:
        started.set()
        await release.wait()

    enter_task = asyncio.create_task(enter())
    await started.wait()
    codex_sdk._schedule_cancelled_startup_cleanup(client, enter_task)
    cleanup = next(iter(codex_sdk._PENDING_STARTUP_CLEANUPS))

    await asyncio.wait_for(cleanup, timeout=1)

    assert closed.is_set()
    assert client.close.await_count == 2
    assert not codex_sdk._PENDING_STARTUP_CLEANUPS


@pytest.mark.asyncio
async def test_cancelled_runtime_startup_cleanup_has_hard_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    process = Mock()
    client = SimpleNamespace(
        close=AsyncMock(),
        _client=SimpleNamespace(_sync=SimpleNamespace(_proc=process)),
    )

    async def enter() -> None:
        started.set()
        await release.wait()

    monkeypatch.setattr(
        codex_sdk,
        "_CANCELLED_STARTUP_CLEANUP_TIMEOUT_SECONDS",
        0.01,
    )
    enter_task = asyncio.create_task(enter())
    await started.wait()
    codex_sdk._schedule_cancelled_startup_cleanup(client, enter_task)
    cleanup = next(iter(codex_sdk._PENDING_STARTUP_CLEANUPS))

    await asyncio.wait_for(cleanup, timeout=1)

    assert enter_task.cancelled()
    process.kill.assert_called_once_with()
    assert not codex_sdk._PENDING_STARTUP_CLEANUPS


def test_runtime_version_comes_from_initialized_server_metadata() -> None:
    client = SimpleNamespace(
        metadata=SimpleNamespace(
            serverInfo=SimpleNamespace(
                version="0.146.0 (Mac OS; arm64) unknown (codex_python_sdk; 0.144.4)"
            )
        )
    )

    assert _initialized_runtime_version(cast(Any, client)) == "0.146.0"
    assert (
        codex_sdk.capability_manifest(runtime_version="0.146.0").runtime_version
        == "0.146.0"
    )


def test_runtime_version_must_be_stable_semantic() -> None:
    with pytest.raises(AdapterInvariantError) as error:
        codex_sdk.capability_manifest(runtime_version="codex-dev")

    assert error.value.failure.code == "runtime_version_invalid"


def test_runtime_slot_can_select_an_explicit_codex_binary(tmp_path: Path) -> None:
    slot = _runtime_slot(tmp_path)
    selected = tmp_path / "codex"
    slot = RuntimeSlotConfig(
        scope_kind=slot.scope_kind,
        project_id=slot.project_id,
        cwd=slot.cwd,
        codex_home=slot.codex_home,
        environment=slot.environment,
        environment_hash=slot.environment_hash,
        topology_contract=slot.topology_contract,
        codex_bin=selected,
    )

    config = _sdk_config(slot, generation=4)

    assert config.codex_bin == str(selected)


def test_known_turn_notification_is_normalized() -> None:
    payload = TurnCompletedNotification(
        thread_id="thread",
        turn=Turn(
            id="provider-turn",
            items=[],
            status=TurnStatus.completed,
        ),
    )

    event = _normalize_notification(
        Notification("turn/completed", payload), cwd=Path("/")
    )

    assert event.kind == "turn.completed"
    assert event.payload["provider_turn_id"] == "provider-turn"


def test_unknown_notification_is_hashed_not_persisted_raw() -> None:
    event = _normalize_notification(
        Notification(
            "future/notification",
            UnknownNotification({"sensitive": "do-not-persist"}),
        ),
        cwd=Path("/"),
    )

    assert event.kind == "provider.unknown"
    assert "do-not-persist" not in str(event.payload)
    assert event.raw_hash is not None
    assert event.raw_size is not None


@pytest.mark.parametrize(
    "payload",
    [
        UnknownNotification(
            {
                "threadId": "thread",
                "turnId": "turn",
                "itemId": "reasoning-item",
                "summaryIndex": 2,
            }
        ),
        ReasoningSummaryPartAddedNotification(
            thread_id="thread",
            turn_id="turn",
            item_id="reasoning-item",
            summary_index=2,
        ),
    ],
    ids=["unknown-sdk-payload", "typed-sdk-payload"],
)
def test_summary_part_notification_is_normalized_and_routed(payload: Any) -> None:
    notification = Notification(
        "item/reasoning/summaryPartAdded",
        payload,
    )

    _assert_notification_route(
        notification,
        expected_thread_id="thread",
        expected_turn_id="turn",
        generation=3,
    )
    event = _normalize_notification(notification, cwd=Path("/"))

    assert event.kind == "reasoning.summary_part.added"
    assert event.payload == {
        "item_id": "reasoning-item",
        "summary_index": 2,
    }
    assert event.raw_hash is None

    with pytest.raises(AdapterInvariantError):
        _assert_notification_route(
            notification,
            expected_thread_id="another-thread",
            expected_turn_id="turn",
            generation=3,
        )


@pytest.mark.parametrize(
    ("method", "params", "expected_kind"),
    [
        (
            "item/commandExecution/terminalInteraction",
            {
                "threadId": "thread",
                "turnId": "turn",
                "itemId": "command",
                "processId": "private-process",
                "stdin": "sk-0123456789abcdef0123456789abcdef",
            },
            "terminal.interaction",
        ),
        (
            "item/fileChange/patchUpdated",
            {
                "threadId": "thread",
                "turnId": "turn",
                "itemId": "patch",
                "changes": [
                    {
                        "path": "/private/project/secret.py",
                        "kind": {"type": "update"},
                        "diff": "+token=sk-0123456789abcdef0123456789abcdef",
                    }
                ],
            },
            "file_change.patch.updated",
        ),
        (
            "item/mcpToolCall/progress",
            {
                "threadId": "thread",
                "turnId": "turn",
                "itemId": "mcp",
                "message": "sk-0123456789abcdef0123456789abcdef",
            },
            "mcp.progress",
        ),
        (
            "hook/started",
            {
                "threadId": "thread",
                "turnId": "turn",
                "run": {
                    "id": "private-hook",
                    "eventName": "preToolUse",
                    "executionMode": "sync",
                    "handlerType": "command",
                    "scope": "turn",
                    "status": "running",
                    "entries": [{"text": "private", "kind": "context"}],
                    "sourcePath": "/private/hook.py",
                },
            },
            "hook.started",
        ),
        (
            "hook/completed",
            {
                "threadId": "thread",
                "turnId": "turn",
                "run": {
                    "id": "private-hook",
                    "eventName": "preToolUse",
                    "executionMode": "sync",
                    "handlerType": "command",
                    "scope": "turn",
                    "status": "completed",
                    "entries": [{"text": "private", "kind": "context"}],
                    "sourcePath": "/private/hook.py",
                },
            },
            "hook.completed",
        ),
        (
            "item/autoApprovalReview/started",
            {
                "threadId": "thread",
                "turnId": "turn",
                "reviewId": "private-review",
                "targetItemId": "command",
                "review": {
                    "riskLevel": "high",
                    "status": "inProgress",
                    "rationale": "private rationale",
                },
                "action": {
                    "root": {
                        "type": "command",
                        "command": "private command",
                        "cwd": "/private",
                    }
                },
            },
            "approval_review.started",
        ),
        (
            "item/autoApprovalReview/completed",
            {
                "threadId": "thread",
                "turnId": "turn",
                "reviewId": "private-review",
                "targetItemId": "command",
                "review": {
                    "riskLevel": "high",
                    "status": "approved",
                    "rationale": "private rationale",
                },
                "action": {
                    "root": {
                        "type": "command",
                        "command": "private command",
                        "cwd": "/private",
                    }
                },
                "decisionSource": {"root": "agent"},
            },
            "approval_review.completed",
        ),
        (
            "model/rerouted",
            {
                "threadId": "thread",
                "turnId": "turn",
                "fromModel": "model-a",
                "toModel": "model-b",
                "reason": {"root": "highRiskCyberActivity"},
            },
            "model.rerouted",
        ),
        (
            "model/safetyBuffering/updated",
            {
                "threadId": "thread",
                "turnId": "turn",
                "model": "model-b",
                "fasterModel": "model-fast",
                "showBufferingUi": True,
                "reasons": ["policy"],
                "useCases": ["verification"],
            },
            "model.safety",
        ),
        (
            "model/verification",
            {
                "threadId": "thread",
                "turnId": "turn",
                "verifications": [{"root": "trustedAccessForCyber"}],
            },
            "model.verification",
        ),
        (
            "turn/moderationMetadata",
            {
                "threadId": "thread",
                "turnId": "turn",
                "metadata": {
                    "private": "sk-0123456789abcdef0123456789abcdef"
                },
            },
            "turn.moderation",
        ),
        (
            "thread/compacted",
            {"threadId": "thread", "turnId": "turn"},
            "context_compaction.completed",
        ),
        (
            "thread/goal/updated",
            {
                "threadId": "thread",
                "turnId": "turn",
                "goal": {
                    "objective": "sk-0123456789abcdef0123456789abcdef",
                    "status": "active",
                    "timeUsedSeconds": 1,
                    "tokenBudget": 100,
                    "tokensUsed": 10,
                },
            },
            "thread_goal.updated",
        ),
        (
            "thread/goal/cleared",
            {"threadId": "thread"},
            "thread_goal.cleared",
        ),
    ],
)
def test_routed_notification_families_are_typed_safe_and_route_checked(
    method: str,
    params: dict[str, Any],
    expected_kind: str,
) -> None:
    notification = Notification(method, UnknownNotification(params))

    _assert_notification_route(
        notification,
        expected_thread_id="thread",
        expected_turn_id="turn",
        generation=3,
    )
    event = _normalize_notification(notification, cwd=Path("/private/project"))

    assert event.kind == expected_kind
    assert event.kind != "provider.unknown"
    assert "sk-0123456789abcdef0123456789abcdef" not in str(event.payload)
    assert "private rationale" not in str(event.payload)
    assert "private command" not in str(event.payload)
    assert "/private/hook.py" not in str(event.payload)

    mismatched = dict(params)
    mismatched["threadId"] = "another-thread"
    with pytest.raises(AdapterInvariantError):
        _assert_notification_route(
            Notification(method, UnknownNotification(mismatched)),
            expected_thread_id="thread",
            expected_turn_id="turn",
            generation=3,
        )


def test_image_view_item_is_normalized_without_exposing_its_path(
    tmp_path: Path,
) -> None:
    event = _normalize_notification(
        cast(
            Any,
            SimpleNamespace(
                method="item/completed",
                payload=SimpleNamespace(
                    item=SimpleNamespace(
                        root=SimpleNamespace(
                            type="imageView",
                            id="image-view",
                            path=SimpleNamespace(root=str(tmp_path / "image.png")),
                        )
                    )
                ),
            ),
        ),
        cwd=tmp_path,
    )

    assert event.kind == "image_view.completed"
    assert event.payload["item_id"] == "image-view"
    assert str(tmp_path) not in str(event.payload)
    assert event.raw_hash is None


@pytest.mark.parametrize(
    ("item", "expected_kind"),
    [
        (
            SimpleNamespace(
                type="sleep",
                id="sleep-item",
                duration_ms=250,
            ),
            "sleep.completed",
        ),
        (
            SimpleNamespace(
                type="enteredReviewMode",
                id="review-enter",
                review="private review instructions",
            ),
            "review_mode.entered",
        ),
        (
            SimpleNamespace(
                type="exitedReviewMode",
                id="review-exit",
                review="private review result",
            ),
            "review_mode.exited",
        ),
        (
            SimpleNamespace(
                type="imageGeneration",
                id="generated-image",
                result="provider-private-result",
                revised_prompt="private revised prompt",
                saved_path=SimpleNamespace(root="/private/generated.png"),
                status="completed",
            ),
            "image_generation.completed",
        ),
    ],
)
def test_documented_optional_items_have_typed_safe_events(
    tmp_path: Path,
    item: object,
    expected_kind: str,
) -> None:
    event = _normalize_notification(
        cast(
            Any,
            SimpleNamespace(
                method="item/completed",
                payload=SimpleNamespace(item=SimpleNamespace(root=item)),
            ),
        ),
        cwd=tmp_path,
    )

    assert event.kind == expected_kind
    serialized = str(event.payload)
    assert "private review" not in serialized
    assert "provider-private-result" not in serialized
    assert "private revised prompt" not in serialized
    assert "/private/generated.png" not in serialized


@pytest.mark.parametrize(
    ("item_type", "collection_name", "count_name", "hash_name"),
    [
        ("hookPrompt", "fragments", "fragment_count", "fragment_hash"),
        ("userMessage", "content", "content_count", "content_hash"),
    ],
)
def test_provider_input_items_retain_only_count_and_hash(
    item_type: str,
    collection_name: str,
    count_name: str,
    hash_name: str,
) -> None:
    private = "sk-0123456789abcdef0123456789abcdef"

    def dump_private(**_kwargs: object) -> dict[str, str]:
        return {"text": private}

    item = SimpleNamespace(
        type=item_type,
        id=f"{item_type}-item",
        **{collection_name: [SimpleNamespace(model_dump=dump_private)]},
    )
    event = _normalize_notification(
        cast(
            Any,
            SimpleNamespace(
                method="item/completed",
                payload=SimpleNamespace(item=SimpleNamespace(root=item)),
            ),
        ),
        cwd=Path("/"),
    )

    assert event.kind == "provider_input.completed"
    assert event.payload[count_name] == 1
    assert event.payload[hash_name] == hashlib.sha256(
        f'[{{"text":"{private}"}}]'.encode()
    ).hexdigest()
    assert private not in str(event.payload)


def test_subagent_activity_only_persists_agent_path_hash_and_size(
    tmp_path: Path,
) -> None:
    agent_path = str(tmp_path / "private" / "agent-state.json")
    event = _normalize_notification(
        cast(
            Any,
            SimpleNamespace(
                method="item/completed",
                payload=SimpleNamespace(
                    item=SimpleNamespace(
                        root=SimpleNamespace(
                            type="subAgentActivity",
                            id="activity-item",
                            kind="started",
                            agent_thread_id="provider-agent-thread",
                            agent_path=agent_path,
                        )
                    )
                ),
            ),
        ),
        cwd=tmp_path,
    )

    assert event.kind == "collaboration.completed"
    assert event.payload["agent_path_hash"] == hashlib.sha256(
        agent_path.encode()
    ).hexdigest()
    assert event.payload["agent_path_size"] == len(agent_path.encode())
    assert "agent_path" not in event.payload
    assert agent_path not in str(event.payload)


@pytest.mark.asyncio
async def test_subagent_activity_enriches_safe_detail_once_per_agent(
    tmp_path: Path,
) -> None:
    secret = "sk-0123456789abcdef0123456789abcdef"

    class SubagentReadClient:
        def __init__(self) -> None:
            self._client = self
            self.read_calls = 0

        async def _ensure_initialized(self) -> None:
            return None

        async def thread_read(
            self,
            thread_id: str,
            *,
            include_turns: bool,
        ) -> SimpleNamespace:
            assert thread_id == "provider-agent-thread"
            assert not include_turns
            self.read_calls += 1
            return SimpleNamespace(
                thread=SimpleNamespace(
                    id=thread_id,
                    session_id="session",
                    parent_thread_id="thread",
                    agent_role="reviewer",
                    preview=f"Review {tmp_path / 'private.py'} using {secret}",
                    name=None,
                )
            )

    item = SimpleNamespace(
        type="subAgentActivity",
        id="activity-item",
        kind="started",
        agent_thread_id="provider-agent-thread",
        agent_path=str(tmp_path / "private" / "agent-state.json"),
    )
    notifications = tuple(
        cast(
            Notification,
            SimpleNamespace(
                method=f"item/{suffix}",
                payload=SimpleNamespace(
                    thread_id="thread",
                    turn_id="turn",
                    item=SimpleNamespace(root=item),
                ),
            ),
        )
        for suffix in ("started", "completed")
    )
    client = SubagentReadClient()
    runtime = CodexSDKRuntime(
        client=cast(Any, client),
        slot=_runtime_slot(tmp_path),
        generation=1,
        manifest=_capability_manifest(),
    )
    runtime._threads["thread"] = cast(
        Any,
        _NotificationThread(_NotificationHandle(notifications)),
    )
    started = await runtime.start_turn(
        local_turn_id="local",
        thread=ThreadIdentity("thread", None, "session", None, None, "test"),
        input=TurnInput(text="hello"),
        config=TurnConfig(
            cwd=tmp_path,
            sandbox=SandboxProfile.READ_ONLY,
            approval_mode=ApprovalPolicy.AUTO_REVIEW,
        ),
    )
    stream = started.stream.__aiter__()
    events = [await anext(stream), await anext(stream)]
    await stream.aclose()

    assert client.read_calls == 1
    for event in events:
        assert event.payload["agent_role"] == "reviewer"
        assert "private.py" in event.payload["activity_summary"]
        assert secret not in str(event.payload)
        assert str(tmp_path) not in str(event.payload)
        assert "provider-agent-thread" not in str(event.payload)


def test_normalized_provider_content_is_redacted_at_adapter_boundary(
    tmp_path: Path,
) -> None:
    secret = "sk-0123456789abcdef0123456789abcdef"

    def secret_dump(**_kwargs: object) -> dict[str, str]:
        return {"detail": secret}

    notifications = (
        SimpleNamespace(
            method="item/completed",
            payload=SimpleNamespace(
                item=SimpleNamespace(
                    root=SimpleNamespace(
                        type="agentMessage",
                        id="assistant",
                        text=f"answer {secret}",
                        phase="final_answer",
                    )
                )
            ),
        ),
        SimpleNamespace(
            method="item/completed",
            payload=SimpleNamespace(
                item=SimpleNamespace(
                    root=SimpleNamespace(
                        type="plan",
                        id="plan",
                        text=f"plan {secret}",
                    )
                )
            ),
        ),
        SimpleNamespace(
            method="item/commandExecution/outputDelta",
            payload=SimpleNamespace(item_id="command", delta=f"output {secret}"),
        ),
        SimpleNamespace(
            method="turn/diff/updated",
            payload=SimpleNamespace(diff=f"+token={secret}"),
        ),
        SimpleNamespace(
            method="turn/plan/updated",
            payload=SimpleNamespace(
                explanation=f"explain {secret}",
                plan=[SimpleNamespace(model_dump=secret_dump)],
            ),
        ),
        SimpleNamespace(
            method="item/completed",
            payload=SimpleNamespace(
                item=SimpleNamespace(
                    root=SimpleNamespace(
                        type="webSearch",
                        id="search",
                        query=f"query {secret}",
                        action=SimpleNamespace(model_dump=secret_dump),
                    )
                )
            ),
        ),
    )

    for notification in notifications:
        event = _normalize_notification(cast(Any, notification), cwd=tmp_path)
        assert secret not in str(event.payload)
        assert "<redacted>" in str(event.payload)


def test_completed_assistant_message_is_not_silently_truncated(tmp_path: Path) -> None:
    text = "长答案😀" * 10_000
    notification = SimpleNamespace(
        method="item/completed",
        payload=SimpleNamespace(
            item=SimpleNamespace(
                root=SimpleNamespace(
                    type="agentMessage",
                    id="assistant-long",
                    text=text,
                    phase="final_answer",
                )
            )
        ),
    )

    event = _normalize_notification(cast(Any, notification), cwd=tmp_path)

    assert event.payload["text"] == text


def test_assistant_stream_delta_is_not_silently_truncated(tmp_path: Path) -> None:
    text = "流式内容😀" * 10_000
    notification = SimpleNamespace(
        method="item/agentMessage/delta",
        payload=SimpleNamespace(item_id="assistant-long", delta=text),
    )

    event = _normalize_notification(cast(Any, notification), cwd=tmp_path)

    assert event.payload["text"] == text


def test_usage_token_counts_are_not_mistaken_for_credentials(tmp_path: Path) -> None:
    usage = {
        "last": {
            "input_tokens": 100,
            "output_tokens": 20,
            "cached_input_tokens": 10,
            "reasoning_output_tokens": 5,
            "total_tokens": 120,
        },
        "total": {
            "input_tokens": 1_000,
            "output_tokens": 200,
            "cached_input_tokens": 100,
            "reasoning_output_tokens": 50,
            "total_tokens": 1_200,
        },
        "model_context_window": 1_000_000,
    }
    event = _normalize_notification(
        cast(
            Any,
            SimpleNamespace(
                method="thread/tokenUsage/updated",
                payload=SimpleNamespace(
                    token_usage=SimpleNamespace(
                        model_dump=lambda **_kwargs: usage,
                    )
                ),
            ),
        ),
        cwd=tmp_path,
    )

    assert event.kind == "usage.updated"
    assert event.payload == usage


@pytest.mark.asyncio
async def test_explicit_no_auth_custom_provider_bypasses_openai_login_gate(
    tmp_path: Path,
) -> None:
    codex_home = tmp_path / "codex-home"
    codex_home.mkdir()
    (codex_home / "config.toml").write_text(
        "\n".join(
            (
                'model_provider = "proxy"',
                "",
                "[model_providers.proxy]",
                'name = "Local proxy"',
                'base_url = "http://localhost:4142/v1"',
                'wire_api = "responses"',
                "requires_openai_auth = false",
            )
        ),
        encoding="utf-8",
    )

    class AccountClient:
        async def account(self, *, refresh_token: bool) -> SimpleNamespace:
            assert not refresh_token
            return SimpleNamespace(account=None, requires_openai_auth=True)

    slot = _runtime_slot(tmp_path)
    runtime = CodexSDKRuntime(
        client=cast(Any, AccountClient()),
        slot=slot,
        generation=1,
        manifest=_capability_manifest(),
    )

    status = await runtime.account_status()

    assert not status.auth_required


def test_required_manifest_rejects_missing_canonical_key() -> None:
    manifest = _capability_manifest()
    required = dict(manifest.required)
    required.pop("turn.interrupt")
    incomplete = CapabilityManifest(
        adapter=manifest.adapter,
        sdk_version=manifest.sdk_version,
        runtime_version=manifest.runtime_version,
        compatibility=manifest.compatibility,
        image_input_modes=manifest.image_input_modes,
        required=required,
        optional=manifest.optional,
    )

    with pytest.raises(InvariantError, match=r"turn\.interrupt"):
        incomplete.assert_required()


def test_sdk_version_gate_enforces_patch_floor(monkeypatch: pytest.MonkeyPatch) -> None:
    def version(distribution: str) -> str:
        return "0.144.3" if distribution == "openai-codex" else "0.144.4"

    monkeypatch.setattr(importlib.metadata, "version", version)

    with pytest.raises(AdapterInvariantError) as error:
        codex_sdk.capability_manifest()
    assert error.value.failure.code == "sdk_version_out_of_range"


def test_in_range_patch_uses_verified_public_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def version(_distribution: str) -> str:
        return "0.144.5"

    monkeypatch.setattr(importlib.metadata, "version", version)
    manifest = codex_sdk.capability_manifest()

    manifest.assert_required()
    assert manifest.compatibility.matrix_tier == "compatible_patch"
    assert manifest.optional["thread.compact"] is True
    assert manifest.optional["thread.archive"] is False
    assert manifest.optional["thread.unarchive"] is False
    assert manifest.optional["mention.input"] is False


def test_mention_capability_rejects_an_incompatible_public_constructor(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class IncompatibleMentionInput:
        def __init__(self, resource: str) -> None:
            self.resource = resource

    monkeypatch.setattr(openai_codex, "MentionInput", IncompatibleMentionInput)

    assert not codex_sdk._mention_input_contract_supported("0.144.4")
    assert codex_sdk.capability_manifest().optional["mention.input"] is False


def test_windows_file_lease_facade_disables_mention_capability(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(private_files, "_platform_name", lambda: "nt")

    manifest = codex_sdk.capability_manifest()

    assert manifest.optional["mention.input"] is False


@pytest.mark.skipif(os.name != "nt", reason="requires native Windows semantics")
def test_windows_runtime_reports_file_input_unsupported_without_handle_contract() -> None:
    assert codex_sdk.capability_manifest().optional["mention.input"] is False


@pytest.mark.asyncio
async def test_missing_optional_mention_export_keeps_text_and_image_turns_working(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delattr(openai_codex, "MentionInput")
    manifest = codex_sdk.capability_manifest()
    assert manifest.optional["mention.input"] is False

    capture = _InputCaptureThread()
    runtime = CodexSDKRuntime(
        client=cast(Any, object()),
        slot=_runtime_slot(tmp_path),
        generation=1,
        manifest=manifest,
    )
    runtime._threads["thread"] = cast(Any, capture)
    image = _turn_image(tmp_path / "image.png", ordinal=0, attachment_id="image")
    await runtime.start_turn(
        local_turn_id="text-image",
        thread=ThreadIdentity("thread", None, "session", None, None, "test"),
        input=TurnInput(text="inspect", images=(image,)),
        config=_turn_config(tmp_path),
    )
    assert capture.inputs == [
        [TextInput("inspect"), LocalImageInput(str(image.canonical_path))]
    ]

    file = _turn_file(tmp_path / "file.bin", ordinal=0, display_name="file.txt")
    with pytest.raises(FileInputUnsupported):
        await runtime.start_turn(
            local_turn_id="file",
            thread=ThreadIdentity("thread", None, "session", None, None, "test"),
            input=TurnInput(files=(file,)),
            config=_turn_config(tmp_path),
        )
    assert len(capture.inputs) == 1


def test_codex_home_is_propagated_and_conflicts_fail(tmp_path: Path) -> None:
    codex_home = tmp_path / "codex-home"
    slot = RuntimeSlotConfig(
        scope_kind="project",
        project_id="project",
        cwd=tmp_path,
        codex_home=codex_home,
        environment={},
        environment_hash="environment",
        topology_contract="project_scoped",
    )

    environment = _sdk_environment(slot, generation=7)
    assert environment["CODEX_HOME"] == str(codex_home.resolve())

    conflicting = RuntimeSlotConfig(
        scope_kind=slot.scope_kind,
        project_id=slot.project_id,
        cwd=slot.cwd,
        codex_home=slot.codex_home,
        environment={"CODEX_HOME": str(tmp_path / "other-home")},
        environment_hash=slot.environment_hash,
        topology_contract=slot.topology_contract,
    )
    with pytest.raises(AdapterInvariantError) as error:
        _sdk_environment(conflicting, generation=7)
    assert error.value.failure.code == "codex_home_conflict"


@pytest.mark.asyncio
async def test_post_handshake_validation_failure_closes_client(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    clients: list[_CaptureClient] = []

    class CaptureClient(_CaptureClient):
        def __init__(self, config: CodexConfig) -> None:
            super().__init__(config)
            clients.append(self)

    monkeypatch.setattr(codex_sdk, "AsyncCodex", CaptureClient)
    monkeypatch.setattr(codex_sdk, "_verify_public_contract", lambda: None)

    def fail_manifest(*, runtime_version: str | None = None) -> CapabilityManifest:
        assert runtime_version == "0.144.4"
        raise RuntimeError("manifest failed")

    monkeypatch.setattr(codex_sdk, "capability_manifest", fail_manifest)
    slot = _runtime_slot(tmp_path)

    with pytest.raises(RuntimeError, match="manifest failed"):
        await CodexSDKRuntime.create(slot=slot, generation=1)
    assert clients[0].closed


@pytest.mark.asyncio
async def test_post_mutation_identity_read_is_reconciled_without_replay(
    tmp_path: Path,
) -> None:
    thread = _MutationThread(identity_failures=1)
    client = _MutationClient(thread)
    runtime = _runtime_for_mutations(tmp_path, client)

    identity = await runtime.start_thread(
        cwd=tmp_path,
        config=_thread_config(),
    )

    assert identity.thread_id == thread.id
    assert thread.names == ["codexD session"]
    assert thread.read_calls == 2
    assert client.mutation_calls == ["start"]


@pytest.mark.asyncio
async def test_initial_thread_persistence_failure_is_outcome_unknown(
    tmp_path: Path,
) -> None:
    class PersistenceFailureThread(_MutationThread):
        async def set_name(self, name: str) -> None:
            raise TransportClosedError(f"lost while persisting {name}")

    thread = PersistenceFailureThread(identity_failures=0)
    client = _MutationClient(thread)
    runtime = _runtime_for_mutations(tmp_path, client)

    with pytest.raises(ProviderOutcomeUnknown) as error:
        await runtime.start_thread(cwd=tmp_path, config=_thread_config())

    assert error.value.failure.thread_id == thread.id
    assert error.value.failure.provider_exception == "TransportClosedError"
    assert thread.id not in runtime._threads


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["start", "fork", "unarchive"])
async def test_post_mutation_identity_read_failure_is_outcome_unknown(
    tmp_path: Path,
    operation: str,
) -> None:
    thread = _MutationThread(identity_failures=2)
    client = _MutationClient(thread)
    runtime = _runtime_for_mutations(tmp_path, client)

    with pytest.raises(ProviderOutcomeUnknown) as error:
        if operation == "start":
            await runtime.start_thread(cwd=tmp_path, config=_thread_config())
        elif operation == "fork":
            await runtime.fork_thread(
                thread_id="source-thread",
                cwd=tmp_path,
                config=_thread_config(),
            )
        else:
            await runtime.unarchive_thread("archived-thread")

    assert error.value.failure.code == "provider_effect_outcome_unknown"
    assert error.value.failure.thread_id == thread.id
    assert thread.read_calls == 2
    assert client.mutation_calls == [operation]
    assert client.resume_calls == []


@pytest.mark.asyncio
async def test_unarchive_uses_the_returned_runtime_handle(
    tmp_path: Path,
) -> None:
    unarchived = _MutationThread(identity_failures=0)
    client = _MutationClient(unarchived)
    runtime = _runtime_for_mutations(tmp_path, client)

    identity = await runtime.unarchive_thread("archived-thread")

    assert identity.thread_id == unarchived.id
    assert client.mutation_calls == ["unarchive"]
    assert client.resume_calls == []
    assert runtime._threads[unarchived.id] is unarchived
    assert unarchived.read_calls == 1


def test_typed_terminal_route_mismatch_is_rejected() -> None:
    notification = Notification(
        "turn/completed",
        TurnCompletedNotification(
            thread_id="thread",
            turn=Turn(
                id="other-turn",
                items=[],
                status=TurnStatus.completed,
            ),
        ),
    )

    with pytest.raises(AdapterInvariantError) as error:
        _assert_notification_route(
            notification,
            expected_thread_id="thread",
            expected_turn_id="expected-turn",
            generation=3,
        )
    assert error.value.failure.code == "runtime_notification_route_mismatch"


@pytest.mark.asyncio
async def test_unknown_terminal_is_recorded_then_retires_protocol(
    tmp_path: Path,
) -> None:
    notification = Notification(
        "turn/completed",
        UnknownNotification(
            {"threadId": "thread", "turn": {"id": "turn", "status": "completed"}}
        ),
    )
    runtime = _runtime_for_notifications(tmp_path, (notification,))
    started = await runtime.start_turn(
        local_turn_id="local",
        thread=ThreadIdentity("thread", None, "session", None, None, "test"),
        input=TurnInput(text="hello"),
        config=TurnConfig(
            cwd=tmp_path,
            sandbox=SandboxProfile.READ_ONLY,
            approval_mode=ApprovalPolicy.AUTO_REVIEW,
        ),
    )
    stream = started.stream.__aiter__()

    event = await anext(stream)
    assert event.kind == "provider.unknown"
    with pytest.raises(RuntimeUnavailable) as error:
        await anext(stream)
    assert error.value.failure.code == "runtime_protocol_incompatible"


@pytest.mark.parametrize(
    ("exception", "expected_type", "expected_code"),
    [
        (TransportClosedError(), RuntimeUnavailable, "runtime_unavailable"),
        (
            ParseError(-32700, "parse"),
            RuntimeUnavailable,
            "runtime_protocol_incompatible",
        ),
        (
            InternalRpcError(-32603, "internal"),
            RuntimeUnavailable,
            "runtime_internal_error",
        ),
        (
            ServerBusyError(-32000, "busy", "server_overloaded"),
            ProviderRateLimited,
            "provider_rate_limited",
        ),
        (
            RetryLimitExceededError(-32000, "retry limit"),
            ProviderRateLimited,
            "provider_rate_limited",
        ),
        (
            MethodNotFoundError(-32601, "missing"),
            UnsupportedCapability,
            "unsupported_capability",
        ),
        (
            InvalidParamsError(-32602, "invalid"),
            ProviderRejected,
            "provider_rejected",
        ),
        (
            InvalidRequestError(-32600, "invalid"),
            ProviderRejected,
            "provider_rejected",
        ),
        (
            JsonRpcError(-32001, "provider"),
            ProviderRejected,
            "provider_rejected",
        ),
    ],
)
def test_public_sdk_errors_are_normalized(
    exception: CodexError,
    expected_type: type[AdapterError],
    expected_code: str,
) -> None:
    error = _adapter_error(
        exception,
        operation="turn.start",
        generation=4,
    )

    assert isinstance(error, expected_type)
    assert error.failure.code == expected_code


@pytest.mark.parametrize(
    "operation",
    [
        "thread.start",
        "thread.resume",
        "thread.fork",
        "thread.archive",
        "thread.unarchive",
        "thread.set_name",
        "thread.compact",
        "turn.steer",
        "turn.interrupt",
    ],
)
def test_transport_loss_during_thread_mutation_is_outcome_unknown(
    operation: str,
) -> None:
    error = _adapter_error(
        TransportClosedError(),
        operation=operation,
        generation=4,
    )

    assert isinstance(error, ProviderOutcomeUnknown)
    assert error.failure.code == "provider_effect_outcome_unknown"
    assert not error.failure.retryable


@pytest.mark.asyncio
async def test_compact_timeout_is_outcome_unknown(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    cancellation_seen = asyncio.Event()
    release = asyncio.Event()

    class HangingCompactThread:
        async def compact(self) -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancellation_seen.set()
                await release.wait()

    monkeypatch.setattr(
        codex_sdk,
        "_THREAD_COMPACT_START_TIMEOUT_SECONDS",
        0.001,
    )
    runtime = CodexSDKRuntime(
        client=cast(Any, object()),
        slot=_runtime_slot(tmp_path),
        generation=1,
        manifest=_capability_manifest(),
    )
    runtime._threads["thread"] = cast(Any, HangingCompactThread())

    try:
        with pytest.raises(ProviderOutcomeUnknown) as error:
            await asyncio.wait_for(runtime.compact_thread("thread"), timeout=0.1)

        assert error.value.failure.code == "provider_effect_outcome_unknown"
        assert error.value.failure.provider_exception == "TimeoutError"
        assert error.value.failure.thread_id == "thread"
        await asyncio.wait_for(cancellation_seen.wait(), timeout=0.1)
    finally:
        release.set()
        await asyncio.sleep(0)


def test_interrupt_error_is_operation_specific() -> None:
    error = _adapter_error(
        CodexError("interrupt failed"),
        operation="turn.interrupt",
        generation=4,
    )

    assert isinstance(error, InterruptFailed)
    assert error.failure.code == "interrupt_failed"


@pytest.mark.asyncio
async def test_runtime_close_can_retry_after_client_close_failure(tmp_path: Path) -> None:
    class RetryCloseClient:
        def __init__(self) -> None:
            self.attempts = 0

        async def close(self) -> None:
            self.attempts += 1
            if self.attempts == 1:
                raise OSError("close failed")

    client = RetryCloseClient()
    runtime = CodexSDKRuntime(
        client=cast(Any, client),
        slot=_runtime_slot(tmp_path),
        generation=1,
        manifest=_capability_manifest(),
    )

    with pytest.raises(OSError, match="close failed"):
        await runtime.close()
    assert not runtime._closed

    await runtime.close()

    assert client.attempts == 2
    assert runtime._closed


class _CaptureClient:
    def __init__(self, config: CodexConfig) -> None:
        self.config = config
        self.closed = False
        self.metadata = SimpleNamespace(
            serverInfo=SimpleNamespace(version="0.144.4")
        )

    async def __aenter__(self) -> _CaptureClient:
        return self

    async def close(self) -> None:
        self.closed = True


class _NotificationHandle:
    id = "turn"
    thread_id = "thread"

    def __init__(self, notifications: tuple[Notification, ...]) -> None:
        self._notifications = notifications

    async def stream(self) -> AsyncIterator[Notification]:
        for notification in self._notifications:
            yield notification


class _NotificationThread:
    def __init__(self, handle: _NotificationHandle) -> None:
        self._handle = handle

    async def turn(self, *_args: object, **_kwargs: object) -> _NotificationHandle:
        return self._handle


class _InputCaptureThread:
    def __init__(self) -> None:
        self.inputs: list[list[object]] = []

    async def turn(
        self,
        input: list[object],
        **_kwargs: object,
    ) -> _NotificationHandle:
        self.inputs.append(input)
        return _NotificationHandle(())


class _MutationThread:
    id = "mutated-thread"

    def __init__(self, *, identity_failures: int) -> None:
        self._identity_failures = identity_failures
        self.read_calls = 0
        self.names: list[str] = []

    async def set_name(self, name: str) -> None:
        self.names.append(name)

    async def read(self, *, include_turns: bool) -> SimpleNamespace:
        assert not include_turns
        self.read_calls += 1
        if self.read_calls <= self._identity_failures:
            raise CodexError("identity read failed")
        return SimpleNamespace(
            thread=SimpleNamespace(
                id=self.id,
                session_id="provider-session",
                forked_from_id=None,
                parent_thread_id=None,
                cli_version="0.144.4",
            )
        )


class _MutationClient:
    def __init__(
        self,
        thread: _MutationThread,
        *,
        resumed_thread: _MutationThread | None = None,
    ) -> None:
        self.thread = thread
        self.resumed_thread = resumed_thread or thread
        self.mutation_calls: list[str] = []
        self.resume_calls: list[str] = []

    async def thread_start(self, **_kwargs: object) -> _MutationThread:
        self.mutation_calls.append("start")
        return self.thread

    async def thread_fork(
        self,
        _thread_id: str,
        **_kwargs: object,
    ) -> _MutationThread:
        self.mutation_calls.append("fork")
        return self.thread

    async def thread_unarchive(self, _thread_id: str) -> _MutationThread:
        self.mutation_calls.append("unarchive")
        return self.thread

    async def thread_resume(self, thread_id: str) -> _MutationThread:
        self.resume_calls.append(thread_id)
        return self.resumed_thread


def _runtime_slot(root: Path) -> RuntimeSlotConfig:
    return RuntimeSlotConfig(
        scope_kind="project",
        project_id="project",
        cwd=root,
        codex_home=root / "codex-home",
        environment={},
        environment_hash="environment",
        topology_contract="project_scoped",
    )


def _runtime_for_notifications(
    root: Path,
    notifications: tuple[Notification, ...],
) -> CodexSDKRuntime:
    runtime = CodexSDKRuntime(
        client=cast(Any, object()),
        slot=_runtime_slot(root),
        generation=1,
        manifest=_capability_manifest(),
    )
    runtime._threads["thread"] = cast(
        Any,
        _NotificationThread(_NotificationHandle(notifications)),
    )
    return runtime


def _runtime_for_input_capture(
    root: Path,
    capture: _InputCaptureThread,
) -> CodexSDKRuntime:
    runtime = CodexSDKRuntime(
        client=cast(Any, object()),
        slot=_runtime_slot(root),
        generation=1,
        manifest=_capability_manifest(),
    )
    runtime._threads["thread"] = cast(Any, capture)
    return runtime


def _runtime_for_mutations(
    root: Path,
    client: _MutationClient,
) -> CodexSDKRuntime:
    return CodexSDKRuntime(
        client=cast(Any, client),
        slot=_runtime_slot(root),
        generation=1,
        manifest=_capability_manifest(),
    )


def _thread_config() -> ThreadConfig:
    return ThreadConfig(
        model=None,
        personality=None,
        sandbox=SandboxProfile.READ_ONLY,
    )


def _turn_config(root: Path) -> TurnConfig:
    return TurnConfig(
        cwd=root,
        sandbox=SandboxProfile.READ_ONLY,
        approval_mode=ApprovalPolicy.AUTO_REVIEW,
    )


def _turn_file(
    path: Path,
    *,
    ordinal: int,
    display_name: str,
) -> TurnFile:
    content = f"opaque:{display_name}".encode()
    data_root = path.parent
    attachment_root = data_root / "attachments"
    input_root = attachment_root / "input"
    input_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    data_root.chmod(0o700)
    attachment_root.chmod(0o700)
    input_root.chmod(0o700)
    path = input_root / path.name
    path.write_bytes(content)
    path.chmod(0o600)
    return TurnFile(
        attachment_id=f"file-{ordinal}",
        ordinal=ordinal,
        canonical_path=path.resolve(strict=True),
        display_name=display_name,
        reported_media_type="application/octet-stream",
        sha256=hashlib.sha256(content).hexdigest(),
        size_bytes=len(content),
        retention_until=9_999_999_999_999,
    )


def _turn_image(path: Path, *, ordinal: int, attachment_id: str) -> TurnImage:
    path.write_bytes(b"normalized-image")
    return TurnImage(
        attachment_id=attachment_id,
        ordinal=ordinal,
        canonical_path=path.resolve(strict=True),
        media_type="image/png",
        source_sha256="source-image-hash",
        sha256="normalized-image-hash",
        size_bytes=16,
        width=1,
        height=1,
        source_name_sanitized=path.name,
        retention_until=9_999_999_999_999,
    )


def _assert_exclusive_lock(path: Path, *, available: bool) -> None:
    if os.name != "posix":
        pytest.skip("POSIX advisory locks are required for this assertion")
    fcntl = importlib.import_module("fcntl")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        if available:
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        else:
            with pytest.raises(BlockingIOError):
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
    finally:
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)
