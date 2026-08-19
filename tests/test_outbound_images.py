from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import discord
import pytest
from conftest import StorageContext
from PIL import Image, PngImagePlugin

from codexd.application.dynamic_tools import DynamicToolDispatcher
from codexd.application.outbound_images import OutboundImageBroker
from codexd.application.volatile_turns import VolatileTurnStore
from codexd.config import RetentionConfig
from codexd.domain.conversations import SandboxProfile, ThreadConfig, ThreadIdentity
from codexd.domain.ids import canonical_json, sha256_text, utc_now_ms
from codexd.domain.turns import TurnInput, TurnSource, TurnState
from codexd.paths import AppPaths
from codexd.rendering.discord import DiscordRenderPlanner
from codexd.rendering.media_worker import MediaWorker
from codexd.rendering.tables import TableLimits
from codexd.runtime.port import DynamicToolCall
from codexd.security.signing import ComponentSigner
from codexd.storage.outbound_images import OutboundImageRepository
from codexd.storage.records import OutboxRecord, TurnRecord
from codexd.storage.repository import Repository
from codexd.storage.retention import run_retention
from codexd.storage.sqlite import SQLiteStore
from codexd.transport.discord.outbox import DiscordOutboxTransport


@pytest.mark.asyncio
async def test_dynamic_dispatcher_routes_publish_image_tool() -> None:
    images = Mock()
    images.handle = AsyncMock(
        return_value={"success": True, "contentItems": []}
    )
    dispatcher = DynamicToolDispatcher(
        schedules=Mock(),
        images=images,
        owner_user_id=400,
        guild_id=100,
    )
    call = DynamicToolCall(
        runtime_generation=1,
        local_turn_id="local-turn",
        provider_thread_id="provider-thread",
        provider_turn_id="provider-turn",
        provider_call_id="provider-call",
        namespace="codexd",
        tool="publish_image",
        arguments={},
    )

    response = await dispatcher.handle(call)

    assert response["success"] is True
    images.handle.assert_awaited_once_with(call)


def test_outbound_image_migration_marks_existing_toolset_for_new_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import codexd.storage.sqlite as sqlite_module

    migrations = sqlite_module._load_migrations()
    root = tmp_path / "project"
    root.mkdir()
    with SQLiteStore(tmp_path / "upgrade.sqlite3") as store:
        monkeypatch.setattr(
            sqlite_module,
            "_load_migrations",
            lambda: tuple(migration for migration in migrations if migration.version < 18),
        )
        assert store.migrate() == 17
        repository = Repository(store)
        project = repository.bind_project(
            name="upgrade",
            root_path=root,
            guild_id=100,
            channel_id=200,
            sandbox_profile=SandboxProfile.FULL_ACCESS,
        )
        conversation = repository.create_conversation(
            project_id=project.id,
            discord_thread_id=300,
            discord_guild_id=100,
            discord_parent_channel_id=200,
            owner_user_id=400,
        )
        before = repository.activate_thread_revision(
            conversation_id=conversation.id,
            identity=ThreadIdentity(
                thread_id="schedule-only-thread",
                requested_thread_id=None,
                provider_session_id="schedule-only-session",
                forked_from_thread_id=None,
                parent_thread_id=None,
                provider_version="0.144.4",
                dynamic_tools_enabled=True,
            ),
            config=ThreadConfig(
                model=None,
                personality=None,
                sandbox=SandboxProfile.FULL_ACCESS,
            ),
        )
        assert before.dynamic_tools_enabled is True

        monkeypatch.setattr(
            sqlite_module,
            "_load_migrations",
            lambda: tuple(migration for migration in migrations if migration.version < 19),
        )
        assert store.migrate() == 18
        after = repository.get_active_revision(conversation.id)
        assert after is not None
        assert after.dynamic_tools_enabled is False


def _active_turn(storage_context: StorageContext) -> tuple[TurnRecord, int]:
    repository = storage_context.repository
    repository.activate_thread_revision(
        conversation_id=storage_context.conversation.id,
        identity=ThreadIdentity(
            thread_id="image-thread",
            requested_thread_id=None,
            provider_session_id="image-session",
            forked_from_thread_id=None,
            parent_thread_id=None,
            provider_version="0.144.4",
            dynamic_tools_enabled=True,
        ),
        config=ThreadConfig(
            model=None,
            personality=None,
            sandbox=SandboxProfile.FULL_ACCESS,
        ),
    )
    lease = repository.create_runtime_lease(
        scope_kind="project",
        scope_key=storage_context.project.id,
        project_id=storage_context.project.id,
        environment_hash="image-environment",
    )
    repository.mark_runtime_ready(
        lease.id,
        sdk_version="0.144.4",
        runtime_version="0.144.4",
        capability_hash="image-capabilities",
    )
    turn = repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(text="Generate and publish a flow chart"),
        input_message_id="image-request",
        requested_by_user_id=400,
    )
    repository.claim_turn(
        turn.id,
        runtime_lease_id=lease.id,
        runtime_generation=lease.generation,
    )
    repository.mark_turn_running(turn.id, "image-provider-turn")
    return repository.get_turn(turn.id), lease.generation


def _broker(storage_context: StorageContext) -> OutboundImageBroker:
    return OutboundImageBroker(
        repository=OutboundImageRepository(storage_context.store),
        media_worker=MediaWorker(),
        artifact_root=storage_context.store.path.parent / "attachments" / "render",
        configured_guild_id=100,
        configured_owner_user_id=400,
        allowed_user_ids=frozenset({400, 401}),
        max_bytes=8 * 1024 * 1024,
        max_pixels=40_000_000,
        retention_days=30,
    )


def _image(path: Path, *, color: str = "navy", metadata: bool = True) -> None:
    info = PngImagePlugin.PngInfo()
    if metadata:
        info.add_text("author", "sensitive metadata")
        info.add_text("GPS", "31.2,121.5")
    with Image.new("RGB", (1024, 1600), color=color) as image:
        image.save(path, format="PNG", pnginfo=info)


def _call(
    turn: TurnRecord,
    generation: int,
    source: Path,
    *,
    call_id: str = "publish-call-1",
    observed: tuple[str, ...] | None = None,
    display_name: str = "host-start-flow.png",
) -> DynamicToolCall:
    return DynamicToolCall(
        runtime_generation=generation,
        local_turn_id=turn.id,
        provider_thread_id="image-thread",
        provider_turn_id="image-provider-turn",
        provider_call_id=call_id,
        namespace="codexd",
        tool="publish_image",
        arguments={
            "source_path": str(source),
            "display_name": display_name,
            "description": "Host startup flow chart",
        },
        observed_image_paths=(str(source),) if observed is None else observed,
    )


def _result(response: dict[str, object]) -> dict[str, object]:
    items = response["contentItems"]
    assert isinstance(items, list) and len(items) == 1
    item = items[0]
    assert isinstance(item, dict) and isinstance(item.get("text"), str)
    parsed = json.loads(item["text"])
    assert isinstance(parsed, dict)
    return parsed


@pytest.mark.asyncio
async def test_publish_image_normalizes_registers_and_replays(
    storage_context: StorageContext,
) -> None:
    turn, generation = _active_turn(storage_context)
    source = storage_context.root / "generated-flow.png"
    _image(source)
    broker = _broker(storage_context)
    call = _call(turn, generation, source)

    first = await broker.handle(call)
    source.unlink()
    replay = await broker.handle(call)
    conflict = await broker.handle(
        _call(
            turn,
            generation,
            source,
            display_name="different.png",
        )
    )

    assert first == replay
    assert _result(conflict)["code"] == "call_identity_conflict"
    assert first["success"] is True, _result(first)
    result = _result(first)
    assert result["status"] == "registered_for_final_delivery"
    assert result["display_name"] == "host-start-flow.png"
    assert result["media_type"] == "image/png"
    records = OutboundImageRepository(storage_context.store).registered_for_turn(
        turn.id
    )
    assert len(records) == 1
    record = records[0]
    assert (record.width, record.height) == (1024, 1600)
    assert record.relative_path is not None
    staged = (
        storage_context.store.path.parent
        / "attachments"
        / "render"
        / record.relative_path
    )
    assert staged.is_file()
    with Image.open(staged) as normalized:
        assert normalized.size == (1024, 1600)
        assert normalized.getexif() == {}
        assert normalized.info == {}
    row = storage_context.store.query_one(
        "SELECT result_json, relative_path FROM outbound_image_invocations"
    )
    assert row is not None
    assert "generated-flow.png" not in row["result_json"]
    assert str(source) not in row["result_json"]


@pytest.mark.asyncio
async def test_publish_image_rejects_unobserved_link_old_and_non_image_sources(
    storage_context: StorageContext,
) -> None:
    old_source = storage_context.root / "old.png"
    _image(old_source)
    os.utime(old_source, (1, 1))
    turn, generation = _active_turn(storage_context)
    broker = _broker(storage_context)

    unobserved_source = storage_context.root / "unobserved.png"
    _image(unobserved_source)
    unobserved = await broker.handle(
        _call(
            turn,
            generation,
            unobserved_source,
            call_id="unobserved",
            observed=(),
        )
    )
    old = await broker.handle(
        _call(turn, generation, old_source, call_id="old-source")
    )
    target = storage_context.root / "target.png"
    _image(target)
    linked = storage_context.root / "linked.png"
    try:
        linked.symlink_to(target)
    except OSError:
        pytest.skip("symlink creation is unavailable")
    link_result = await broker.handle(
        _call(turn, generation, linked, call_id="linked-source")
    )
    text = storage_context.root / "not-image.png"
    text.write_text("not a raster image", encoding="utf-8")
    non_image = await broker.handle(
        _call(turn, generation, text, call_id="non-image")
    )
    unsafe_name_source = storage_context.root / "unsafe-name.png"
    _image(unsafe_name_source, metadata=False)
    unsafe_name = await broker.handle(
        _call(
            turn,
            generation,
            unsafe_name_source,
            call_id="unsafe-name",
            display_name="@everyone.png",
        )
    )

    assert _result(unobserved)["code"] == "source_not_observed"
    assert _result(old)["code"] == "source_not_created_for_turn"
    assert _result(link_result)["code"] == "source_link_forbidden"
    assert _result(non_image)["code"] == "image_decode_failed"
    assert _result(unsafe_name)["code"] == "invalid_display_name"
    assert OutboundImageRepository(storage_context.store).registered_for_turn(
        turn.id
    ) == ()


@pytest.mark.asyncio
async def test_final_delivery_merges_images_and_suppresses_visualize_marker(
    storage_context: StorageContext,
) -> None:
    turn, generation = _active_turn(storage_context)
    source = storage_context.root / "delivery.png"
    _image(source, metadata=False)
    response = await _broker(storage_context).handle(
        _call(turn, generation, source)
    )
    assert response["success"] is True, _result(response)
    second_source = storage_context.root / "delivery-second.png"
    _image(second_source, color="maroon", metadata=False)
    second = await _broker(storage_context).handle(
        _call(
            turn,
            generation,
            second_source,
            call_id="publish-call-2",
            display_name="second-flow.png",
        )
    )
    assert second["success"] is True
    renderer = DiscordRenderPlanner(
        media_worker=MediaWorker(),
        table_limits=TableLimits(),
        artifact_root=storage_context.store.path.parent / "attachments" / "render",
    )
    sends: list[dict[str, object]] = []

    async def send(_content: str = "", **kwargs: object) -> object:
        sends.append(kwargs)
        return SimpleNamespace(id=len(sends), attachments=[])

    thread = Mock(spec=discord.Thread)
    thread.id = 300
    thread.archived = False
    thread.locked = False
    thread.send = send
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    client.get_channel.return_value = thread
    marker = '\ue200visualize\ue202{"path":"secret.html"}\ue201'
    volatile_turns = VolatileTurnStore()
    volatile_turns.put_final(
        turn.id,
        visible_text=f"Here is the flow chart.\n\n{marker}",
        final_answer_text=f"Here is the flow chart.\n\n{marker}",
    )
    transport = DiscordOutboxTransport(
        client=client,
        repository=storage_context.repository,
        renderer=renderer,
        signer=ComponentSigner(b"k" * 32),
        volatile_turns=volatile_turns,
    )
    record = OutboxRecord(
        id="image-final",
        destination_key="thread:300",
        operation="send",
        payload_json=canonical_json(
            {
                "kind": "turn_final",
                "turn_id": turn.id,
                "plain_text": f"Here is the flow chart.\n\n{marker}",
                "state": "completed",
                "terminal_code": "provider_completed",
            }
        ),
        delivery_marker="image-final",
        state="pending",
        attempts=0,
        lease_owner="test",
    )

    delivered = await transport.deliver(record)

    assert delivered.discord_message_id == "1"
    file_batches = [call["files"] for call in sends if "files" in call]
    assert len(file_batches) == 1
    assert [file.filename for file in file_batches[0]] == [
        "host-start-flow.png",
        "second-flow.png",
    ]
    plan = storage_context.repository.render_plan(turn.id)
    assert plan is None


@pytest.mark.asyncio
async def test_visualize_marker_without_registered_image_becomes_visible_failure(
    storage_context: StorageContext,
) -> None:
    turn, _generation = _active_turn(storage_context)
    renderer = DiscordRenderPlanner(
        media_worker=MediaWorker(),
        table_limits=TableLimits(),
        artifact_root=storage_context.store.path.parent / "attachments" / "render",
    )
    thread = Mock(spec=discord.Thread)
    thread.id = 300
    thread.archived = False
    thread.locked = False
    sent_content: list[str] = []

    async def send(content: str = "", **_kwargs: object) -> object:
        sent_content.append(content)
        return SimpleNamespace(id=len(sent_content), attachments=[])

    thread.send = send
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    client.get_channel.return_value = thread
    marker = "\ue200visualize\ue202{}\ue201"
    volatile_turns = VolatileTurnStore()
    volatile_turns.put_final(
        turn.id,
        visible_text=marker,
        final_answer_text=marker,
    )
    transport = DiscordOutboxTransport(
        client=client,
        repository=storage_context.repository,
        renderer=renderer,
        signer=ComponentSigner(b"k" * 32),
        volatile_turns=volatile_turns,
    )

    await transport.deliver(
        OutboxRecord(
            id="missing-image-final",
            destination_key="thread:300",
            operation="send",
            payload_json=canonical_json(
                {
                    "kind": "turn_final",
                    "turn_id": turn.id,
                    "plain_text": marker,
                    "state": "completed",
                    "dynamic_tools_enabled": True,
                }
            ),
            delivery_marker="missing-image-final",
            state="pending",
            attempts=0,
            lease_owner="test",
        )
    )

    assert any("could not be delivered" in content for content in sent_content)
    assert all("\ue200" not in content and "\ue201" not in content for content in sent_content)
    incident = storage_context.store.query_one(
        "SELECT code FROM incidents WHERE turn_id = ? AND code = ?",
        (turn.id, "visualization_publish_tool_not_used"),
    )
    assert incident is not None


@pytest.mark.asyncio
async def test_legacy_visualize_marker_recommends_new_session(
    storage_context: StorageContext,
) -> None:
    turn, _generation = _active_turn(storage_context)
    renderer = DiscordRenderPlanner(
        media_worker=MediaWorker(),
        table_limits=TableLimits(),
        artifact_root=storage_context.store.path.parent / "attachments" / "render",
    )
    thread = Mock(spec=discord.Thread)
    thread.id = 300
    thread.archived = False
    thread.locked = False
    thread.send = AsyncMock(
        side_effect=lambda content="", **_kwargs: SimpleNamespace(
            id=1,
            content=content,
            attachments=[],
        )
    )
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    client.get_channel.return_value = thread
    marker = "\ue200visualize\ue202{}\ue201"
    volatile_turns = VolatileTurnStore()
    volatile_turns.put_final(turn.id, visible_text=marker, final_answer_text=marker)
    transport = DiscordOutboxTransport(
        client=client,
        repository=storage_context.repository,
        renderer=renderer,
        signer=ComponentSigner(b"k" * 32),
        volatile_turns=volatile_turns,
    )

    await transport.deliver(
        OutboxRecord(
            id="legacy-image-final",
            destination_key="thread:300",
            operation="send",
            payload_json=canonical_json(
                {
                    "kind": "turn_final",
                    "turn_id": turn.id,
                    "state": "completed",
                    "dynamic_tools_enabled": False,
                }
            ),
            delivery_marker="legacy-image-final",
            state="pending",
            attempts=0,
            lease_owner="test",
        )
    )

    final_content = thread.send.await_args_list[0].args[0]
    assert "/session new" in final_content
    assert "created before Discord image delivery" in final_content
    assert all(not ("\ufe00" <= character <= "\ufe0f") for character in final_content)
    assert thread.send.await_args_list[0].kwargs["nonce"]
    incident = storage_context.store.query_one(
        "SELECT code FROM incidents WHERE turn_id = ? AND code = ?",
        (turn.id, "visualization_legacy_session"),
    )
    assert incident is not None


@pytest.mark.asyncio
async def test_registered_image_artifact_missing_keeps_distinct_failure(
    storage_context: StorageContext,
) -> None:
    turn, generation = _active_turn(storage_context)
    source = storage_context.root / "missing-after-registration.png"
    _image(source, metadata=False)
    result = await _broker(storage_context).handle(
        _call(turn, generation, source)
    )
    assert result["success"] is True
    registered = storage_context.repository.registered_outbound_images(turn.id)
    assert len(registered) == 1
    artifact = (
        storage_context.store.path.parent
        / "attachments"
        / "render"
        / registered[0].relative_path
    )
    artifact.unlink()
    marker = "\ue200visualize\ue202{}\ue201"
    volatile_turns = VolatileTurnStore()
    volatile_turns.put_final(turn.id, visible_text=marker, final_answer_text=marker)
    thread = Mock(spec=discord.Thread)
    thread.id = 300
    thread.archived = False
    thread.locked = False
    thread.send = AsyncMock(
        side_effect=lambda content="", **_kwargs: SimpleNamespace(
            id=1,
            content=content,
            attachments=[],
        )
    )
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    client.get_channel.return_value = thread
    transport = DiscordOutboxTransport(
        client=client,
        repository=storage_context.repository,
        renderer=DiscordRenderPlanner(
            media_worker=MediaWorker(),
            table_limits=TableLimits(),
            artifact_root=storage_context.store.path.parent / "attachments" / "render",
        ),
        signer=ComponentSigner(b"k" * 32),
        volatile_turns=volatile_turns,
    )

    await transport.deliver(
        OutboxRecord(
            id="missing-registered-image-final",
            destination_key="thread:300",
            operation="send",
            payload_json=canonical_json(
                {
                    "kind": "turn_final",
                    "turn_id": turn.id,
                    "state": "completed",
                    "dynamic_tools_enabled": True,
                }
            ),
            delivery_marker="missing-registered-image-final",
            state="pending",
            attempts=0,
            lease_owner="test",
        )
    )

    delivered = thread.send.await_args_list[0].args[0]
    assert "registered image artifact was unavailable" in delivered
    assert "/session new" not in delivered
    assert storage_context.store.query_one(
        "SELECT 1 FROM incidents WHERE turn_id = ? AND code = ?",
        (turn.id, "outbound_image_artifact_unavailable"),
    ) is not None
    assert storage_context.store.query_one(
        "SELECT 1 FROM incidents WHERE turn_id = ? AND code = ?",
        (turn.id, "visualization_artifact_unavailable"),
    ) is not None
    assert storage_context.store.query_one(
        "SELECT 1 FROM incidents WHERE turn_id = ? AND code = ?",
        (turn.id, "visualization_publish_tool_not_used"),
    ) is None


@pytest.mark.asyncio
async def test_image_upload_rejection_produces_visible_fallback_and_incident(
    storage_context: StorageContext,
) -> None:
    turn, generation = _active_turn(storage_context)
    source = storage_context.root / "rejected-upload.png"
    _image(source, metadata=False)
    publish_result = await _broker(storage_context).handle(
        _call(turn, generation, source)
    )
    assert publish_result["success"] is True, _result(publish_result)
    renderer = DiscordRenderPlanner(
        media_worker=MediaWorker(),
        table_limits=TableLimits(),
        artifact_root=storage_context.store.path.parent / "attachments" / "render",
    )
    response = Mock(status=413, reason="too large")
    thread = Mock(spec=discord.Thread)
    thread.id = 300
    thread.archived = False
    thread.locked = False
    thread.send = AsyncMock(
        side_effect=[
            Mock(id=1),
            discord.HTTPException(response, "too large"),
            Mock(id=2),
            Mock(id=3),
        ]
    )
    client = Mock(spec=discord.Client)
    client.user = Mock(id=999)
    client.get_channel.return_value = thread
    transport = DiscordOutboxTransport(
        client=client,
        repository=storage_context.repository,
        renderer=renderer,
        signer=ComponentSigner(b"k" * 32),
    )

    result = await transport.deliver(
        OutboxRecord(
            id="rejected-image-final",
            destination_key="thread:300",
            operation="send",
            payload_json=canonical_json(
                {
                    "kind": "turn_final",
                    "turn_id": turn.id,
                    "plain_text": "The generated flow chart follows.",
                    "state": "completed",
                }
            ),
            delivery_marker="rejected-image-final",
            state="pending",
            attempts=0,
            lease_owner="test",
        )
    )

    assert result.discord_message_id == "1"
    fallback_content = thread.send.await_args_list[2].args[0]
    assert "could not be attached" in fallback_content
    assert "base64" not in fallback_content
    incident = storage_context.store.query_one(
        "SELECT code FROM incidents WHERE turn_id = ? AND code = ?",
        (turn.id, "outbound_image_delivery_failed"),
    )
    assert incident is not None


@pytest.mark.asyncio
async def test_outbound_image_retention_removes_registered_file_with_render_plan(
    storage_context: StorageContext,
) -> None:
    turn, generation = _active_turn(storage_context)
    source = storage_context.root / "retained-image.png"
    _image(source, metadata=False)
    publish_result = await _broker(storage_context).handle(
        _call(turn, generation, source)
    )
    assert publish_result["success"] is True, _result(publish_result)
    record = OutboundImageRepository(storage_context.store).registered_for_turn(
        turn.id
    )[0]
    assert record.relative_path is not None
    render_root = storage_context.store.path.parent / "attachments" / "render"
    path = render_root / record.relative_path
    plan = {
        "version": 3,
        "messages": ["Delivered image"],
        "incident_codes": [],
        "attachments": [
            {
                "filename": record.display_name,
                "relative_path": record.relative_path,
                "description": record.description,
                "sha256": record.normalized_sha256,
                "size_bytes": record.size_bytes,
                "kind": "image",
                "group_id": None,
            }
        ],
    }
    storage_context.repository.persist_render_plan(
        turn_id=turn.id,
        source_sha256=sha256_text("Delivered image"),
        plan=plan,
        retention_until=1,
    )
    storage_context.repository.terminal_turn(
        turn.id,
        target=TurnState.COMPLETED,
        terminal_code="provider_completed",
    )
    with storage_context.store.transaction() as connection:
        connection.execute(
            """
            UPDATE discord_outbox
            SET state = 'sent', updated_at = 1
            WHERE json_extract(payload_json, '$.kind') = 'turn_final'
              AND json_extract(payload_json, '$.turn_id') = ?
            """,
            (turn.id,),
        )
        connection.execute(
            "UPDATE outbound_image_invocations SET retention_until = 1 WHERE turn_id = ?",
            (turn.id,),
        )

    run_retention(
        storage_context.store,
        AppPaths(
            storage_context.store.path.parent,
            storage_context.store.path.parent / "logs",
        ),
        RetentionConfig(),
        now_ms=utc_now_ms(),
    )

    assert not path.exists()
    assert storage_context.repository.render_plan(turn.id) is None
    assert OutboundImageRepository(storage_context.store).registered_for_turn(
        turn.id
    ) == ()
