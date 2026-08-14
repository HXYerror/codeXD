from __future__ import annotations

import hashlib
import importlib.metadata
import io
import os
import secrets
import zipfile

import pytest
from conftest import StorageContext
from openai_codex import (
    ApprovalMode,
    AsyncCodex,
    CodexConfig,
    Sandbox,
    TextInput,
)

from codexd.application.attachment_materializer import AttachmentMaterializer
from codexd.domain.turns import TurnFile, TurnInput, TurnSource
from codexd.security import private_files


@pytest.mark.asyncio
async def test_live_0144_4_app_server_reads_materialized_zip_marker(
    storage_context: StorageContext,
) -> None:
    """Opt-in semantic contract; this starts one real, potentially billed Turn."""

    if os.environ.get("CODEXD_RUN_LIVE_ATTACHMENT_CONTRACT") != "1":
        pytest.skip("set CODEXD_RUN_LIVE_ATTACHMENT_CONTRACT=1 to run a real Codex Turn")
    versions = {
        name: importlib.metadata.version(name)
        for name in ("openai-codex", "openai-codex-cli-bin")
    }
    if set(versions.values()) != {"0.144.4"}:
        pytest.skip(f"the semantic regression fixture is pinned to 0.144.4: {versions}")

    marker = "CODEXD_ATTACHMENT_" + secrets.token_hex(16)
    archive_stream = io.BytesIO()
    with zipfile.ZipFile(
        archive_stream,
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as archive:
        archive.writestr("logs/marker.txt", marker + "\n")
    content = archive_stream.getvalue()
    private_files.ensure_private_directory(storage_context.store.path.parent)
    attachment_root = storage_context.store.path.parent / "attachments"
    private_files.ensure_private_directory(attachment_root)
    input_root = attachment_root / "input"
    private_files.ensure_private_directory(input_root)
    source_path = input_root / "live-contract.zip"
    source_path.write_bytes(content)
    private_files.secure_private_file(source_path)
    source = TurnFile(
        attachment_id="live-contract-attachment",
        ordinal=0,
        canonical_path=source_path.resolve(strict=True),
        display_name="contract.zip",
        reported_media_type="application/zip",
        sha256=hashlib.sha256(content).hexdigest(),
        size_bytes=len(content),
        retention_until=9_999_999_999_999,
    )
    turn = storage_context.repository.enqueue_turn(
        conversation_id=storage_context.conversation.id,
        source=TurnSource.DISCORD,
        turn_input=TurnInput(files=(source,)),
        input_message_id="live-app-server-attachment-contract",
    )
    materialized = AttachmentMaterializer(
        store=storage_context.store,
        data_root=storage_context.store.path.parent,
    ).materialize(turn_id=turn.id, files=(source,))

    config = CodexConfig(
        cwd=str(storage_context.root),
        env={"RUST_LOG": "warn"},
        experimental_api=False,
    )
    async with AsyncCodex(config) as client:
        thread = await client.thread_start(
            approval_mode=ApprovalMode.deny_all,
            cwd=str(storage_context.root),
            ephemeral=True,
            sandbox=Sandbox.full_access,
        )
        handle = await thread.turn(
            [
                TextInput(
                    "Read the attached file and return only the unique marker it contains."
                ),
                TextInput(materialized.context),
            ],
            approval_mode=ApprovalMode.deny_all,
            cwd=str(storage_context.root),
            sandbox=Sandbox.full_access,
        )
        result = await handle.run()

    roots = [getattr(item, "root", item) for item in result.items]
    assert result.final_response is not None and marker in result.final_response
    assert any("command" in type(item).__name__.casefold() for item in roots)
    assert not any(storage_context.root.iterdir())
