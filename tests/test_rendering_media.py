from __future__ import annotations

import asyncio
import io
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import psutil
import pytest
from PIL import Image, ImageCms

import codexd.rendering.media_worker as media_worker_module
from codexd.domain.content_blocks import TableBlock
from codexd.rendering import tables as table_renderer
from codexd.rendering.discord import (
    AttachmentKind,
    DiscordRenderPlanner,
    split_discord_code,
    split_discord_text,
)
from codexd.rendering.markdown import MarkdownContentParser
from codexd.rendering.media_worker import MediaWorker, MediaWorkerError
from codexd.rendering.tables import TableLimits, TableRenderKind, render_table


def test_markdown_table_renders_png_and_preserves_markdown() -> None:
    markdown = "| name | value |\n| --- | ---: |\n| formula | =2+2 |\n"
    blocks = MarkdownContentParser().parse(markdown)
    table = next(block for block in blocks if isinstance(block, TableBlock))

    rendered = render_table(table, TableLimits())

    assert rendered.kind is TableRenderKind.PNG_WITH_SOURCE
    assert rendered.pages[0].startswith(b"\x89PNG\r\n\x1a\n")
    assert rendered.markdown == markdown.encode()
    assert not hasattr(rendered, "csv")


def test_table_font_probe_covers_ascii_cjk_punctuation_and_emoji() -> None:
    markdown = (
        "| A0 | 中文， | Emoji |\n| --- | --- | --- |\n| value | 内容。 | 😀 |\n"  # noqa: RUF001
    )
    table = next(
        block
        for block in MarkdownContentParser().parse(markdown)
        if isinstance(block, TableBlock)
    )

    rendered = render_table(table, TableLimits())

    assert rendered.kind is TableRenderKind.PNG_WITH_SOURCE
    assert rendered.reason is None


def test_table_font_probe_uses_stable_fallback_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    markdown = "| name | value |\n| --- | --- |\n| a | b |\n"
    table = next(
        block
        for block in MarkdownContentParser().parse(markdown)
        if isinstance(block, TableBlock)
    )
    original = table_renderer._font_supports
    monkeypatch.setattr(
        table_renderer,
        "_font_supports",
        lambda font, character: character != "😀" and original(font, character),
    )

    rendered = render_table(table, TableLimits())

    assert rendered.kind is TableRenderKind.CODE_BLOCK_WITH_SOURCE
    assert rendered.reason == "table_font_coverage_missing"


@pytest.mark.asyncio
async def test_media_worker_normalizes_image_to_metadata_free_png(
    tmp_path: Path,
) -> None:
    source = tmp_path / "input.jpg"
    output = tmp_path / "normalized.png"
    image = Image.new("RGB", (8, 4), "red")
    exif = Image.Exif()
    exif[274] = 6
    profile = ImageCms.ImageCmsProfile(ImageCms.createProfile("sRGB")).tobytes()
    image.save(source, format="JPEG", exif=exif, icc_profile=profile)

    result = await MediaWorker().normalize_image(
        source=source,
        output=output,
        max_bytes=1024 * 1024,
        max_pixels=1_000_000,
    )

    assert result.media_type == "image/png"
    assert (result.width, result.height) == (4, 8)
    assert result.source_sha256 != result.normalized_sha256
    with Image.open(output) as normalized:
        assert normalized.format == "PNG"
        assert not normalized.getexif()
        assert normalized.info == {}


@pytest.mark.asyncio
async def test_media_worker_normalizes_webp_to_png(tmp_path: Path) -> None:
    source = tmp_path / "discord-image.png"
    output = tmp_path / "normalized.png"
    Image.new("RGBA", (7, 5), (25, 50, 75, 128)).save(
        source,
        format="WEBP",
        lossless=True,
    )

    result = await MediaWorker().normalize_image(
        source=source,
        output=output,
        max_bytes=1024 * 1024,
        max_pixels=1_000_000,
    )

    assert result.media_type == "image/png"
    assert (result.width, result.height) == (7, 5)
    with Image.open(output) as normalized:
        assert normalized.format == "PNG"
        assert normalized.mode == "RGBA"


@pytest.mark.asyncio
async def test_media_worker_flattens_animated_image_to_first_frame(
    tmp_path: Path,
) -> None:
    source = tmp_path / "animated.gif"
    output = tmp_path / "normalized.png"
    first = Image.new("RGB", (6, 4), "red")
    second = Image.new("RGB", (6, 4), "blue")
    first.save(
        source,
        format="GIF",
        save_all=True,
        append_images=[second],
        duration=100,
        loop=0,
    )

    await MediaWorker().normalize_image(
        source=source,
        output=output,
        max_bytes=1024 * 1024,
        max_pixels=1_000_000,
    )

    with Image.open(output) as normalized:
        assert normalized.format == "PNG"
        assert not getattr(normalized, "is_animated", False)
        assert normalized.convert("RGB").getpixel((0, 0)) == (255, 0, 0)


@pytest.mark.asyncio
async def test_media_worker_rejects_non_image(tmp_path: Path) -> None:
    source = tmp_path / "not-image.png"
    source.write_bytes(io.BytesIO(b"not an image").getvalue())

    with pytest.raises(Exception, match="image decode failed"):
        await MediaWorker().normalize_image(
            source=source,
            output=tmp_path / "output.png",
            max_bytes=1024,
            max_pixels=100,
        )


@pytest.mark.asyncio
async def test_media_worker_reports_child_eof() -> None:
    worker = MediaWorker()
    worker._command = (sys.executable, "-c", "raise SystemExit(17)")

    with pytest.raises(MediaWorkerError, match="exit code 17"):
        await worker._run(("unused",))


@pytest.mark.asyncio
async def test_media_worker_receives_only_explicit_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("CODEXD_SECRET_TEST", "must-not-cross")
    worker = MediaWorker(environment={"CODEXD_SAFE_TEST": "allowed"})

    child_environment = await worker._run(("environment",))

    assert isinstance(child_environment, dict)
    assert child_environment == {"CODEXD_SAFE_TEST": "allowed"}


def test_media_worker_uses_windows_job_object_containment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[int, int]] = []
    monkeypatch.setattr(media_worker_module, "_IS_WINDOWS", True)
    monkeypatch.setattr(
        media_worker_module,
        "_apply_windows_job_limits",
        lambda *, memory_bytes, cpu_seconds: calls.append(
            (memory_bytes, cpu_seconds)
        ),
    )

    media_worker_module._apply_limits(memory_mib=64, cpu_seconds=7)

    assert calls == [(64 * 1024 * 1024, 7)]


@pytest.mark.asyncio
async def test_table_media_failure_falls_back_to_markdown_source() -> None:
    worker = Mock()
    worker.render_table = AsyncMock(
        side_effect=MediaWorkerError("worker unavailable")
    )
    planner = DiscordRenderPlanner(
        media_worker=worker,
        table_limits=TableLimits(),
    )
    source = "| name | value |\n| --- | --- |\n| a | b |\n"

    rendered = await planner.render_markdown(source)

    assert rendered.messages[0] == "Table rendering failed; Markdown source attached."
    assert source in rendered.messages[1]
    assert rendered.attachments[0].content == source.encode()
    assert rendered.incident_codes == ("table_media_worker_failed",)


@pytest.mark.asyncio
async def test_durable_render_plan_reuses_verified_artifacts(tmp_path: Path) -> None:
    planner = DiscordRenderPlanner(
        media_worker=Mock(),
        table_limits=TableLimits(),
        artifact_root=tmp_path / "render",
    )
    source = f"```python\n{'x' * 2500}\n```"

    plan = await planner.create_durable_plan(turn_id="turn-test", source=source)
    payload = plan.to_payload(planner.artifact_root)
    loaded = planner.load_durable_plan(json.dumps(payload))

    assert loaded.messages == ("Oversized code block attached as `code.txt`.",)
    assert len(loaded.attachments) == 1
    assert loaded.attachments[0].kind is AttachmentKind.CODE
    assert loaded.attachments[0].path.read_bytes().rstrip(b"\n") == (
        "x" * 2500
    ).encode()
    legacy_payload = json.loads(json.dumps(payload))
    legacy_payload["version"] = 1
    for attachment in legacy_payload["attachments"]:
        attachment.pop("kind")
        attachment.pop("group_id")
    legacy = planner.load_durable_plan(json.dumps(legacy_payload))
    assert legacy.attachments[0].kind is AttachmentKind.CODE

    loaded.attachments[0].path.write_bytes(b"changed")
    with pytest.raises(Exception, match="changed or is missing"):
        planner.load_durable_plan(json.dumps(payload))


@pytest.mark.asyncio
async def test_near_limit_code_with_long_backtick_run_is_attached_completely() -> None:
    planner = DiscordRenderPlanner(
        media_worker=Mock(),
        table_limits=TableLimits(),
    )
    code = ("x" * 900) + ("`" * 10) + ("y" * 982)
    source = f"```````````text\n{code}\n```````````"

    rendered = await planner.render_markdown(source)

    assert rendered.messages == ("Oversized code block attached as `code.txt`.",)
    assert rendered.attachments[0].content.decode("utf-8").rstrip("\n") == code


@pytest.mark.asyncio
async def test_long_render_fallback_preserves_complete_source_attachment(
    tmp_path: Path,
) -> None:
    planner = DiscordRenderPlanner(
        media_worker=Mock(),
        table_limits=TableLimits(),
        artifact_root=tmp_path / "render",
    )
    source = ("持续输出😀\n" * 10_000).strip()

    plan = await planner.create_plain_text_fallback_plan(
        turn_id="fallback-turn",
        source=source,
    )

    assert len(plan.messages) == 1
    assert b"".join(
        attachment.path.read_bytes() for attachment in plan.attachments
    ).decode("utf-8") == source


@pytest.mark.asyncio
async def test_final_text_chunking_does_not_truncate_content() -> None:
    planner = DiscordRenderPlanner(
        media_worker=Mock(),
        table_limits=TableLimits(),
    )
    source = "x" * 5000

    rendered = await planner.render_markdown(source)

    assert all(len(message) <= 1900 for message in rendered.messages)
    assert "".join(rendered.messages) == source


def test_markdown_chunking_preserves_links_and_graphemes() -> None:
    link = "[documentation](https://example.invalid/a_(nested)_path)"
    linked_source = "x" * 1880 + link + " tail"

    linked = split_discord_text(linked_source)

    assert "".join(linked) == linked_source
    assert any(link in part for part in linked)
    family = "👩‍👩‍👧‍👦"
    grapheme_source = "x" * 1899 + family + "tail"

    graphemes = split_discord_text(grapheme_source)

    assert "".join(graphemes) == grapheme_source
    assert any(family in part for part in graphemes)
    assert all(not part.endswith("\u200d") for part in graphemes)


def test_code_chunking_balances_every_fence() -> None:
    chunks = split_discord_code("x" * 5000)

    assert all(len(chunk) <= 1900 for chunk in chunks)
    assert all(chunk.startswith("```\n") and chunk.endswith("\n```") for chunk in chunks)
    assert "".join(chunk[4:-4] for chunk in chunks) == "x" * 5000


@pytest.mark.asyncio
async def test_rendered_code_uses_a_fence_longer_than_its_content() -> None:
    planner = DiscordRenderPlanner(
        media_worker=Mock(),
        table_limits=TableLimits(),
    )

    rendered = await planner.render_markdown(
        "````python\nprint('``` inside')\n````"
    )

    assert rendered.messages == (
        "````python\nprint('``` inside')\n````",
    )


@pytest.mark.asyncio
async def test_media_worker_cancellation_terminates_child(tmp_path: Path) -> None:
    pid_file = tmp_path / "worker.pid"
    worker = MediaWorker(timeout_seconds=30)
    worker._command = (
        sys.executable,
        "-c",
        (
            "import os,pathlib,time;"
            f"pathlib.Path({str(pid_file)!r}).write_text(str(os.getpid()));"
            "time.sleep(30)"
        ),
    )
    task = asyncio.create_task(worker._run(("unused",)))
    for _ in range(100):
        if pid_file.exists():
            break
        await asyncio.sleep(0.01)
    assert pid_file.exists()
    pid = int(pid_file.read_text())
    assert psutil.pid_exists(pid)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert not psutil.pid_exists(pid)


@pytest.mark.asyncio
async def test_oversized_source_bypasses_markdown_parser_and_worker() -> None:
    worker = Mock()
    planner = DiscordRenderPlanner(
        media_worker=worker,
        table_limits=TableLimits(max_source_bytes=32),
    )
    planner._parser = Mock()  # type: ignore[assignment]

    rendered = await planner.render_markdown("x" * 33)

    planner._parser.parse.assert_not_called()
    worker.render_table.assert_not_called()
    assert rendered.messages == (
        "Final response exceeded rich-rendering limits; Markdown source attached.",
    )
    assert rendered.attachments[0].filename.startswith("response.part001")
    assert b"".join(
        attachment.content for attachment in rendered.attachments
    ) == b"x" * 33
    assert all(len(attachment.content) <= 32 for attachment in rendered.attachments)


@pytest.mark.asyncio
async def test_oversized_unicode_source_parts_remain_valid_utf8() -> None:
    planner = DiscordRenderPlanner(
        media_worker=Mock(),
        table_limits=TableLimits(max_source_bytes=7),
    )
    source = "前😀后" * 4

    rendered = await planner.render_markdown(source)

    assert len(rendered.attachments) > 1
    assert all(
        attachment.content.decode("utf-8")
        for attachment in rendered.attachments
    )
    assert b"".join(
        attachment.content for attachment in rendered.attachments
    ).decode("utf-8") == source


@pytest.mark.asyncio
async def test_oversized_markdown_link_becomes_source_attachment() -> None:
    planner = DiscordRenderPlanner(
        media_worker=Mock(),
        table_limits=TableLimits(max_source_bytes=10_000),
    )
    source = f"[label](https://example.invalid/{'x' * 2000})"

    rendered = await planner.render_markdown(source)

    assert rendered.messages == ("Oversized Markdown block attached as source.",)
    assert rendered.attachments[0].content == source.encode()


def test_table_source_limit_keeps_markdown_only() -> None:
    table = TableBlock(
        headers=("name",),
        rows=(("value",),),
        source_markdown="x" * 33,
    )
    rendered = render_table(table, TableLimits(max_source_bytes=32))

    assert rendered.kind is TableRenderKind.SOURCE_ATTACHMENT_ONLY
    assert rendered.markdown == b"x" * 33
    assert not hasattr(rendered, "csv")
