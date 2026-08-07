from __future__ import annotations

from collections.abc import Mapping, Sequence

import discord

COLOR_CODEX = 0x7C3AED
COLOR_RUNNING = 0xF59E0B
COLOR_SUCCESS = 0x10B981
COLOR_FAILURE = 0xEF4444
COLOR_INFO = 0x3B82F6
COLOR_MUTED = 0x6B7280

TABLE_COPY_CUSTOM_ID = "tb:v1:copy"


def progress_embed(content: str) -> discord.Embed:
    state, separator, detail = content.partition(" · ")
    normalized = state.casefold()
    if normalized == "running":
        title, icon, color = "Codex is working", "⚙️", COLOR_RUNNING
    elif normalized == "queued":
        title, icon, color = "Turn queued", "⏳", COLOR_INFO
    elif normalized == "completed":
        title, icon, color = "Turn completed", "✅", COLOR_SUCCESS
    elif normalized == "cancelled":
        title, icon, color = "Turn cancelled", "⏹️", COLOR_MUTED
    elif normalized == "interrupted":
        title, icon, color = "Turn interrupted", "⚠️", COLOR_FAILURE
    elif normalized == "failed":
        title, icon, color = "Turn failed", "❌", COLOR_FAILURE
    else:
        title, icon, color = "Turn status", "💡", COLOR_INFO
    description = detail if separator else content
    embed = discord.Embed(
        title=f"{icon} {title}",
        description=_safe_markdown(description, 4096),
        color=color,
    )
    embed.set_footer(text="codexD · live Turn status")
    return embed


def terminal_footer(payload: Mapping[str, object]) -> str:
    state = str(payload.get("state") or "unknown").casefold()
    label, icon = _terminal_style(state)
    terminal_code = str(payload.get("terminal_code") or state or "unknown")
    parts = [icon if state == "completed" else f"{icon} {_safe_plain(label, 32)}"]
    if state != "completed":
        parts.append(f"`{_safe_plain(terminal_code, 128)}`")

    usage = payload.get("usage")
    if isinstance(usage, Mapping):
        latest = usage.get("last")
        if isinstance(latest, Mapping):
            input_tokens = _token_count(latest.get("input_tokens"))
            output_tokens = _token_count(latest.get("output_tokens"))
            if input_tokens is not None:
                parts.append(f"📥 {_compact_count(input_tokens)}")
            if output_tokens is not None:
                parts.append(f"📤 {_compact_count(output_tokens)}")

    duration = _duration_text(payload.get("started_at"), payload.get("ended_at"))
    if duration:
        parts.append(f"⏱️ {duration}")

    if isinstance(usage, Mapping):
        context_usage = _context_usage(usage)
        if context_usage is not None:
            parts.append(f"🧠 {context_usage}")

    lines = [f"-# {' | '.join(parts)}"]
    execution = [
        value
        for value in (
            _optional_text(payload.get("sandbox"), 64),
            _optional_text(payload.get("approval_mode"), 64),
        )
        if value
    ]
    if execution:
        lines.append(
            "-# ⚡ " + " · ".join(_safe_plain(value, 64) for value in execution)
        )
    return "\n".join(lines)


def format_usage(payload: Mapping[str, object]) -> str:
    context = payload.get("model_context_window")
    context_text = (
        f"{context:,}"
        if isinstance(context, int) and not isinstance(context, bool)
        else "`not reported`"
    )
    return "\n".join(
        (
            "**Provider-reported token usage**",
            "Latest provider breakdown (`last`): "
            f"{_usage_breakdown(payload.get('last'))}",
            f"Thread cumulative (`total`): {_usage_breakdown(payload.get('total'))}",
            f"Model context window: {context_text}",
            "Cost: `not reported`",
            "All-subagent attribution: `unknown`",
        )
    )


def task_card_embed(
    payload: Mapping[str, object],
    *,
    expanded: bool,
) -> discord.Embed:
    state = _optional_text(payload.get("state"), 64) or "unknown"
    title = _optional_text(payload.get("title"), 256) or "Codex task"
    icon, color = _task_style(state)
    embed = discord.Embed(
        title=f"{icon} {_safe_plain(title, 240)}",
        description=f"State: **{_safe_plain(state, 64)}**",
        color=color,
    )
    if expanded:
        summary = _optional_text(payload.get("status_summary"), 512)
        operation = _optional_text(payload.get("operation"), 128)
        model = _optional_text(payload.get("model"), 128)
        reasoning = _optional_text(payload.get("reasoning_effort"), 64)
        if summary:
            embed.description = _safe_markdown(summary, 1024)
        if operation and operation != "activity":
            embed.add_field(
                name="Operation",
                value=f"`{_safe_plain(operation, 128)}`",
                inline=True,
            )
        if model:
            embed.add_field(
                name="Model",
                value=f"`{_safe_plain(model, 128)}`",
                inline=True,
            )
        if reasoning:
            embed.add_field(
                name="Reasoning",
                value=f"`{_safe_plain(reasoning, 64)}`",
                inline=True,
            )
        agents = payload.get("agents")
        if isinstance(agents, list):
            rows = _agent_rows(agents)
            if rows:
                embed.add_field(name="Agents", value="\n".join(rows), inline=False)
    embed.set_footer(text="codexD · task card")
    return embed


def notice_embed(
    content: str,
    *,
    level: str = "info",
    title: str | None = None,
) -> discord.Embed:
    normalized = level.casefold()
    if normalized == "error":
        icon, default_title, color = "❌", "codexD error", COLOR_FAILURE
    elif normalized == "warning":
        icon, default_title, color = "⚠️", "codexD warning", COLOR_RUNNING
    elif normalized == "success":
        icon, default_title, color = "✅", "codexD update", COLOR_SUCCESS
    else:
        icon, default_title, color = "💡", "codexD update", COLOR_INFO
    embed = discord.Embed(
        title=f"{icon} {_safe_plain(title or default_title, 220)}",
        description=_safe_markdown(content, 4096),
        color=color,
    )
    embed.set_footer(text="codexD")
    return embed


def attachment_embed(filenames: Sequence[str]) -> discord.Embed:
    visible = [f"• `{_safe_plain(name, 160)}`" for name in filenames[:10]]
    embed = discord.Embed(
        title="📎 Codex attachments",
        description="\n".join(visible) or "Attached output",
        color=COLOR_INFO,
    )
    embed.set_footer(text=f"codexD · {len(filenames)} file(s)")
    return embed


def table_embed(
    *,
    summary: str,
    image_filename: str,
    page_number: int,
    page_count: int,
    source_attached: bool,
) -> discord.Embed:
    embed = discord.Embed(
        title="📊 Codex table",
        description=_safe_markdown(summary, 1024),
        color=COLOR_CODEX,
    )
    embed.set_image(url=f"attachment://{image_filename}")
    source = " · Markdown source attached" if source_attached else ""
    embed.set_footer(text=f"codexD · page {page_number}/{page_count}{source}")
    return embed


def table_source_embed(*, summary: str, reason: str) -> discord.Embed:
    embed = discord.Embed(
        title="📊 Codex table source",
        description=_safe_markdown(summary, 1024),
        color=COLOR_CODEX,
    )
    embed.add_field(name="Display", value=_safe_plain(reason, 512), inline=False)
    embed.set_footer(text="codexD · Markdown source attached")
    return embed


def table_copy_view() -> discord.ui.View:
    view = discord.ui.View(timeout=None)
    view.add_item(
        discord.ui.Button(
            label="Copy as text",
            emoji="📋",
            style=discord.ButtonStyle.secondary,
            custom_id=TABLE_COPY_CUSTOM_ID,
        )
    )
    return view


def _terminal_style(state: str) -> tuple[str, str]:
    if state == "completed":
        return "completed", "✅"
    if state == "cancelled":
        return "cancelled", "⏹️"
    if state == "interrupted":
        return "interrupted", "⚠️"
    if state == "failed":
        return "failed", "❌"
    return "finished", "💡"


def _task_style(state: str) -> tuple[str, int]:
    normalized = state.casefold()
    if normalized in {"completed", "success", "succeeded"}:
        return "✅", COLOR_SUCCESS
    if normalized in {
        "failed",
        "errored",
        "error",
        "interrupted",
        "shutdown",
        "not_found",
    }:
        return "❌", COLOR_FAILURE
    if normalized in {"running", "pending", "pending_init"}:
        return "⚙️", COLOR_RUNNING
    return "💡", COLOR_INFO


def _agent_rows(agents: list[object]) -> list[str]:
    rows: list[str] = []
    for raw_agent in agents[:12]:
        if not isinstance(raw_agent, dict):
            continue
        label = _optional_text(raw_agent.get("label"), 64) or "Agent"
        state = _optional_text(raw_agent.get("state"), 64) or "unknown"
        message = _optional_text(raw_agent.get("message"), 160)
        row = f"• **{_safe_plain(label, 64)}** · `{_safe_plain(state, 64)}`"
        if message:
            row += f" — {_safe_markdown(message, 160)}"
        rows.append(row[:300])
    return rows


def _duration_text(started_at: object, ended_at: object) -> str | None:
    if (
        isinstance(started_at, bool)
        or isinstance(ended_at, bool)
        or not isinstance(started_at, int)
        or not isinstance(ended_at, int)
        or ended_at < started_at
    ):
        return None
    seconds = (ended_at - started_at) / 1000
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes, remaining = divmod(int(seconds), 60)
    return f"{minutes}m {remaining}s"


def _optional_text(value: object, limit: int) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text[:limit] if text else None


def _usage_breakdown(value: object) -> str:
    if not isinstance(value, Mapping):
        return "`not reported`"
    fields = (
        ("input_tokens", "input"),
        ("output_tokens", "output"),
        ("cached_input_tokens", "cached"),
        ("reasoning_output_tokens", "reasoning"),
        ("total_tokens", "total"),
    )
    parts = [
        f"{label} {token_count:,}"
        for field, label in fields
        if isinstance((token_count := value.get(field)), int)
        and not isinstance(token_count, bool)
    ]
    return " · ".join(parts) if parts else "`not reported`"


def _token_count(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
        return value
    return None


def _compact_count(value: int) -> str:
    if value < 1_000:
        return str(value)
    if value < 1_000_000:
        return f"{value / 1_000:.1f}".rstrip("0").rstrip(".") + "k"
    return f"{value / 1_000_000:.1f}".rstrip("0").rstrip(".") + "m"


def _context_usage(usage: Mapping[str, object]) -> str | None:
    latest = usage.get("last")
    if not isinstance(latest, Mapping):
        return None
    input_tokens = _token_count(latest.get("input_tokens"))
    context_window = _token_count(usage.get("model_context_window"))
    if input_tokens is None or context_window is None or context_window == 0:
        return None
    percent = input_tokens * 100 / context_window
    if 0 < percent < 0.1:
        return "<0.1%"
    precision = 1 if percent < 10 else 0
    return f"{percent:.{precision}f}%"


def _safe_markdown(value: object, limit: int) -> str:
    return discord.utils.escape_mentions(str(value)[:limit])


def _safe_plain(value: object, limit: int) -> str:
    return discord.utils.escape_markdown(discord.utils.escape_mentions(str(value)[:limit]))
