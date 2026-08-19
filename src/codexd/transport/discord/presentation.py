from __future__ import annotations

import re
from collections.abc import Mapping, Sequence

import discord

from codexd.application.session_lifecycle import SessionStatusView

COLOR_CODEX = 0x7C3AED
COLOR_RUNNING = 0xF59E0B
COLOR_SUCCESS = 0x10B981
COLOR_FAILURE = 0xEF4444
COLOR_INFO = 0x3B82F6
COLOR_MUTED = 0x6B7280

TABLE_COPY_CUSTOM_ID = "tb:v1:copy"
_OPAQUE_PROVIDER_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")


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


def session_status_embed(
    view: SessionStatusView,
    *,
    disclose_provider_session_id: bool = False,
) -> discord.Embed:
    conversation = view.conversation
    runtime_state = view.activity.runtime_state
    if (
        conversation.state.value == "blocked"
        or conversation.provider_barrier_kind is not None
        or runtime_state in {"starting", "unhealthy", "failed"}
    ):
        title, color = "⚠️ Session needs attention", COLOR_RUNNING
    elif conversation.state.value == "archived":
        title, color = "📦 Session archived", COLOR_MUTED
    elif conversation.state.value == "uninitialized":
        title, color = "⚪ Session not started", COLOR_MUTED
    elif conversation.state.value == "active":
        title, color = "🟢 Session active", COLOR_SUCCESS
    else:
        title, color = "⚠️ Session unavailable", COLOR_FAILURE

    revision = view.active_revision
    description_parts = []
    if revision is not None and revision.name:
        description_parts.append(_safe_plain(revision.name, 120))
    description_parts.append(f"Project **{_safe_plain(view.project_name, 120)}**")
    embed = discord.Embed(
        title=title,
        description=" · ".join(description_parts)[:4096],
        color=color,
    )

    behavior = view.behavior
    modalities = (
        "/".join(_safe_plain(item, 24) for item in behavior.input_modalities)
        if behavior.input_modalities
        else "resolves on next Turn"
    )
    model_lines = [
        f"**{_safe_plain(behavior.model.value, 128)}** · "
        f"{_safe_plain(behavior.model.source, 48)}",
        f"Reasoning `{_safe_plain(behavior.reasoning_effort.value, 64)}` · "
        f"summary `{_safe_plain(behavior.reasoning_summary.value, 64)}`",
        f"Personality `{_safe_plain(behavior.personality.value, 64)}` · "
        f"tier `{_safe_plain(behavior.service_tier.value, 64)}`",
        f"Web `{_safe_plain(behavior.web_search_mode, 48)}` · "
        f"input `{modalities}`",
    ]
    if behavior.resolution != "resolved":
        model_lines.append(f"Resolution: {_safe_plain(behavior.resolution, 128)}")
    embed.add_field(
        name="Model & behavior · next Turn",
        value="\n".join(model_lines)[:1024],
        inline=False,
    )

    activity = view.activity
    runtime = f"Runtime `{_safe_plain(runtime_state, 48)}`"
    if activity.runtime_generation > 0:
        runtime += f" · generation `{activity.runtime_generation}`"
    activity_lines = [
        runtime,
        f"Turns active `{activity.active_turns}` · queued `{activity.queued_turns}`",
        f"Last completed {_relative_time(activity.last_completed_at)}",
    ]
    if activity.active_turn is not None:
        turn = activity.active_turn
        current = (
            f"Current Turn `{_safe_plain(turn.effective_model or 'provider default', 96)}`"
            " · reasoning `"
            + _safe_plain(
                turn.effective_reasoning_effort or "provider default",
                48,
            )
            + "`"
        )
        if activity.active_settings_differ:
            current += " · differs from next Turn"
        activity_lines.append(current)
    embed.add_field(
        name="Activity",
        value="\n".join(activity_lines)[:1024],
        inline=False,
    )

    session_lines = _session_identity_lines(
        view,
        disclose_provider_session_id=disclose_provider_session_id,
    )
    embed.add_field(name="Session", value="\n".join(session_lines), inline=False)

    execution_lines = ["**FULL ACCESS** · `auto_review`"]
    if conversation.provider_barrier_kind is not None:
        execution_lines.append(
            "⚠️ Provider barrier `"
            + _safe_plain(conversation.provider_barrier_kind, 64)
            + "`"
        )
    if view.degraded_reason is not None:
        execution_lines.append(
            "⚠️ " + _safe_plain(view.degraded_reason, 180)
        )
    embed.add_field(
        name="Execution",
        value="\n".join(execution_lines)[:1024],
        inline=False,
    )
    embed.set_footer(
        text="Advanced: /model show · /usage · /session list · /capabilities"
    )
    return embed


def _session_identity_lines(
    view: SessionStatusView,
    *,
    disclose_provider_session_id: bool,
) -> list[str]:
    revision = view.active_revision
    if revision is None:
        return [
            "Provider Session ID",
            "`none`",
            "Current revision `none`",
            f"Identity: {_safe_plain(view.resume_verification, 160)}",
        ]
    if disclose_provider_session_id and view.provider_session_id is not None:
        session_reference = _exact_provider_id(view.provider_session_id)
    elif view.provider_session_hash is not None:
        session_reference = _exact_provider_id(f"hash:{view.provider_session_hash}")
    else:
        session_reference = "unavailable"
    provider_thread = (
        _exact_provider_id(f"hash:{view.provider_thread_hash}")
        if view.provider_thread_hash is not None
        else "unavailable"
    )
    return [
        "Provider Session ID",
        f"`{session_reference}`",
        f"Current revision `{revision.id[:8]}` · Codex "
        f"`{_safe_plain(revision.provider_version, 64)}`",
        f"Provider Thread `{provider_thread}`",
        f"Identity: {_safe_plain(view.resume_verification, 160)}",
    ]


def _exact_provider_id(value: str) -> str:
    return value if _OPAQUE_PROVIDER_ID.fullmatch(value) is not None else "unavailable"


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


def schedule_draft_embed(payload: Mapping[str, object]) -> discord.Embed:
    state = str(payload.get("state") or "pending")
    if state == "confirmed":
        description = "✅ Confirmed by the configured Discord owner."
        color = COLOR_SUCCESS
    elif state == "cancelled":
        description = "Cancelled; no Schedule was activated."
        color = COLOR_MUTED
    elif state in {"expired", "delivery_failed"}:
        description = "Expired; no Schedule was activated."
        color = COLOR_FAILURE
    else:
        description = (
            "⚠️ **FULL ACCESS / unattended:** confirming activates work with "
            "unrestricted project and system access."
        )
        color = COLOR_FAILURE
    embed = discord.Embed(
        title=f"Schedule · {_safe_plain(payload.get('action') or 'create', 32)}",
        description=description,
        color=color,
    )
    embed.add_field(
        name="Name",
        value=_safe_plain(payload.get("name") or "unknown", 1024),
        inline=True,
    )
    expression = _safe_plain(payload.get("expression") or "unknown", 512)
    timezone = _safe_plain(payload.get("timezone") or "unknown", 256)
    embed.add_field(
        name="When",
        value=f"`{expression}`\n{timezone}"[:1024],
        inline=True,
    )
    embed.add_field(
        name="Misfire",
        value=f"`{_safe_plain(payload.get('misfire_policy') or 'unknown', 64)}`",
        inline=True,
    )
    embed.add_field(
        name="Prompt",
        value=_safe_plain(payload.get("prompt_text") or "unavailable", 1000),
        inline=False,
    )
    occurrences = payload.get("occurrences")
    preview: list[str] = []
    if isinstance(occurrences, Sequence) and not isinstance(occurrences, str):
        for index, item in enumerate(occurrences[:3], start=1):
            if not isinstance(item, Mapping):
                continue
            utc_ms = item.get("utc_ms")
            local = item.get("local_display")
            if (
                isinstance(utc_ms, int)
                and not isinstance(utc_ms, bool)
                and isinstance(local, str)
            ):
                preview.append(
                    f"{index}. <t:{utc_ms // 1000}:F> "
                    f"(`{_safe_plain(local, 160)}`)"
                )
    embed.add_field(
        name="Next occurrences",
        value="\n".join(preview)[:1024] or "No future occurrence resolved.",
        inline=False,
    )
    if state == "confirmed":
        schedule_ref = _safe_plain(payload.get("schedule_ref") or "unknown", 16)
        next_due_at = payload.get("next_due_at")
        detail = f"Schedule `{schedule_ref}` is active."
        if isinstance(next_due_at, int) and not isinstance(next_due_at, bool):
            detail += f" Next: <t:{next_due_at // 1000}:F>."
        embed.add_field(name="Result", value=detail[:1024], inline=False)
    footer = (
        "codexD · confirmation expires in 10 minutes"
        if state == "pending"
        else f"codexD · {state}"
    )
    embed.set_footer(text=footer)
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


def _relative_time(timestamp_ms: int | None) -> str:
    if timestamp_ms is None:
        return "`never`"
    return f"<t:{timestamp_ms // 1000}:R>"


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
