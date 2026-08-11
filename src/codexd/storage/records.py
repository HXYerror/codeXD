from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from codexd.domain.conversations import ConversationState, SandboxProfile
from codexd.domain.schedules import MisfirePolicy, ScheduleKind, ScheduleState
from codexd.domain.turns import InterruptOrigin, TurnSource, TurnState


@dataclass(frozen=True)
class ProjectRecord:
    id: str
    name: str
    root_path: Path
    sandbox_profile: SandboxProfile
    default_model: str | None
    default_reasoning_effort: str | None
    default_reasoning_summary: str | None
    default_personality: str | None
    default_service_tier: str | None
    default_web_search_mode: str


@dataclass(frozen=True)
class ConversationRecord:
    id: str
    project_id: str
    discord_thread_id: int
    discord_guild_id: int
    discord_parent_channel_id: int
    owner_user_id: int
    state: ConversationState
    active_revision_id: str | None
    sandbox_profile: SandboxProfile
    model_override: str | None
    reasoning_effort_override: str | None
    reasoning_summary_override: str | None
    personality_override: str | None
    service_tier_override: str | None
    web_search_mode: str
    provider_barrier_kind: str | None


@dataclass(frozen=True)
class ThreadRevisionRecord:
    id: str
    conversation_id: str
    provider_thread_id: str
    provider_session_id: str
    provider_forked_from_thread_id: str | None
    provider_parent_thread_id: str | None
    name: str | None
    parent_revision_id: str | None
    state: str
    thread_config_json: str
    provider_version: str
    created_at: int
    activated_at: int | None
    archived_at: int | None


@dataclass(frozen=True)
class RuntimeLeaseRecord:
    id: str
    scope_key: str
    generation: int
    state: str


@dataclass(frozen=True)
class TurnRecord:
    id: str
    conversation_id: str
    thread_revision_id: str | None
    runtime_lease_id: str | None
    runtime_generation: int | None
    provider_turn_id: str | None
    source_kind: TurnSource
    state: TurnState
    interrupt_origin: InterruptOrigin | None
    interrupt_reason: str | None
    input_message_id: str | None
    schedule_fire_id: str | None
    input_hash: str
    input_summary: str
    queued_input_text: str | None
    queued_skill_inputs_json: str | None
    effective_model: str | None
    effective_reasoning_effort: str | None
    effective_reasoning_summary: str | None
    effective_personality: str | None
    effective_service_tier: str | None
    effective_web_search_mode: str
    effective_sandbox: SandboxProfile
    queued_at: int
    started_at: int | None
    ended_at: int | None
    terminal_code: str | None
    error_code: str | None
    error_message_redacted: str | None
    usage_scope: str | None


@dataclass(frozen=True)
class OutboxRecord:
    id: str
    destination_key: str
    operation: str
    payload_json: str
    delivery_marker: str
    state: str
    attempts: int
    lease_owner: str


@dataclass(frozen=True)
class TurnProgressDeleteTarget:
    destination_key: str
    discord_message_id: str | None


@dataclass(frozen=True)
class RenderPlanRecord:
    turn_id: str
    source_sha256: str
    plan_json: str
    retention_until: int


@dataclass(frozen=True)
class IngressMessageRecord:
    id: str
    discord_message_id: str
    accepted_content_hash: str
    accepted_attachment_manifest_hash: str
    project_id: str
    discord_guild_id: int
    discord_channel_id: int
    conversation_id: str | None
    state: str
    turn_id: str | None
    accepted_boot_id: str
    error_code: str | None


@dataclass(frozen=True)
class CommandIntentRecord:
    interaction_id: str
    command_name: str
    request_hash: str
    project_id: str | None
    conversation_id: str | None
    turn_id: str | None
    state: str
    result_json: str | None
    effect_kind: str | None
    effect_correlation_id: str | None
    accepted_boot_id: str


@dataclass(frozen=True)
class ModalIntentRecord:
    id: str
    kind: str
    project_id: str
    conversation_id: str
    turn_id: str | None
    schedule_id: str | None
    expected_version: int | None
    discord_guild_id: int
    discord_channel_id: int
    owner_user_id: int
    state: str
    consumed_interaction_id: str | None
    expires_at: int


@dataclass(frozen=True)
class ScheduleRecord:
    id: str
    conversation_id: str
    name: str
    kind: ScheduleKind
    expression: str
    timezone: str
    misfire_policy: MisfirePolicy
    prompt_text: str | None
    prompt_hash: str
    skill_inputs_json: str | None
    state: ScheduleState
    next_due_at: int | None
    last_due_at: int | None
    version: int
    created_by_user_id: int


@dataclass(frozen=True)
class ScheduleDraftRecord:
    id: str
    conversation_id: str
    owner_user_id: int
    discord_guild_id: int
    discord_channel_id: int
    action: str
    schedule_id: str | None
    expected_version: int | None
    payload_json: str
    occurrences_json: str
    state: str
    component_nonce_hash: str
    expires_at: int


@dataclass(frozen=True)
class ScheduleFireRecord:
    id: str
    trigger_kind: str
    scheduled_for: int | None
    scheduled_local: str | None
    state: str
    turn_id: str | None
    error_code: str | None
    created_at: int


@dataclass(frozen=True)
class MaterializedScheduleTurn:
    fire_id: str
    turn_id: str | None
    conversation_id: str
    fire_state: str
