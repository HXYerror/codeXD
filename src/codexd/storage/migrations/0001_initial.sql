CREATE TABLE projects (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    root_path TEXT NOT NULL UNIQUE,
    root_path_casefold TEXT NOT NULL,
    discord_guild_id TEXT NOT NULL,
    discord_channel_id TEXT NOT NULL UNIQUE,
    enabled INTEGER NOT NULL DEFAULT 1 CHECK (enabled IN (0, 1)),
    default_model TEXT,
    default_reasoning_effort TEXT,
    default_reasoning_summary TEXT,
    default_personality TEXT,
    default_service_tier TEXT,
    default_web_search_mode TEXT NOT NULL DEFAULT 'cached'
        CHECK (default_web_search_mode IN (
            'cached', 'indexed', 'live', 'disabled', 'provider_default_uncontrolled'
        )),
    sandbox_profile TEXT NOT NULL DEFAULT 'full_access'
        CHECK (sandbox_profile IN ('full_access', 'workspace_write', 'read_only')),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE UNIQUE INDEX projects_root_casefold_unique
    ON projects(root_path_casefold);

CREATE TABLE conversations (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id),
    discord_thread_id TEXT NOT NULL UNIQUE,
    owner_user_id TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'uninitialized'
        CHECK (state IN ('uninitialized', 'active', 'archived', 'blocked', 'deleted')),
    active_revision_id TEXT,
    mailbox_version INTEGER NOT NULL DEFAULT 0,
    model_override TEXT,
    reasoning_effort_override TEXT,
    reasoning_summary_override TEXT,
    personality_override TEXT,
    service_tier_override TEXT,
    web_search_mode TEXT NOT NULL DEFAULT 'cached'
        CHECK (web_search_mode IN (
            'cached', 'indexed', 'live', 'disabled', 'provider_default_uncontrolled'
        )),
    sandbox_profile TEXT NOT NULL DEFAULT 'full_access'
        CHECK (sandbox_profile IN ('full_access', 'workspace_write', 'read_only')),
    provider_barrier_kind TEXT
        CHECK (provider_barrier_kind IS NULL OR provider_barrier_kind IN (
            'compact', 'external_active', 'unknown_effect'
        )),
    provider_barrier_intent_id TEXT,
    provider_barrier_since INTEGER,
    last_activity_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX conversations_project_idx ON conversations(project_id);

CREATE TABLE thread_revisions (
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES conversations(id),
    provider_thread_id TEXT NOT NULL UNIQUE,
    provider_session_id TEXT NOT NULL,
    provider_forked_from_thread_id TEXT,
    provider_parent_thread_id TEXT,
    name TEXT,
    parent_revision_id TEXT REFERENCES thread_revisions(id),
    state TEXT NOT NULL
        CHECK (state IN ('active', 'archived', 'superseded', 'blocked')),
    thread_config_json TEXT NOT NULL CHECK (json_valid(thread_config_json)),
    requested_resume_id TEXT,
    provider_version TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    activated_at INTEGER,
    archived_at INTEGER
);

CREATE UNIQUE INDEX one_active_revision_per_conversation
    ON thread_revisions(conversation_id)
    WHERE state = 'active';

CREATE INDEX thread_revisions_conversation_idx
    ON thread_revisions(conversation_id, created_at);

CREATE TABLE runtime_leases (
    id TEXT PRIMARY KEY,
    scope_kind TEXT NOT NULL CHECK (scope_kind IN ('project', 'shared')),
    scope_key TEXT NOT NULL,
    project_id TEXT REFERENCES projects(id),
    generation INTEGER NOT NULL CHECK (generation > 0),
    state TEXT NOT NULL
        CHECK (state IN ('starting', 'ready', 'unhealthy', 'stopping', 'stopped', 'failed')),
    sdk_version TEXT,
    runtime_version TEXT,
    capability_hash TEXT,
    environment_hash TEXT NOT NULL,
    failure_code TEXT,
    started_at INTEGER NOT NULL,
    heartbeat_at INTEGER NOT NULL,
    ended_at INTEGER,
    UNIQUE(scope_key, generation),
    CHECK (
        (scope_kind = 'project' AND project_id IS NOT NULL AND scope_key = project_id)
        OR (scope_kind = 'shared' AND project_id IS NULL AND scope_key = 'shared')
    )
);

CREATE INDEX runtime_leases_scope_idx
    ON runtime_leases(scope_key, generation DESC);

CREATE TABLE schedules (
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES conversations(id),
    name TEXT NOT NULL,
    kind TEXT NOT NULL CHECK (kind IN ('once', 'cron')),
    expression TEXT NOT NULL,
    timezone TEXT NOT NULL,
    misfire_policy TEXT NOT NULL CHECK (misfire_policy IN ('skip', 'latest', 'all')),
    prompt_text TEXT,
    prompt_hash TEXT NOT NULL,
    skill_inputs_json TEXT CHECK (skill_inputs_json IS NULL OR json_valid(skill_inputs_json)),
    state TEXT NOT NULL
        CHECK (state IN ('active', 'paused', 'completed', 'blocked', 'deleted')),
    next_due_at INTEGER,
    last_due_at INTEGER,
    version INTEGER NOT NULL DEFAULT 1,
    created_by_user_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    deleted_at INTEGER,
    UNIQUE(conversation_id, name)
);

CREATE INDEX schedules_due_idx
    ON schedules(state, next_due_at)
    WHERE state = 'active';

CREATE TABLE schedule_fires (
    id TEXT PRIMARY KEY,
    schedule_id TEXT NOT NULL REFERENCES schedules(id),
    occurrence_key TEXT NOT NULL,
    trigger_kind TEXT NOT NULL CHECK (trigger_kind IN ('timer', 'manual', 'misfire')),
    scheduled_for INTEGER,
    scheduled_local TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('due', 'materialized', 'skipped', 'blocked')),
    turn_id TEXT UNIQUE,
    error_code TEXT,
    created_at INTEGER NOT NULL,
    materialized_at INTEGER,
    UNIQUE(schedule_id, occurrence_key)
);

CREATE TABLE turns (
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES conversations(id),
    thread_revision_id TEXT REFERENCES thread_revisions(id),
    runtime_lease_id TEXT REFERENCES runtime_leases(id),
    runtime_generation INTEGER,
    provider_turn_id TEXT UNIQUE,
    source_kind TEXT NOT NULL CHECK (source_kind IN ('discord', 'schedule')),
    input_message_id TEXT UNIQUE,
    schedule_fire_id TEXT UNIQUE REFERENCES schedule_fires(id),
    state TEXT NOT NULL
        CHECK (state IN (
            'queued', 'starting', 'running', 'cancelling',
            'completed', 'failed', 'cancelled', 'interrupted'
        )),
    interrupt_origin TEXT CHECK (
        interrupt_origin IS NULL OR interrupt_origin IN ('user', 'shutdown', 'runtime')
    ),
    interrupt_reason TEXT,
    input_hash TEXT NOT NULL,
    queued_input_text TEXT,
    queued_skill_inputs_json TEXT
        CHECK (queued_skill_inputs_json IS NULL OR json_valid(queued_skill_inputs_json)),
    effective_skill_names_json TEXT
        CHECK (effective_skill_names_json IS NULL OR json_valid(effective_skill_names_json)),
    effective_model TEXT,
    effective_reasoning_effort TEXT,
    effective_reasoning_summary TEXT,
    effective_personality TEXT,
    effective_service_tier TEXT,
    effective_web_search_mode TEXT NOT NULL,
    effective_sandbox TEXT NOT NULL
        CHECK (effective_sandbox IN ('full_access', 'workspace_write', 'read_only')),
    effective_approval_mode TEXT NOT NULL CHECK (effective_approval_mode = 'auto_review'),
    queued_at INTEGER NOT NULL,
    started_at INTEGER,
    ended_at INTEGER,
    terminal_code TEXT,
    error_code TEXT,
    error_message_redacted TEXT,
    usage_scope TEXT,
    CHECK (
        (source_kind = 'discord' AND input_message_id IS NOT NULL AND schedule_fire_id IS NULL)
        OR (source_kind = 'schedule' AND input_message_id IS NULL AND schedule_fire_id IS NOT NULL)
    ),
    CHECK (
        (runtime_generation IS NULL AND runtime_lease_id IS NULL)
        OR (runtime_generation IS NOT NULL AND runtime_lease_id IS NOT NULL)
    )
);

CREATE UNIQUE INDEX one_active_turn_per_conversation
    ON turns(conversation_id)
    WHERE state IN ('starting', 'running', 'cancelling');

CREATE INDEX turns_mailbox_idx
    ON turns(conversation_id, state, queued_at, id);

CREATE INDEX turns_runtime_generation_idx
    ON turns(runtime_lease_id, runtime_generation, state);

CREATE TRIGGER schedule_fires_turn_fk_insert
BEFORE INSERT ON schedule_fires
WHEN NEW.turn_id IS NOT NULL
BEGIN
    SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM turns WHERE id = NEW.turn_id)
        THEN RAISE(ABORT, 'schedule fire turn does not exist') END;
END;

CREATE TRIGGER schedule_fires_turn_fk_update
BEFORE UPDATE OF turn_id ON schedule_fires
WHEN NEW.turn_id IS NOT NULL
BEGIN
    SELECT CASE WHEN NOT EXISTS (SELECT 1 FROM turns WHERE id = NEW.turn_id)
        THEN RAISE(ABORT, 'schedule fire turn does not exist') END;
END;

CREATE TABLE events (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    turn_id TEXT REFERENCES turns(id),
    project_id TEXT NOT NULL REFERENCES projects(id),
    conversation_id TEXT REFERENCES conversations(id),
    runtime_generation INTEGER,
    provider_event_id TEXT,
    local_event_index INTEGER,
    kind TEXT NOT NULL,
    schema_version INTEGER NOT NULL,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    raw_type TEXT,
    raw_hash TEXT,
    raw_size INTEGER,
    occurred_at INTEGER NOT NULL,
    recorded_at INTEGER NOT NULL,
    UNIQUE(turn_id, local_event_index)
);

CREATE UNIQUE INDEX events_provider_id_unique
    ON events(turn_id, provider_event_id)
    WHERE provider_event_id IS NOT NULL;

CREATE INDEX events_turn_sequence_idx ON events(turn_id, sequence);
CREATE INDEX events_project_sequence_idx ON events(project_id, sequence);

CREATE TABLE message_projections (
    id TEXT PRIMARY KEY,
    turn_id TEXT NOT NULL UNIQUE REFERENCES turns(id),
    content_revision INTEGER NOT NULL,
    content_ast_json TEXT NOT NULL CHECK (json_valid(content_ast_json)),
    plain_text TEXT NOT NULL,
    is_final INTEGER NOT NULL CHECK (is_final IN (0, 1)),
    last_event_sequence INTEGER NOT NULL REFERENCES events(sequence)
);

CREATE TABLE tool_projections (
    id TEXT PRIMARY KEY,
    turn_id TEXT NOT NULL REFERENCES turns(id),
    provider_item_id TEXT,
    kind TEXT NOT NULL,
    label TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('started', 'completed', 'failed')),
    summary_json TEXT NOT NULL CHECK (json_valid(summary_json)),
    last_event_sequence INTEGER NOT NULL REFERENCES events(sequence),
    UNIQUE(turn_id, kind, provider_item_id)
);

CREATE TABLE task_projections (
    id TEXT PRIMARY KEY,
    turn_id TEXT NOT NULL REFERENCES turns(id),
    source_type TEXT NOT NULL CHECK (
        source_type IN ('collab_agent_tool_call', 'subagent_activity')
    ),
    provider_item_id TEXT NOT NULL,
    provider_correlation_hash TEXT NOT NULL UNIQUE,
    parent_task_id TEXT REFERENCES task_projections(id),
    operation TEXT NOT NULL,
    tool_status TEXT,
    state TEXT NOT NULL CHECK (
        state IN (
            'pending', 'running', 'interrupted', 'completed', 'errored',
            'shutdown', 'not_found', 'unknown'
        )
    ),
    display_title TEXT NOT NULL,
    safe_status_summary TEXT,
    sender_thread_hash TEXT,
    model TEXT,
    reasoning_effort TEXT,
    prompt_hash TEXT,
    prompt_size INTEGER,
    error_code TEXT,
    last_event_sequence INTEGER NOT NULL REFERENCES events(sequence),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    ended_at INTEGER,
    UNIQUE(turn_id, source_type, provider_item_id)
);

CREATE TABLE task_projection_agents (
    task_projection_id TEXT NOT NULL REFERENCES task_projections(id),
    provider_agent_thread_hash TEXT NOT NULL,
    agent_label TEXT NOT NULL,
    state TEXT NOT NULL CHECK (
        state IN (
            'pending', 'running', 'interrupted', 'completed',
            'errored', 'shutdown', 'not_found'
        )
    ),
    safe_message TEXT,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(task_projection_id, provider_agent_thread_hash)
);

CREATE TABLE task_card_views (
    id TEXT PRIMARY KEY,
    task_projection_id TEXT NOT NULL UNIQUE REFERENCES task_projections(id),
    destination_key TEXT NOT NULL,
    discord_message_id TEXT,
    display_state TEXT NOT NULL DEFAULT 'collapsed'
        CHECK (display_state IN ('collapsed', 'expanded')),
    content_revision INTEGER NOT NULL DEFAULT 1,
    component_nonce_hash TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE discord_outbox (
    id TEXT PRIMARY KEY,
    event_sequence INTEGER REFERENCES events(sequence),
    destination_key TEXT NOT NULL,
    operation TEXT NOT NULL CHECK (
        operation IN ('create_thread', 'send', 'edit', 'delete', 'upload', 'unarchive_thread')
    ),
    depends_on_outbox_id TEXT REFERENCES discord_outbox(id),
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    dedupe_key TEXT NOT NULL UNIQUE,
    coalesce_key TEXT,
    delivery_marker TEXT NOT NULL,
    state TEXT NOT NULL
        CHECK (state IN (
            'pending', 'sending', 'reconciling', 'retry',
            'sent', 'dead_letter', 'superseded'
        )),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at INTEGER NOT NULL,
    discord_message_id TEXT,
    lease_owner TEXT,
    lease_expires_at INTEGER,
    last_error_code TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX discord_outbox_ready_idx
    ON discord_outbox(state, next_attempt_at, destination_key, created_at);

CREATE TABLE ingress_messages (
    id TEXT PRIMARY KEY,
    discord_message_id TEXT NOT NULL UNIQUE,
    accepted_content_hash TEXT NOT NULL,
    accepted_attachment_manifest_hash TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id),
    conversation_id TEXT REFERENCES conversations(id),
    state TEXT NOT NULL
        CHECK (state IN ('pending_thread', 'pending_preflight', 'ready', 'rejected')),
    turn_id TEXT UNIQUE REFERENCES turns(id),
    thread_creation_outbox_id TEXT REFERENCES discord_outbox(id),
    progress_outbox_id TEXT REFERENCES discord_outbox(id),
    error_code TEXT,
    accepted_boot_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    completed_at INTEGER
);

CREATE TABLE command_intents (
    interaction_id TEXT PRIMARY KEY,
    command_name TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    project_id TEXT REFERENCES projects(id),
    conversation_id TEXT REFERENCES conversations(id),
    turn_id TEXT REFERENCES turns(id),
    state TEXT NOT NULL
        CHECK (state IN (
            'accepted', 'effect_in_flight', 'reconciling',
            'succeeded', 'rejected', 'failed', 'unknown'
        )),
    result_json TEXT CHECK (result_json IS NULL OR json_valid(result_json)),
    effect_kind TEXT,
    effect_correlation_id TEXT,
    accepted_boot_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    completed_at INTEGER
);

CREATE TABLE attachments (
    id TEXT PRIMARY KEY,
    ingress_id TEXT REFERENCES ingress_messages(id),
    turn_id TEXT REFERENCES turns(id),
    kind TEXT NOT NULL CHECK (
        kind IN ('input_image', 'table_md', 'table_csv', 'table_png', 'diagnostic')
    ),
    ordinal INTEGER,
    relative_path TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    normalized_sha256 TEXT NOT NULL,
    size_bytes INTEGER NOT NULL,
    mime_type TEXT NOT NULL,
    width INTEGER,
    height INTEGER,
    source_name_sanitized TEXT NOT NULL,
    retention_until INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    CHECK (ingress_id IS NOT NULL OR turn_id IS NOT NULL)
);

CREATE INDEX attachments_turn_idx ON attachments(turn_id, kind, ordinal);

CREATE TABLE incidents (
    id TEXT PRIMARY KEY,
    severity TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'error', 'critical')),
    code TEXT NOT NULL,
    project_id TEXT REFERENCES projects(id),
    conversation_id TEXT REFERENCES conversations(id),
    turn_id TEXT REFERENCES turns(id),
    schedule_id TEXT REFERENCES schedules(id),
    summary TEXT NOT NULL,
    details_json TEXT NOT NULL CHECK (json_valid(details_json)),
    occurrence_count INTEGER NOT NULL DEFAULT 1,
    first_seen_at INTEGER NOT NULL,
    last_seen_at INTEGER NOT NULL,
    resolved_at INTEGER
);

CREATE INDEX incidents_open_idx
    ON incidents(severity, last_seen_at)
    WHERE resolved_at IS NULL;

CREATE TABLE audit_log (
    sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    id TEXT NOT NULL UNIQUE,
    actor_kind TEXT NOT NULL,
    actor_id_hash TEXT,
    action TEXT NOT NULL,
    project_id TEXT REFERENCES projects(id),
    conversation_id TEXT REFERENCES conversations(id),
    turn_id TEXT REFERENCES turns(id),
    schedule_id TEXT REFERENCES schedules(id),
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    occurred_at INTEGER NOT NULL
);

CREATE TABLE daemon_leases (
    lease_name TEXT PRIMARY KEY,
    boot_id TEXT NOT NULL,
    pid INTEGER NOT NULL,
    process_start_token TEXT NOT NULL,
    acquired_at INTEGER NOT NULL,
    heartbeat_at INTEGER NOT NULL
);

CREATE INDEX audit_log_occurred_idx ON audit_log(occurred_at);

