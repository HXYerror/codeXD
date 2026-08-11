-- codexd:foreign_keys_off
CREATE TABLE modal_intents_v19 (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (
        kind IN ('schedule_create', 'schedule_update', 'steer', 'side_query')
    ),
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE RESTRICT,
    turn_id TEXT REFERENCES turns(id) ON DELETE RESTRICT,
    schedule_id TEXT REFERENCES schedules(id) ON DELETE RESTRICT,
    expected_version INTEGER,
    discord_guild_id TEXT NOT NULL,
    discord_channel_id TEXT NOT NULL,
    owner_user_id TEXT NOT NULL,
    nonce_hash TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('open', 'consumed', 'expired')),
    consumed_interaction_id TEXT UNIQUE,
    expires_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    consumed_at INTEGER,
    CHECK (
        (kind = 'schedule_create'
         AND turn_id IS NULL
         AND schedule_id IS NULL
         AND expected_version IS NULL)
        OR
        (kind = 'schedule_update'
         AND turn_id IS NULL
         AND schedule_id IS NOT NULL
         AND expected_version IS NOT NULL)
        OR
        (kind = 'steer'
         AND turn_id IS NOT NULL
         AND schedule_id IS NULL
         AND expected_version IS NULL)
        OR
        (kind = 'side_query'
         AND turn_id IS NULL
         AND schedule_id IS NULL
         AND expected_version IS NULL)
    )
);

INSERT INTO modal_intents_v19
SELECT * FROM modal_intents;

DROP TABLE modal_intents;
ALTER TABLE modal_intents_v19 RENAME TO modal_intents;

CREATE INDEX idx_modal_intents_expiry
ON modal_intents(state, expires_at);

CREATE TRIGGER modal_intents_scope_immutable
BEFORE UPDATE OF kind, project_id, conversation_id, turn_id, schedule_id,
                 expected_version, discord_guild_id, discord_channel_id,
                 owner_user_id, nonce_hash, expires_at
ON modal_intents
BEGIN
    SELECT RAISE(ABORT, 'modal intent scope is immutable');
END;

CREATE TABLE side_queries (
    id TEXT PRIMARY KEY,
    interaction_id TEXT NOT NULL UNIQUE,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE RESTRICT,
    requested_by_user_id TEXT NOT NULL,
    question_hash TEXT NOT NULL,
    question_size INTEGER NOT NULL CHECK (question_size > 0),
    state TEXT NOT NULL CHECK (
        state IN ('accepted', 'running', 'completed', 'failed', 'interrupted')
    ),
    answer_hash TEXT,
    answer_size INTEGER,
    terminal_code TEXT,
    error_code TEXT,
    accepted_boot_id TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    started_at INTEGER,
    completed_at INTEGER,
    updated_at INTEGER NOT NULL,
    CHECK (
        (state IN ('accepted', 'running')
         AND answer_hash IS NULL
         AND answer_size IS NULL
         AND completed_at IS NULL)
        OR
        (state = 'completed'
         AND answer_hash IS NOT NULL
         AND answer_size IS NOT NULL
         AND answer_size > 0
         AND terminal_code IS NOT NULL
         AND completed_at IS NOT NULL)
        OR
        (state IN ('failed', 'interrupted')
         AND answer_hash IS NULL
         AND answer_size IS NULL
         AND terminal_code IS NOT NULL
         AND completed_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX one_active_side_query_per_user
ON side_queries(conversation_id, requested_by_user_id)
WHERE state IN ('accepted', 'running');

CREATE INDEX side_queries_state_idx
ON side_queries(state, updated_at);
