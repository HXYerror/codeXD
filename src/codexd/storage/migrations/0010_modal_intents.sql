CREATE TABLE modal_intents (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('schedule_create', 'schedule_update', 'steer')),
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
    )
);

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
