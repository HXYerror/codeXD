CREATE TABLE turn_progress_views (
    turn_id TEXT PRIMARY KEY REFERENCES turns(id),
    destination_key TEXT NOT NULL,
    discord_message_id TEXT,
    content_revision INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL
        CHECK (state IN ('queued', 'running', 'cancelling', 'terminal')),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE schedule_drafts (
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES conversations(id),
    owner_user_id TEXT NOT NULL,
    action TEXT NOT NULL CHECK (action IN ('create', 'update')),
    schedule_id TEXT REFERENCES schedules(id),
    expected_version INTEGER,
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)),
    occurrences_json TEXT NOT NULL CHECK (json_valid(occurrences_json)),
    state TEXT NOT NULL
        CHECK (state IN ('pending', 'confirmed', 'cancelled', 'expired')),
    component_nonce_hash TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    CHECK (
        (action = 'create' AND schedule_id IS NULL AND expected_version IS NULL)
        OR
        (action = 'update' AND schedule_id IS NOT NULL AND expected_version IS NOT NULL)
    )
);

CREATE INDEX schedule_drafts_expiry_idx
    ON schedule_drafts(state, expires_at);
