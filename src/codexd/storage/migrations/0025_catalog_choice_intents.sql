CREATE TABLE catalog_choice_intents (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL CHECK (kind IN ('model', 'reasoning')),
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE RESTRICT,
    conversation_id TEXT NOT NULL REFERENCES conversations(id) ON DELETE RESTRICT,
    discord_guild_id TEXT NOT NULL,
    discord_channel_id TEXT NOT NULL,
    owner_user_id TEXT NOT NULL,
    runtime_generation INTEGER NOT NULL CHECK (runtime_generation > 0),
    catalog_hash TEXT NOT NULL CHECK (length(catalog_hash) = 64),
    allowed_values_json TEXT NOT NULL CHECK (json_valid(allowed_values_json)),
    nonce_hash TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('open', 'consumed', 'expired')),
    consumed_interaction_id TEXT UNIQUE,
    expires_at INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    consumed_at INTEGER
);

CREATE INDEX catalog_choice_intents_expiry_idx
ON catalog_choice_intents(state, expires_at);
