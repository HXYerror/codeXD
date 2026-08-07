ALTER TABLE task_projections
    ADD COLUMN sender_thread_hash_version INTEGER NOT NULL DEFAULT 0
        CHECK (sender_thread_hash_version IN (0, 1));

ALTER TABLE task_projection_agents
    ADD COLUMN provider_agent_thread_hash_version INTEGER NOT NULL DEFAULT 0
        CHECK (provider_agent_thread_hash_version IN (0, 1));

CREATE TABLE projection_key_metadata (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    key_fingerprint TEXT NOT NULL,
    created_at INTEGER NOT NULL
);
