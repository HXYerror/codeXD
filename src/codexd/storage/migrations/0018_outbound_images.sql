UPDATE thread_revisions
SET dynamic_tools_enabled = 0
WHERE dynamic_tools_enabled = 1;

CREATE TABLE outbound_image_invocations (
    id TEXT PRIMARY KEY,
    turn_id TEXT NOT NULL REFERENCES turns(id) ON DELETE CASCADE,
    runtime_generation INTEGER NOT NULL,
    provider_thread_id TEXT NOT NULL,
    provider_turn_id TEXT NOT NULL,
    provider_call_id TEXT NOT NULL,
    arguments_hash TEXT NOT NULL,
    success INTEGER NOT NULL CHECK (success IN (0, 1)),
    result_json TEXT NOT NULL CHECK (json_valid(result_json)),
    artifact_ordinal INTEGER,
    relative_path TEXT,
    source_sha256 TEXT,
    normalized_sha256 TEXT,
    size_bytes INTEGER,
    width INTEGER,
    height INTEGER,
    media_type TEXT,
    display_name TEXT,
    description TEXT,
    state TEXT NOT NULL CHECK (state IN ('rejected', 'registered')),
    retention_until INTEGER,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(
        runtime_generation,
        provider_thread_id,
        provider_turn_id,
        provider_call_id
    ),
    CHECK (
        (
            success = 0
            AND state = 'rejected'
            AND artifact_ordinal IS NULL
            AND relative_path IS NULL
            AND source_sha256 IS NULL
            AND normalized_sha256 IS NULL
            AND size_bytes IS NULL
            AND width IS NULL
            AND height IS NULL
            AND media_type IS NULL
            AND display_name IS NULL
            AND description IS NULL
            AND retention_until IS NULL
        )
        OR
        (
            success = 1
            AND state = 'registered'
            AND artifact_ordinal IS NOT NULL
            AND artifact_ordinal >= 0
            AND relative_path IS NOT NULL
            AND source_sha256 IS NOT NULL
            AND normalized_sha256 IS NOT NULL
            AND size_bytes IS NOT NULL
            AND size_bytes > 0
            AND width IS NOT NULL
            AND width > 0
            AND height IS NOT NULL
            AND height > 0
            AND media_type = 'image/png'
            AND display_name IS NOT NULL
            AND description IS NOT NULL
            AND retention_until IS NOT NULL
        )
    )
);

CREATE UNIQUE INDEX outbound_image_turn_ordinal_unique
ON outbound_image_invocations(turn_id, artifact_ordinal)
WHERE success = 1;

CREATE INDEX outbound_image_retention_idx
ON outbound_image_invocations(state, retention_until);
