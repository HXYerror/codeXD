CREATE TABLE materialized_attachments (
    id TEXT PRIMARY KEY,
    attachment_id TEXT NOT NULL UNIQUE REFERENCES attachments(id) ON DELETE CASCADE,
    turn_id TEXT NOT NULL REFERENCES turns(id) ON DELETE CASCADE,
    kind TEXT NOT NULL CHECK (kind IN ('file', 'zip')),
    root_relative_path TEXT NOT NULL CHECK (
        root_relative_path = replace(root_relative_path, '\', '/')
        AND substr(root_relative_path, 1, 25) = 'attachments/materialized/'
        AND instr('/' || root_relative_path || '/', '/../') = 0
        AND instr('/' || root_relative_path || '/', '/./') = 0
    ),
    manifest_json TEXT NOT NULL CHECK (json_valid(manifest_json)),
    manifest_hash TEXT NOT NULL CHECK (
        length(manifest_hash) = 64
        AND manifest_hash NOT GLOB '*[^0-9a-f]*'
    ),
    file_count INTEGER NOT NULL CHECK (file_count > 0),
    total_bytes INTEGER NOT NULL CHECK (total_bytes > 0),
    retention_until INTEGER NOT NULL CHECK (retention_until > 0),
    created_at INTEGER NOT NULL CHECK (created_at > 0)
);

CREATE INDEX materialized_attachments_retention_idx
ON materialized_attachments(retention_until, turn_id);

CREATE INDEX materialized_attachments_turn_idx
ON materialized_attachments(turn_id, attachment_id);
