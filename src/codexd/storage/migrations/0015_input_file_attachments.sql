-- codexd:foreign_keys_off
CREATE TABLE attachments_v15 (
    id TEXT PRIMARY KEY,
    ingress_id TEXT REFERENCES ingress_messages(id),
    turn_id TEXT REFERENCES turns(id),
    kind TEXT NOT NULL CHECK (
        kind IN (
            'input_image', 'input_file',
            'table_md', 'table_csv', 'table_png', 'diagnostic'
        )
    ),
    ordinal INTEGER CHECK (ordinal IS NULL OR ordinal >= 0),
    relative_path TEXT NOT NULL CHECK (
        length(relative_path) > 0
        AND instr(relative_path, char(0)) = 0
        AND substr(relative_path, 1, 1) NOT IN ('/', '\')
        AND relative_path NOT GLOB '[A-Za-z]:*'
        AND instr('/' || replace(relative_path, '\', '/') || '/', '/../') = 0
        AND instr('/' || replace(relative_path, '\', '/') || '/', '/./') = 0
        AND instr(replace(relative_path, '\', '/'), '//') = 0
    ),
    source_sha256 TEXT NOT NULL,
    normalized_sha256 TEXT NOT NULL,
    size_bytes INTEGER NOT NULL CHECK (size_bytes >= 0),
    mime_type TEXT,
    width INTEGER,
    height INTEGER,
    source_name_sanitized TEXT NOT NULL,
    retention_until INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    CHECK (ingress_id IS NOT NULL OR turn_id IS NOT NULL),
    CHECK (
        kind <> 'input_image'
        OR (
            ordinal IS NOT NULL
            AND mime_type IS NOT NULL
            AND width IS NOT NULL
            AND height IS NOT NULL
        )
    ),
    CHECK (
        kind <> 'input_file'
        OR (
            ordinal IS NOT NULL
            AND substr(replace(relative_path, '\', '/'), 1, 18)
                = 'attachments/input/'
            AND length(replace(relative_path, '\', '/')) > 18
            AND instr(substr(replace(relative_path, '\', '/'), 19), '/') = 0
            AND width IS NULL
            AND height IS NULL
            AND source_sha256 = normalized_sha256
            AND length(source_sha256) = 64
            AND source_sha256 NOT GLOB '*[^0-9a-f]*'
            AND size_bytes > 0
            AND length(source_name_sanitized) BETWEEN 1 AND 128
            AND instr(source_name_sanitized, '/') = 0
            AND instr(source_name_sanitized, '\') = 0
            AND instr(source_name_sanitized, char(0)) = 0
        )
    )
);

INSERT INTO attachments_v15(
    id, ingress_id, turn_id, kind, ordinal, relative_path,
    source_sha256, normalized_sha256, size_bytes, mime_type,
    width, height, source_name_sanitized, retention_until, created_at
)
SELECT
    id, ingress_id, turn_id, kind, ordinal, relative_path,
    source_sha256, normalized_sha256, size_bytes, mime_type,
    width, height, source_name_sanitized, retention_until, created_at
FROM attachments;

DROP TABLE attachments;
ALTER TABLE attachments_v15 RENAME TO attachments;

CREATE INDEX attachments_turn_idx ON attachments(turn_id, kind, ordinal);

CREATE UNIQUE INDEX attachments_turn_input_ordinal_unique
    ON attachments(turn_id, ordinal)
    WHERE turn_id IS NOT NULL
      AND kind IN ('input_image', 'input_file');

CREATE UNIQUE INDEX attachments_ingress_input_ordinal_unique
    ON attachments(ingress_id, ordinal)
    WHERE ingress_id IS NOT NULL
      AND kind IN ('input_image', 'input_file');
