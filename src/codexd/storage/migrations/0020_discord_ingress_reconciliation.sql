ALTER TABLE ingress_messages
ADD COLUMN discovery_kind TEXT NOT NULL DEFAULT 'live'
CHECK (discovery_kind IN ('live', 'backfill'));

CREATE TABLE discord_ingress_feature_state (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    activated_at INTEGER NOT NULL
);

INSERT INTO discord_ingress_feature_state(singleton, activated_at)
VALUES (1, CAST(strftime('%s', 'now') AS INTEGER) * 1000);

CREATE TABLE discord_ingress_checkpoints (
    id TEXT PRIMARY KEY,
    discord_guild_id TEXT NOT NULL,
    discord_channel_id TEXT NOT NULL,
    scope_kind TEXT NOT NULL CHECK (
        scope_kind IN ('parent_channel', 'conversation_thread')
    ),
    conversation_id TEXT REFERENCES conversations(id) ON DELETE CASCADE,
    discord_parent_channel_id TEXT,
    last_scanned_message_id TEXT NOT NULL,
    in_progress_barrier_id TEXT,
    in_progress_after_id TEXT,
    scan_state TEXT NOT NULL CHECK (
        scan_state IN ('idle', 'scanning', 'retry', 'blocked')
    ),
    last_scan_started_at INTEGER,
    last_scan_completed_at INTEGER,
    last_error_code TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(discord_guild_id, discord_channel_id),
    CHECK (
        (scope_kind = 'parent_channel'
         AND conversation_id IS NULL
         AND discord_parent_channel_id IS NULL)
        OR
        (scope_kind = 'conversation_thread'
         AND conversation_id IS NOT NULL
         AND discord_parent_channel_id IS NOT NULL)
    ),
    CHECK (
        (in_progress_barrier_id IS NULL AND in_progress_after_id IS NULL)
        OR
        (in_progress_barrier_id IS NOT NULL AND in_progress_after_id IS NOT NULL)
    )
);

CREATE INDEX discord_ingress_checkpoints_state_idx
ON discord_ingress_checkpoints(scan_state, updated_at);

CREATE TRIGGER discord_ingress_checkpoint_scope_immutable
BEFORE UPDATE OF discord_guild_id, discord_channel_id, scope_kind,
                 conversation_id, discord_parent_channel_id
ON discord_ingress_checkpoints
BEGIN
    SELECT RAISE(ABORT, 'Discord ingress checkpoint scope is immutable');
END;
