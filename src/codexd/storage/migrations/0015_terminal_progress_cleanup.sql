ALTER TABLE turn_progress_views
    ADD COLUMN remote_message_seen_at INTEGER;

ALTER TABLE turn_progress_views
    ADD COLUMN cleanup_state TEXT NOT NULL DEFAULT 'active'
        CHECK (cleanup_state IN (
            'active', 'delete_pending', 'delete_failed', 'deleted'
        ));

ALTER TABLE turn_progress_views
    ADD COLUMN deleted_at INTEGER;

UPDATE turn_progress_views
SET remote_message_seen_at = updated_at
WHERE discord_message_id IS NOT NULL;

CREATE INDEX turn_progress_views_cleanup_idx
    ON turn_progress_views(cleanup_state, updated_at);
