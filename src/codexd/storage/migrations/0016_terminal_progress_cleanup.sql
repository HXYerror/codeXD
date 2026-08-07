ALTER TABLE turn_progress_views
    ADD COLUMN cleanup_state TEXT NOT NULL DEFAULT 'active'
        CHECK (cleanup_state IN (
            'active', 'legacy_ineligible',
            'delete_pending', 'delete_failed', 'deleted'
        ));

ALTER TABLE turn_progress_views
    ADD COLUMN deleted_at INTEGER;

UPDATE turn_progress_views
SET cleanup_state = 'legacy_ineligible'
WHERE turn_id IN (
    SELECT id
    FROM turns
    WHERE state IN ('completed', 'failed', 'cancelled', 'interrupted')
);
