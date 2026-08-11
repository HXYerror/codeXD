ALTER TABLE ingress_messages
ADD COLUMN requested_by_user_id TEXT;

ALTER TABLE turns
ADD COLUMN requested_by_user_id TEXT;

ALTER TABLE thread_revisions
ADD COLUMN dynamic_tools_enabled INTEGER NOT NULL DEFAULT 0
CHECK (dynamic_tools_enabled IN (0, 1));

ALTER TABLE schedule_drafts
ADD COLUMN confirmation_message_id TEXT;

ALTER TABLE schedule_drafts
ADD COLUMN confirmation_outbox_id TEXT REFERENCES discord_outbox(id);

CREATE TRIGGER schedule_drafts_confirmation_outbox_immutable
BEFORE UPDATE OF confirmation_outbox_id ON schedule_drafts
WHEN OLD.confirmation_outbox_id IS NOT NEW.confirmation_outbox_id
BEGIN
    SELECT RAISE(ABORT, 'schedule draft confirmation outbox is immutable');
END;

CREATE TRIGGER schedule_drafts_confirmation_message_immutable
BEFORE UPDATE OF confirmation_message_id ON schedule_drafts
WHEN OLD.confirmation_message_id IS NOT NULL
 AND OLD.confirmation_message_id IS NOT NEW.confirmation_message_id
BEGIN
    SELECT RAISE(ABORT, 'schedule draft confirmation message is immutable');
END;

CREATE TABLE dynamic_tool_invocations (
    id TEXT PRIMARY KEY,
    turn_id TEXT NOT NULL REFERENCES turns(id) ON DELETE CASCADE,
    runtime_generation INTEGER NOT NULL,
    provider_thread_id TEXT NOT NULL,
    provider_turn_id TEXT NOT NULL,
    provider_call_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    tool_name TEXT NOT NULL,
    arguments_hash TEXT NOT NULL,
    success INTEGER NOT NULL CHECK (success IN (0, 1)),
    result_json TEXT NOT NULL CHECK (json_valid(result_json)),
    draft_id TEXT REFERENCES schedule_drafts(id) ON DELETE CASCADE,
    outbox_id TEXT REFERENCES discord_outbox(id),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE(
        runtime_generation,
        provider_thread_id,
        provider_turn_id,
        provider_call_id
    ),
    CHECK (
        (success = 1 AND draft_id IS NOT NULL AND outbox_id IS NOT NULL)
        OR
        (success = 0 AND draft_id IS NULL AND outbox_id IS NULL)
    )
);

CREATE INDEX dynamic_tool_invocations_turn_idx
ON dynamic_tool_invocations(turn_id, created_at);
