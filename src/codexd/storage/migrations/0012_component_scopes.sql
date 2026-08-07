ALTER TABLE schedule_drafts
ADD COLUMN discord_guild_id TEXT;

ALTER TABLE schedule_drafts
ADD COLUMN discord_channel_id TEXT;

UPDATE schedule_drafts
SET state = 'expired', payload_json = '{}', occurrences_json = '[]'
WHERE state = 'pending';

CREATE TRIGGER schedule_drafts_discord_scope_required_insert
BEFORE INSERT ON schedule_drafts
WHEN NEW.discord_guild_id IS NULL OR NEW.discord_channel_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'schedule draft Discord scope is required');
END;

CREATE TRIGGER schedule_drafts_discord_scope_immutable
BEFORE UPDATE OF discord_guild_id, discord_channel_id ON schedule_drafts
BEGIN
    SELECT RAISE(ABORT, 'schedule draft Discord scope is immutable');
END;
