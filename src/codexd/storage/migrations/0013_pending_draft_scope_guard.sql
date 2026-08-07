UPDATE schedule_drafts
SET state = 'expired',
    updated_at = MAX(updated_at, CAST(strftime('%s', 'now') AS INTEGER) * 1000)
WHERE state = 'pending'
  AND (discord_guild_id IS NULL OR discord_channel_id IS NULL);

CREATE TRIGGER schedule_drafts_scope_required_update
BEFORE UPDATE OF state, discord_guild_id, discord_channel_id ON schedule_drafts
WHEN NEW.state = 'pending'
 AND (NEW.discord_guild_id IS NULL OR NEW.discord_channel_id IS NULL)
BEGIN
    SELECT RAISE(ABORT, 'pending schedule draft requires Discord scope');
END;
