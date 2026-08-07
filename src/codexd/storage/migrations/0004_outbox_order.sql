ALTER TABLE discord_outbox ADD COLUMN enqueue_sequence INTEGER;

UPDATE discord_outbox
SET enqueue_sequence = rowid
WHERE enqueue_sequence IS NULL;

CREATE UNIQUE INDEX discord_outbox_enqueue_sequence_unique
    ON discord_outbox(enqueue_sequence);

CREATE TRIGGER discord_outbox_assign_enqueue_sequence
AFTER INSERT ON discord_outbox
WHEN NEW.enqueue_sequence IS NULL
BEGIN
    UPDATE discord_outbox
    SET enqueue_sequence = (
        SELECT COALESCE(MAX(enqueue_sequence), 0) + 1
        FROM discord_outbox
        WHERE id <> NEW.id
    )
    WHERE id = NEW.id;
END;
