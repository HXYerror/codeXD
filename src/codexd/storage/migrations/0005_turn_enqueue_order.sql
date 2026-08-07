ALTER TABLE turns ADD COLUMN enqueue_sequence INTEGER;

UPDATE turns
SET enqueue_sequence = rowid
WHERE enqueue_sequence IS NULL;

CREATE UNIQUE INDEX turns_enqueue_sequence_unique
    ON turns(enqueue_sequence);

CREATE TABLE turn_enqueue_sequence_counter (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
    next_sequence INTEGER NOT NULL
);

INSERT INTO turn_enqueue_sequence_counter(singleton, next_sequence)
SELECT 1, COALESCE(MAX(enqueue_sequence), 0) + 1
FROM turns;

CREATE TRIGGER turns_assign_enqueue_sequence
AFTER INSERT ON turns
WHEN NEW.enqueue_sequence IS NULL
BEGIN
    UPDATE turns
    SET enqueue_sequence = (
        SELECT next_sequence
        FROM turn_enqueue_sequence_counter
        WHERE singleton = 1
    )
    WHERE id = NEW.id;

    UPDATE turn_enqueue_sequence_counter
    SET next_sequence = next_sequence + 1
    WHERE singleton = 1;
END;
