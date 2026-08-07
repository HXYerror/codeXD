-- codexd:foreign_keys_off
DROP TRIGGER schedule_fires_turn_fk_insert;
DROP TRIGGER schedule_fires_turn_fk_update;

CREATE TABLE schedule_fires_v7 (
    id TEXT PRIMARY KEY,
    schedule_id TEXT NOT NULL REFERENCES schedules(id),
    occurrence_key TEXT NOT NULL,
    trigger_kind TEXT NOT NULL CHECK (trigger_kind IN ('timer', 'manual', 'misfire')),
    scheduled_for INTEGER,
    scheduled_local TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('due', 'materialized', 'skipped', 'blocked')),
    turn_id TEXT UNIQUE REFERENCES turns(id) ON DELETE CASCADE,
    error_code TEXT,
    created_at INTEGER NOT NULL,
    materialized_at INTEGER,
    UNIQUE(schedule_id, occurrence_key)
);

INSERT INTO schedule_fires_v7(
    id, schedule_id, occurrence_key, trigger_kind, scheduled_for,
    scheduled_local, state, turn_id, error_code, created_at, materialized_at
)
SELECT
    id, schedule_id, occurrence_key, trigger_kind, scheduled_for,
    scheduled_local, state, turn_id, error_code, created_at, materialized_at
FROM schedule_fires;

DROP TABLE schedule_fires;
ALTER TABLE schedule_fires_v7 RENAME TO schedule_fires;

CREATE TEMP TABLE schedule_fire_pair_validation (
    invalid_pairs INTEGER NOT NULL CHECK (invalid_pairs = 0)
);

INSERT INTO schedule_fire_pair_validation(invalid_pairs)
SELECT COUNT(*)
FROM (
    SELECT sf.id
    FROM schedule_fires sf
    WHERE sf.turn_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM turns t
          JOIN schedules s ON s.id = sf.schedule_id
          WHERE t.id = sf.turn_id
            AND t.source_kind = 'schedule'
            AND t.schedule_fire_id = sf.id
            AND t.conversation_id = s.conversation_id
      )
    UNION ALL
    SELECT t.id
    FROM turns t
    WHERE t.schedule_fire_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM schedule_fires sf
          JOIN schedules s ON s.id = sf.schedule_id
          WHERE sf.id = t.schedule_fire_id
            AND sf.turn_id = t.id
            AND t.source_kind = 'schedule'
            AND t.conversation_id = s.conversation_id
      )
);

DROP TABLE schedule_fire_pair_validation;

CREATE TRIGGER schedule_fires_turn_pair_insert
BEFORE INSERT ON schedule_fires
WHEN NEW.turn_id IS NOT NULL
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM turns t
        JOIN schedules s ON s.id = NEW.schedule_id
        WHERE t.id = NEW.turn_id
          AND t.source_kind = 'schedule'
          AND t.schedule_fire_id = NEW.id
          AND t.conversation_id = s.conversation_id
    ) THEN RAISE(ABORT, 'schedule fire and turn are not reciprocal') END;
END;

CREATE TRIGGER schedule_fires_turn_pair_update
BEFORE UPDATE OF turn_id, schedule_id ON schedule_fires
WHEN NEW.turn_id IS NOT NULL
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM turns t
        JOIN schedules s ON s.id = NEW.schedule_id
        WHERE t.id = NEW.turn_id
          AND t.source_kind = 'schedule'
          AND t.schedule_fire_id = NEW.id
          AND t.conversation_id = s.conversation_id
    ) THEN RAISE(ABORT, 'schedule fire and turn are not reciprocal') END;
END;

CREATE TRIGGER schedule_fires_turn_pair_immutable
BEFORE UPDATE OF turn_id ON schedule_fires
WHEN OLD.turn_id IS NOT NULL AND NEW.turn_id IS NOT OLD.turn_id
BEGIN
    SELECT RAISE(ABORT, 'schedule fire turn pairing is immutable');
END;

CREATE TRIGGER turns_schedule_fire_pair_insert
BEFORE INSERT ON turns
WHEN NEW.schedule_fire_id IS NOT NULL
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM schedule_fires sf
        JOIN schedules s ON s.id = sf.schedule_id
        WHERE sf.id = NEW.schedule_fire_id
          AND NEW.source_kind = 'schedule'
          AND NEW.conversation_id = s.conversation_id
          AND (sf.turn_id IS NULL OR sf.turn_id = NEW.id)
    ) THEN RAISE(ABORT, 'schedule turn and fire are not reciprocal') END;
END;

CREATE TRIGGER turns_schedule_fire_pair_link_insert
AFTER INSERT ON turns
WHEN NEW.schedule_fire_id IS NOT NULL
BEGIN
    UPDATE schedule_fires
    SET turn_id = NEW.id
    WHERE id = NEW.schedule_fire_id AND turn_id IS NULL;
END;

CREATE TRIGGER turns_schedule_fire_pair_update
BEFORE UPDATE OF schedule_fire_id, conversation_id, source_kind ON turns
WHEN NEW.schedule_fire_id IS NOT NULL
BEGIN
    SELECT CASE WHEN NOT EXISTS (
        SELECT 1
        FROM schedule_fires sf
        JOIN schedules s ON s.id = sf.schedule_id
        WHERE sf.id = NEW.schedule_fire_id
          AND NEW.source_kind = 'schedule'
          AND NEW.conversation_id = s.conversation_id
          AND (sf.turn_id IS NULL OR sf.turn_id = NEW.id)
    ) THEN RAISE(ABORT, 'schedule turn and fire are not reciprocal') END;
END;

CREATE TRIGGER turns_schedule_fire_pair_immutable
BEFORE UPDATE OF schedule_fire_id ON turns
WHEN NEW.schedule_fire_id IS NOT OLD.schedule_fire_id
BEGIN
    SELECT RAISE(ABORT, 'schedule turn fire pairing is immutable');
END;
