ALTER TABLE schedules ADD COLUMN config_revision INTEGER NOT NULL DEFAULT 1;
CREATE TRIGGER schedules_config_revision
AFTER UPDATE OF name, kind, expression, timezone, misfire_policy, prompt_hash,
                skill_inputs_json, state ON schedules
WHEN OLD.name IS NOT NEW.name OR OLD.kind IS NOT NEW.kind
 OR OLD.expression IS NOT NEW.expression OR OLD.timezone IS NOT NEW.timezone
 OR OLD.misfire_policy IS NOT NEW.misfire_policy OR OLD.prompt_hash IS NOT NEW.prompt_hash
 OR OLD.skill_inputs_json IS NOT NEW.skill_inputs_json OR OLD.state IS NOT NEW.state
BEGIN
 UPDATE schedules SET config_revision = OLD.config_revision + 1 WHERE id = NEW.id;
END;

CREATE TABLE schedule_delete_confirmations (
 id TEXT PRIMARY KEY,
 schedule_id TEXT NOT NULL REFERENCES schedules(id),
 conversation_id TEXT NOT NULL REFERENCES conversations(id),
 owner_user_id TEXT NOT NULL,
 config_revision INTEGER NOT NULL,
 expires_at INTEGER NOT NULL,
 consumed_at INTEGER
);

CREATE TABLE schedule_management_calls (
 runtime_generation INTEGER NOT NULL,
 provider_thread_id TEXT NOT NULL,
 provider_turn_id TEXT NOT NULL,
 provider_call_id TEXT NOT NULL,
 turn_id TEXT NOT NULL REFERENCES turns(id) ON DELETE CASCADE,
 tool_name TEXT NOT NULL,
 arguments_hash TEXT NOT NULL,
 result_json TEXT NOT NULL CHECK(json_valid(result_json)),
 created_at INTEGER NOT NULL,
 PRIMARY KEY(runtime_generation, provider_thread_id, provider_turn_id, provider_call_id)
);

-- A provider call ID identifies one tool, including across the two result stores.
CREATE TRIGGER management_call_identity_guard
BEFORE INSERT ON schedule_management_calls
WHEN EXISTS (
 SELECT 1 FROM dynamic_tool_invocations
 WHERE runtime_generation=NEW.runtime_generation
 AND provider_thread_id=NEW.provider_thread_id
 AND provider_turn_id=NEW.provider_turn_id AND provider_call_id=NEW.provider_call_id
)
BEGIN
 SELECT RAISE(ABORT, 'dynamic tool call identity already used');
END;
CREATE TRIGGER dynamic_call_identity_guard
BEFORE INSERT ON dynamic_tool_invocations
WHEN EXISTS (
 SELECT 1 FROM schedule_management_calls
 WHERE runtime_generation=NEW.runtime_generation
 AND provider_thread_id=NEW.provider_thread_id
 AND provider_turn_id=NEW.provider_turn_id AND provider_call_id=NEW.provider_call_id
)
BEGIN
 SELECT RAISE(ABORT, 'dynamic tool call identity already used');
END;
