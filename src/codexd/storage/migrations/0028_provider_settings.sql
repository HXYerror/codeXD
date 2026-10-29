CREATE TABLE provider_settings_observations (
 revision_id TEXT NOT NULL REFERENCES thread_revisions(id) ON DELETE CASCADE,
 turn_id TEXT NOT NULL DEFAULT '',
 runtime_generation INTEGER NOT NULL,
 provider_thread_id TEXT NOT NULL,
 observed_at INTEGER NOT NULL,
 sequence INTEGER NOT NULL,
 source TEXT NOT NULL,
 values_json TEXT NOT NULL CHECK(json_valid(values_json)),
 PRIMARY KEY(revision_id, turn_id)
);
