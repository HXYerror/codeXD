-- codexd:foreign_keys_off

ALTER TABLE ingress_messages ADD COLUMN discord_guild_id TEXT;
ALTER TABLE ingress_messages ADD COLUMN discord_channel_id TEXT;
ALTER TABLE conversations ADD COLUMN discord_guild_id TEXT;
ALTER TABLE conversations ADD COLUMN discord_parent_channel_id TEXT;

UPDATE ingress_messages
SET discord_guild_id = (
        SELECT p.discord_guild_id FROM projects p WHERE p.id = ingress_messages.project_id
    ),
    discord_channel_id = (
        SELECT p.discord_channel_id FROM projects p WHERE p.id = ingress_messages.project_id
    );

UPDATE conversations
SET discord_guild_id = (
        SELECT p.discord_guild_id FROM projects p WHERE p.id = conversations.project_id
    ),
    discord_parent_channel_id = (
        SELECT p.discord_channel_id FROM projects p WHERE p.id = conversations.project_id
    ),
    sandbox_profile = 'full_access';

UPDATE thread_revisions
SET thread_config_json = json_set(
    thread_config_json,
    '$.sandbox', 'full_access',
    '$.approval_mode', 'auto_review'
);

CREATE TABLE project_bindings_v9 AS
SELECT discord_guild_id, discord_channel_id, id AS project_id, created_at, updated_at
FROM projects
WHERE enabled = 1;

CREATE TABLE projects_v9 (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    root_path TEXT NOT NULL UNIQUE,
    root_path_casefold TEXT NOT NULL,
    default_model TEXT,
    default_reasoning_effort TEXT,
    default_reasoning_summary TEXT,
    default_personality TEXT,
    default_service_tier TEXT,
    default_web_search_mode TEXT NOT NULL DEFAULT 'cached'
        CHECK (default_web_search_mode IN (
            'cached', 'indexed', 'live', 'disabled', 'provider_default_uncontrolled'
        )),
    sandbox_profile TEXT NOT NULL DEFAULT 'full_access'
        CHECK (sandbox_profile = 'full_access'),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

INSERT INTO projects_v9(
    id, name, root_path, root_path_casefold, default_model,
    default_reasoning_effort, default_reasoning_summary, default_personality,
    default_service_tier, default_web_search_mode, sandbox_profile,
    created_at, updated_at
)
SELECT
    id, name, root_path, root_path_casefold, default_model,
    default_reasoning_effort, default_reasoning_summary, default_personality,
    default_service_tier, default_web_search_mode, 'full_access',
    created_at, updated_at
FROM projects;

DROP INDEX projects_root_casefold_unique;
DROP TABLE projects;
ALTER TABLE projects_v9 RENAME TO projects;

CREATE UNIQUE INDEX projects_root_casefold_unique
    ON projects(root_path_casefold);

CREATE TABLE channel_bindings (
    discord_guild_id TEXT NOT NULL,
    discord_channel_id TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id),
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(discord_guild_id, discord_channel_id),
    UNIQUE(discord_channel_id)
);

CREATE INDEX channel_bindings_project_idx
    ON channel_bindings(project_id);

INSERT INTO channel_bindings(
    discord_guild_id, discord_channel_id, project_id, created_at, updated_at
)
SELECT
    discord_guild_id, discord_channel_id, project_id, created_at, updated_at
FROM project_bindings_v9;

DROP TABLE project_bindings_v9;

CREATE TRIGGER projects_identity_immutable
BEFORE UPDATE OF root_path, root_path_casefold ON projects
BEGIN
    SELECT RAISE(ABORT, 'project root identity is immutable');
END;

CREATE TRIGGER conversations_identity_immutable
BEFORE UPDATE OF project_id, discord_thread_id, discord_guild_id, discord_parent_channel_id
ON conversations
BEGIN
    SELECT RAISE(ABORT, 'conversation routing identity is immutable');
END;

CREATE TRIGGER conversations_origin_required_insert
BEFORE INSERT ON conversations
WHEN NEW.discord_guild_id IS NULL OR NEW.discord_parent_channel_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'conversation Discord origin is required');
END;

CREATE TRIGGER conversations_full_access_insert
BEFORE INSERT ON conversations
WHEN NEW.sandbox_profile <> 'full_access'
BEGIN
    SELECT RAISE(ABORT, 'conversation sandbox is fixed to full_access');
END;

CREATE TRIGGER conversations_full_access_update
BEFORE UPDATE OF sandbox_profile ON conversations
WHEN NEW.sandbox_profile <> 'full_access'
BEGIN
    SELECT RAISE(ABORT, 'conversation sandbox is fixed to full_access');
END;

CREATE TRIGGER ingress_identity_immutable
BEFORE UPDATE OF project_id, discord_guild_id, discord_channel_id ON ingress_messages
BEGIN
    SELECT RAISE(ABORT, 'ingress routing identity is immutable');
END;

CREATE TRIGGER ingress_origin_required_insert
BEFORE INSERT ON ingress_messages
WHEN NEW.discord_guild_id IS NULL OR NEW.discord_channel_id IS NULL
BEGIN
    SELECT RAISE(ABORT, 'ingress Discord origin is required');
END;
