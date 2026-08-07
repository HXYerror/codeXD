CREATE TABLE discord_render_plans (
    turn_id TEXT PRIMARY KEY REFERENCES turns(id),
    source_sha256 TEXT NOT NULL,
    plan_json TEXT NOT NULL CHECK (json_valid(plan_json)),
    retention_until INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX discord_render_plans_retention_idx
    ON discord_render_plans(retention_until);
