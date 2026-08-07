ALTER TABLE audit_log ADD COLUMN correlation_id TEXT;

CREATE UNIQUE INDEX audit_log_action_correlation_unique
    ON audit_log(action, correlation_id)
    WHERE correlation_id IS NOT NULL;
