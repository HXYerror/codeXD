ALTER TABLE turns ADD COLUMN provider_error_code TEXT;
ALTER TABLE turns ADD COLUMN provider_error_underlying_code TEXT;
ALTER TABLE turns ADD COLUMN provider_retry_count INTEGER NOT NULL DEFAULT 0
    CHECK (provider_retry_count >= 0);
ALTER TABLE turns ADD COLUMN provider_retry_limit INTEGER
    CHECK (provider_retry_limit IS NULL OR provider_retry_limit > 0);
ALTER TABLE turns ADD COLUMN provider_http_status INTEGER
    CHECK (provider_http_status IS NULL OR provider_http_status >= 100);

ALTER TABLE thread_revisions ADD COLUMN degraded_failure_code TEXT;
ALTER TABLE thread_revisions ADD COLUMN degraded_fingerprint TEXT
    CHECK (
        degraded_fingerprint IS NULL
        OR (
            length(degraded_fingerprint) = 64
            AND degraded_fingerprint NOT GLOB '*[^0-9a-f]*'
        )
    );
ALTER TABLE thread_revisions ADD COLUMN consecutive_failure_count INTEGER NOT NULL DEFAULT 0
    CHECK (consecutive_failure_count >= 0);
ALTER TABLE thread_revisions ADD COLUMN first_failed_at INTEGER;
ALTER TABLE thread_revisions ADD COLUMN last_failed_at INTEGER;

ALTER TABLE conversations ADD COLUMN recovery_reason TEXT
    CHECK (
        recovery_reason IS NULL OR recovery_reason IN (
            'provider_thread_identity_mismatch',
            'provider_effect_outcome_unknown',
            'provider_mutation_commit_failed',
            'provider_rollout_missing_or_corrupt',
            'provider_protocol_terminal_unparseable'
        )
    );
ALTER TABLE conversations ADD COLUMN provider_recovery_state TEXT
    CHECK (
        provider_recovery_state IS NULL
        OR provider_recovery_state = 'thread_reconciling'
    );
ALTER TABLE conversations ADD COLUMN provider_recovery_since INTEGER;
