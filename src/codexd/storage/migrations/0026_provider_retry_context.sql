-- Terminal classification and earlier retry evidence are independent facts.
ALTER TABLE turns ADD COLUMN provider_terminal_typed_code TEXT;
ALTER TABLE turns ADD COLUMN provider_prior_retry_typed_code TEXT;
ALTER TABLE turns ADD COLUMN provider_prior_retry_code TEXT;
