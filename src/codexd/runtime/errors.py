from __future__ import annotations

from dataclasses import dataclass

from codexd.errors import CodexDError


@dataclass(frozen=True)
class AdapterFailure:
    code: str
    provider_exception: str
    message: str
    retryable: bool
    runtime_generation: int
    thread_id: str | None = None
    turn_id: str | None = None
    cause_chain_hash: str | None = None


class AdapterError(CodexDError):
    code = "adapter_error"

    def __init__(self, failure: AdapterFailure) -> None:
        self.failure = failure
        super().__init__(failure.message)


class RuntimeUnavailable(AdapterError):
    code = "runtime_unavailable"


class AuthenticationRequired(AdapterError):
    code = "authentication_required"


class UnsupportedCapability(AdapterError):
    code = "unsupported_capability"


class FileInputUnsupported(UnsupportedCapability):
    code = "file_input_unsupported"


class InvalidThread(AdapterError):
    code = "invalid_thread"


class ThreadIdentityMismatch(AdapterError):
    code = "thread_identity_mismatch"


class ProviderRateLimited(AdapterError):
    code = "provider_rate_limited"


class ProviderRejected(AdapterError):
    code = "provider_rejected"


class ProviderOutcomeUnknown(AdapterError):
    code = "provider_effect_outcome_unknown"


class StreamEndedUnexpectedly(AdapterError):
    code = "stream_ended_unexpectedly"


class EventJournalError(CodexDError):
    code = "event_journal_error"


class InterruptFailed(AdapterError):
    code = "interrupt_failed"


class AdapterInvariantError(AdapterError):
    code = "adapter_invariant_error"


def file_input_unsupported(
    *,
    generation: int,
    thread_id: str | None = None,
    turn_id: str | None = None,
) -> FileInputUnsupported:
    return FileInputUnsupported(
        AdapterFailure(
            code=FileInputUnsupported.code,
            provider_exception="MentionInputUnavailable",
            message="Codex runtime does not support ordinary file input",
            retryable=False,
            runtime_generation=generation,
            thread_id=thread_id,
            turn_id=turn_id,
        )
    )
