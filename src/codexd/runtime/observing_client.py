"""Exact-pair adapter retaining typed mutation responses the high-level SDK drops."""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

from openai_codex import AsyncCodex as SDKAsyncCodex
from openai_codex import CodexConfig
from openai_codex.async_client import AsyncCodexClient
from openai_codex.generated.v2_all import (
    ThreadForkResponse,
    ThreadResumeResponse,
    ThreadStartResponse,
)


class ObservingClient(AsyncCodexClient):
    response_sink: Callable[[Any, str, int], None] | None = None
    sequence: Callable[[], int] = staticmethod(lambda: 0)

    async def thread_start(self, params: Any = None) -> ThreadStartResponse:
        before = self.sequence()
        response = await super().thread_start(params)
        if self.response_sink:
            self.response_sink(response, "thread/start", before)
        return response

    async def thread_resume(self, thread_id: str, params: Any = None) -> ThreadResumeResponse:
        before = self.sequence()
        response = await super().thread_resume(thread_id, params)
        if self.response_sink:
            self.response_sink(response, "thread/resume", before)
        return response

    async def thread_fork(self, thread_id: str, params: Any = None) -> ThreadForkResponse:
        before = self.sequence()
        response = await super().thread_fork(thread_id, params)
        if self.response_sink:
            self.response_sink(response, "thread/fork", before)
        return response


class ObservingAsyncCodex(SDKAsyncCodex):
    def __init__(self, config: CodexConfig | None = None) -> None:
        super().__init__(config)
        # Neither client has launched a process yet. No router/queue is replaced.
        self._client = ObservingClient(config=config)
