from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager


class ConversationLocks:
    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}
        self._registry_lock = asyncio.Lock()

    @asynccontextmanager
    async def hold(self, conversation_id: str) -> AsyncIterator[None]:
        async with self._registry_lock:
            lock = self._locks.setdefault(conversation_id, asyncio.Lock())
        async with lock:
            yield
