from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass

from codexd.storage.records import TurnRecord
from codexd.storage.repository import Repository

logger = logging.getLogger(__name__)

TurnHandler = Callable[[TurnRecord], Awaitable[float | None]]
MailboxErrorHandler = Callable[[str, Exception], Awaitable[None]]


@dataclass
class ConversationMailbox:
    conversation_id: str
    repository: Repository
    handler: TurnHandler
    error_handler: MailboxErrorHandler

    def __post_init__(self) -> None:
        self._wake = asyncio.Event()
        self._closed = False
        self._task = asyncio.create_task(
            self._run(), name=f"codexd-mailbox-{self.conversation_id}"
        )
        self._task.add_done_callback(self._observe_completion)

    @property
    def done(self) -> bool:
        return self._task.done()

    def wake(self) -> None:
        self._wake.set()

    async def close(self) -> None:
        self._closed = True
        self._wake.set()
        self._task.cancel()
        with suppress(asyncio.CancelledError):
            await self._task

    async def _run(self) -> None:
        while not self._closed:
            await self._wake.wait()
            self._wake.clear()
            while not self._closed:
                turn = await asyncio.to_thread(
                    self.repository.next_queued_turn, self.conversation_id
                )
                if turn is None:
                    break
                try:
                    retry_after = await self.handler(turn)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    try:
                        await self.error_handler(self.conversation_id, exc)
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        logger.exception(
                            "Mailbox error reporting failed",
                            extra={"conversation_id": self.conversation_id},
                        )
                    retry_after = 1.0
                if retry_after is not None:
                    await self._wake_after(retry_after)
                    break

    async def _wake_after(self, delay: float) -> None:
        await asyncio.sleep(delay)
        if not self._closed:
            self._wake.set()

    def _observe_completion(self, task: asyncio.Task[None]) -> None:
        if task.cancelled():
            return
        error = task.exception()
        if error is not None:
            logger.error(
                "Conversation mailbox stopped unexpectedly",
                exc_info=(type(error), error, error.__traceback__),
                extra={"conversation_id": self.conversation_id},
            )


class MailboxRegistry:
    def __init__(
        self,
        *,
        repository: Repository,
        handler: TurnHandler,
        error_handler: MailboxErrorHandler,
    ) -> None:
        self._repository = repository
        self._handler = handler
        self._error_handler = error_handler
        self._mailboxes: dict[str, ConversationMailbox] = {}
        self._lock = asyncio.Lock()
        self._closed = False

    async def wake(self, conversation_id: str) -> None:
        mailbox = await self._get(conversation_id)
        if mailbox is not None:
            mailbox.wake()

    async def restore(self) -> None:
        conversation_ids = await asyncio.to_thread(
            self._repository.queued_conversation_ids
        )
        for conversation_id in conversation_ids:
            await self.wake(conversation_id)

    async def close(self) -> None:
        async with self._lock:
            if self._closed:
                return
            self._closed = True
            mailboxes = tuple(self._mailboxes.values())
            self._mailboxes.clear()
        await asyncio.gather(*(mailbox.close() for mailbox in mailboxes))

    async def _get(self, conversation_id: str) -> ConversationMailbox | None:
        async with self._lock:
            if self._closed:
                return None
            mailbox = self._mailboxes.get(conversation_id)
            if mailbox is None or mailbox.done:
                mailbox = ConversationMailbox(
                    conversation_id=conversation_id,
                    repository=self._repository,
                    handler=self._handler,
                    error_handler=self._error_handler,
                )
                self._mailboxes[conversation_id] = mailbox
            return mailbox
