from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path

from codexd.config import SecurityConfig, resolve_project_path
from codexd.domain.conversations import SandboxProfile
from codexd.storage.records import ConversationRecord, ProjectRecord
from codexd.storage.repository import Repository


@dataclass(frozen=True)
class ResolvedProject:
    project: ProjectRecord
    source: str


class SessionCoordinator:
    def __init__(
        self,
        *,
        repository: Repository,
        security: SecurityConfig,
        home_path: Path | None = None,
    ) -> None:
        self._repository = repository
        self._security = security
        self._home_path = (home_path or Path.home()).resolve(strict=True)

    async def ensure_home_project(self) -> ProjectRecord:
        return await asyncio.to_thread(
            self._repository.ensure_project,
            name="$HOME",
            root_path=self._home_path,
        )

    async def bind_project(
        self,
        *,
        name: str,
        path: str,
        guild_id: int,
        channel_id: int,
        interaction_id: str | None = None,
    ) -> ProjectRecord:
        root = resolve_project_path(
            path,
            self._security.allowed_roots,
            relative_to=self._home_path,
        )
        return await asyncio.to_thread(
            self._repository.bind_project,
            name=name,
            root_path=root,
            guild_id=guild_id,
            channel_id=channel_id,
            sandbox_profile=SandboxProfile.FULL_ACCESS,
            command_interaction_id=interaction_id,
        )

    async def create_conversation(
        self,
        *,
        project: ProjectRecord,
        discord_thread_id: int,
        discord_guild_id: int,
        discord_parent_channel_id: int,
        owner_user_id: int,
    ) -> ConversationRecord:
        return await asyncio.to_thread(
            self._repository.create_conversation,
            project_id=project.id,
            discord_thread_id=discord_thread_id,
            discord_guild_id=discord_guild_id,
            discord_parent_channel_id=discord_parent_channel_id,
            owner_user_id=owner_user_id,
        )

    async def project_for_channel(
        self, *, guild_id: int, channel_id: int
    ) -> ProjectRecord | None:
        return await asyncio.to_thread(
            self._repository.project_for_channel, guild_id, channel_id
        )

    async def resolve_project_for_channel(
        self, *, guild_id: int, channel_id: int
    ) -> ResolvedProject:
        bound = await self.project_for_channel(guild_id=guild_id, channel_id=channel_id)
        if bound is not None:
            return ResolvedProject(bound, "binding")
        return ResolvedProject(await self.ensure_home_project(), "home")

    async def unbind_project(
        self,
        *,
        guild_id: int,
        channel_id: int,
        confirmation_name: str,
        interaction_id: str | None = None,
    ) -> ProjectRecord:
        return await asyncio.to_thread(
            self._repository.unbind_project,
            guild_id=guild_id,
            channel_id=channel_id,
            confirmation_name=confirmation_name,
            command_interaction_id=interaction_id,
        )

    async def conversation_for_thread(
        self, thread_id: int
    ) -> ConversationRecord | None:
        return await asyncio.to_thread(
            self._repository.conversation_for_thread, thread_id
        )
