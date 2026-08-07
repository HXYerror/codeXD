from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest

from codexd.domain.conversations import SandboxProfile
from codexd.storage.records import ConversationRecord, ProjectRecord
from codexd.storage.repository import Repository
from codexd.storage.sqlite import SQLiteStore


@dataclass(frozen=True)
class StorageContext:
    store: SQLiteStore
    repository: Repository
    project: ProjectRecord
    conversation: ConversationRecord
    root: Path


@pytest.fixture
def storage_context(tmp_path: Path) -> StorageContext:
    data = tmp_path / "data"
    root = tmp_path / "project"
    root.mkdir()
    with SQLiteStore(data / "codexd.sqlite3") as store:
        store.migrate()
        repository = Repository(store)
        project = repository.bind_project(
            name="test",
            root_path=root,
            guild_id=100,
            channel_id=200,
            sandbox_profile=SandboxProfile.FULL_ACCESS,
        )
        conversation = repository.create_conversation(
            project_id=project.id,
            discord_thread_id=300,
            discord_guild_id=100,
            discord_parent_channel_id=200,
            owner_user_id=400,
        )
        yield StorageContext(store, repository, project, conversation, root)
