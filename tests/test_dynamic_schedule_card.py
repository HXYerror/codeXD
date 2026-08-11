from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import discord
import pytest

from codexd.domain.ids import utc_now_ms
from codexd.security.signing import ComponentSigner
from codexd.storage.repository import Repository
from codexd.transport.discord.outbox import DiscordOutboxTransport


def _payload(*, state: str = "pending") -> dict[str, object]:
    payload: dict[str, object] = {
        "kind": "schedule_draft_card",
        "state": state,
        "draft_id": "11111111-1111-1111-1111-111111111111",
        "action": "create",
        "name": "daily status",
        "schedule_kind": "cron",
        "expression": "0 9 * * *",
        "timezone": "Asia/Shanghai",
        "misfire_policy": "latest",
        "prompt_text": "Check CI",
        "occurrences": [
            {
                "utc_ms": 1786496400000,
                "local_display": "2026-08-12T09:00:00+08:00",
            }
        ],
    }
    if state == "pending":
        payload.update(
            nonce="card-nonce",
            expires_at=utc_now_ms() + 600_000,
        )
    return payload


@pytest.mark.asyncio
async def test_schedule_card_delivery_has_persistent_signed_controls() -> None:
    client = Mock(spec=discord.Client)
    repository = Mock(spec=Repository)
    signer = ComponentSigner(b"k" * 32)
    transport = DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=Mock(),
        signer=signer,
    )
    channel = Mock(spec=discord.Thread)
    channel.id = 300
    channel.send = AsyncMock(return_value=SimpleNamespace(id=7777))

    result = await transport._deliver_schedule_draft_card(
        channel,
        _payload(),
        "card-outbox",
        "send",
        "schedule-draft-11111111",
        "pending",
    )

    assert result.discord_message_id == "7777"
    assert result.schedule_draft_id == "11111111-1111-1111-1111-111111111111"
    repository.bind_schedule_draft_message.assert_called_once_with(
        draft_id="11111111-1111-1111-1111-111111111111",
        outbox_id="card-outbox",
        discord_message_id="7777",
    )
    kwargs = channel.send.await_args.kwargs
    assert kwargs["embed"].title == "Schedule · create"
    assert "FULL ACCESS / unattended" in kwargs["embed"].description
    view = kwargs["view"]
    assert view.timeout is None
    assert [item.label for item in view.children] == ["Confirm", "Cancel"]
    actions = [
        signer.verify_schedule_draft_id(item.custom_id) for item in view.children
    ]
    assert [(action.action, action.nonce) for action in actions] == [
        ("confirm", "card-nonce"),
        ("cancel", "card-nonce"),
    ]


@pytest.mark.asyncio
async def test_schedule_card_terminal_delivery_edits_original_and_removes_controls() -> (
    None
):
    client = Mock(spec=discord.Client)
    client.user = SimpleNamespace(id=500)
    repository = Mock(spec=Repository)
    repository.schedule_draft_message.return_value = "7777"
    transport = DiscordOutboxTransport(
        client=client,
        repository=repository,
        renderer=Mock(),
        signer=ComponentSigner(b"k" * 32),
    )
    channel = Mock(spec=discord.Thread)
    channel.id = 300
    message = SimpleNamespace(
        id=7777,
        author=SimpleNamespace(id=500),
        edit=AsyncMock(),
    )
    channel.fetch_message = AsyncMock(return_value=message)
    payload = _payload(state="confirmed")
    payload.update(schedule_ref="abcdef12", next_due_at=1786496400000)

    result = await transport._deliver_schedule_draft_card(
        channel,
        payload,
        "terminal-outbox",
        "edit",
        "schedule-draft-11111111",
        "pending",
    )

    assert result.discord_message_id == "7777"
    repository.schedule_draft_message.assert_called_once_with(
        "11111111-1111-1111-1111-111111111111"
    )
    assert message.edit.await_args.kwargs["view"] is None
    assert message.edit.await_args.kwargs["embed"].description.startswith("✅")
