from __future__ import annotations

import json

from codexd.domain.ids import canonical_json
from codexd.domain.observations import FIELDS, ProviderSettings, scalar
from codexd.storage.repository import Repository


def save(repository: Repository, project_id: str, observation: ProviderSettings) -> None:
    if observation.source not in {
        "thread/start",
        "thread/resume",
        "thread/fork",
        "thread/settings/updated",
    }:
        return
    with repository.store.transaction() as connection:
        valid = connection.execute(
            "SELECT 1 FROM runtime_leases WHERE generation=? AND state='ready' "
            "AND (project_id=? OR scope_kind='shared')",
            (observation.generation, project_id),
        ).fetchone()
        if valid is None:
            return
        revisions = connection.execute(
            "SELECT r.id FROM thread_revisions r JOIN conversations c ON c.active_revision_id=r.id "
            "WHERE c.project_id=? AND r.provider_thread_id=? AND r.state='active'",
            (project_id, observation.thread_id),
        ).fetchall()
        for revision in revisions:
            if observation.turn_id:
                turn = connection.execute(
                    "SELECT t.id FROM turns t JOIN runtime_leases l ON l.id=t.runtime_lease_id "
                    "WHERE t.id=? AND t.thread_revision_id=? AND t.runtime_generation=? "
                    "AND l.generation=? AND l.state='ready'",
                    (
                        observation.turn_id,
                        revision["id"],
                        observation.generation,
                        observation.generation,
                    ),
                ).fetchone()
                if turn is None:
                    continue
            values = {
                key: scalar(value) for key, value in observation.values.items() if key in FIELDS
            }
            for turn_id in ("", observation.turn_id) if observation.turn_id else ("",):
                connection.execute(
                    "INSERT INTO provider_settings_observations VALUES (?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(revision_id,turn_id) DO UPDATE SET "
                    "runtime_generation=excluded.runtime_generation,provider_thread_id=excluded.provider_thread_id,"
                    "observed_at=excluded.observed_at,sequence=excluded.sequence,"
                    "source=excluded.source,values_json=excluded.values_json "
                    "WHERE excluded.runtime_generation>runtime_generation OR "
                    "(excluded.runtime_generation=runtime_generation "
                    "AND excluded.sequence>=sequence)",
                    (
                        revision["id"],
                        turn_id,
                        observation.generation,
                        observation.thread_id,
                        observation.observed_at,
                        observation.sequence,
                        observation.source,
                        canonical_json(values),
                    ),
                )


def load(repository: Repository, revision_id: str, turn_id: str = "") -> ProviderSettings | None:
    row = repository.store.query_one(
        "SELECT * FROM provider_settings_observations WHERE revision_id=? AND turn_id=?",
        (revision_id, turn_id),
    )
    if row is None:
        return None
    return ProviderSettings(row["provider_thread_id"], row["runtime_generation"],
                            json.loads(row["values_json"]), row["source"], row["observed_at"],
                            row["sequence"], row["turn_id"] or None, fresh=False)
