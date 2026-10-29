# Issues 44–46: implementation boundaries

## Schedule controls (#45, #46)

Slash pause/resume/delete accept a task UUID or unique prefix, with descriptions
and owner-scoped autocomplete. List/show link to the management commands. Pause
is idempotent and stops only future triggers; queued/running Turns are counted,
not cancelled. Delete is a separate 10-minute, owner/Conversation-scoped durable
confirmation. It removes prompt/skill inputs, not audit/fire/Turn history.

Schema 27 adds a configuration revision maintained by a trigger. Timer cursor
advances retain that revision. Confirmation checks the configuration revision;
the actual mutation still checks the current row version inside the same write
transaction. Configuration/state edits invalidate old confirmations. A timer
finishing a once schedule changes its state and therefore requires a new review.

Dynamic schedule_list/show/pause reuse the creation tool's owner, Discord source,
revision, runtime generation, guild and provider Turn checks. Results are durable
and invocation-idempotent, contain metadata but not the scheduled prompt, and
never create cross-Conversation or background management authority. No dynamic
delete/resume or background self-pause is added. Existing provider threads may
not acquire newly registered dynamic tools until an explicit new session; slash
commands remain the fallback. No automatic thread recreation is performed.

## Observed settings (#44)

Start/resume/fork responses and the sole consumer of global settings notifications
maintain per-thread snapshots. The global reader does not consume Turn queues.
A per-connection sequence prevents a response from replacing a newer settings
notification. Fields are allowlisted scalars, without cwd, prompts, encrypted
output, complete notifications or credentials. Query-time thread/read refreshes
runtime state; it is not misrepresented as a source of model/effort settings.

Schema 28 persists snapshots by revision and optionally Turn, with generation,
source, sequence and observation time. Old generations and foreign threads are
rejected. Unloaded/disconnected runtimes show last observations as stale; absent
fields remain unknown. Commands do not resume/start/compact/replay to obtain
settings. Requested overrides and catalog capabilities are separate from observed
values. Clearing overrides does not promise that the existing thread resets.

Because thread/settings/updated has no Turn ID, a setting observed without a
current correlated Turn is revision-level only. Historical Turns without captured
observations remain unknown; this change does not scrape old rollouts or backfill
guessed values. Wire-order notifications are used as last observations, not proof
of a proxy's backend model mapping.

Observed limitation: the real 0.147 localhost fixture returned start/resume
settings but did not emit thread/settings/updated for its unchanged-settings Turn.
Notification routing/race handling is tested with injected typed notifications;
continuous full-field production observation is not certified by that fixture.
Response-absent summary/personality fields and uncorrelated historical Turns remain
unknown. #44 requires live notification/Discord acceptance before closure.
A new Turn invalidates the previous response's freshness until a supported
settings notification or subsequent resume response confirms settings again.

## Validation and release

Local unit/integration tests cover management schema, timer-only changes versus
real edits, confirmation scope, repeat pause/tool calls, no prompt disclosure,
settings races, generation isolation and offline fallback. The real 0.147 runtime
fixture uses localhost SSE without billable provider calls or Discord writes.

No production database migration, command registration, bot restart, merge or issue
closure is performed by this implementation run. Cross-platform CI and live
Discord acceptance remain release steps. #41's live-provider signoff is not
automatically satisfied by local protocol tests.

Local verification on 2026-10-29: full suite 847 passed / 8 skipped; Ruff, strict
Mypy, and wheel/sdist builds pass. Seven skips require native Windows and one is
the opt-in real-provider attachment test. The additional real-runtime fixtures
verify response observation in both dynamic and non-dynamic client paths.
