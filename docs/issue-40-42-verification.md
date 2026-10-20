# Issues #40–#42: implementation and release checks

## Behavior

- #42: schema 26 stores terminal typed code independently from the most recent
  explicit retry code. The terminal classification remains authoritative;
  `other` finals explain earlier retries without claiming a network root cause.
  Context is scoped to one Turn, survives restart, and does not cause replay.
  Bounded nested error messages are unwrapped and redacted for display only;
  free text never changes the typed classification.
- #40: model identity resolves conversation override, project default, then
  project-scoped Codex effective configuration. Catalog is capability metadata,
  never evidence of the configured model. Unreadable configuration and absent
  capabilities are explicit unknowns. An active Turn's requested model is not
  presented as a runtime observation. No query changes model preferences/threads.
- #41: release pins SDK/runtime 0.147.0/0.147.0, rejects mismatched or prerelease
  runtimes, retains existing recovery policies. Cache-write usage is retained
  when reported; model specialty is intentionally not exposed in the UI.
  Environment notifications are route-checked, then reduced to hash/size.
  Audio output stays rejected. No Goal/section/audio features were added.

## Compatibility discovered by real runtime testing

Low-level initialize may return only `userAgent`; normalize that metadata before
checking the runtime version, without replacing an explicitly reported version.
0.147 ephemeral forks omit persisted ancestry fields. Only that exact pair may
use the locally retained, correlated fork request/response as ancestry evidence;
the fork must be a distinct thread, the subsequent read must match its returned
identity, and ephemeral status is still required. Persisted metadata is not forged.

## Verification boundaries

`tests/test_runtime_0147_contract.py` launches the real bundled app-server with
an isolated Codex home and a deterministic localhost SSE provider. It checks
schedule/publish-image callbacks and Turn routing, real command reads of random
plain-file/ZIP markers after daemon materialization, local image input,
resume/fork/steer/interrupt/unsubscribe/compact/archive/unarchive. Its callbacks
are fixtures: no real schedule, artifact publication, Discord write, credential,
or billable provider request. App-layer schedule/image safety tests run separately.

This verifies protocol/runtime behavior, not production model behavior. The
opt-in attachment test still requires explicit live-provider authorization.
Cross-platform CI is configured but must pass on the remote runners before release.
The product's existing direct-unarchive capability gate remains closed.

## Release checklist

1. Run `python -m pip install -e '.[dev]'` in a new environment; verify both
   package versions, `python -m pip check`, ruff, mypy, full pytest and build.
2. Require all four runtime-contract CI jobs. Run opt-in production-provider
   text/image/attachment smoke in a designated test session, never as startup health.
3. Before production deployment, stop the daemon through the normal service
   workflow and snapshot SQLite plus configuration; retain the old code/wheel
   and 0.144.4 SDK/runtime environment. Do not overwrite the running environment.
4. Deploy as an explicit maintenance action. Observe startup, typed failures,
   unknown notifications, Discord outbox, and SQLite writes. Confirm ordinary
   failures remain recoverable with the next message or explicit `/session new`.
5. Roll back code and both packages together. Schema 26 only adds nullable fields;
   older code ignores them. Do not restore an old database over newly accepted
   work; coordinate any snapshot restore separately.

No production rollout, live-provider signoff, remote CI run, or issue closure is
implied by local test success.

## Local evidence (2026-10-13, macOS arm64)

- Fresh isolated install resolves SDK/runtime 0.147.0/0.147.0; `pip check` passes.
- Final full suite: 835 passed, 8 skipped in the clean environment.
  Skips are seven native-Windows cases and one opt-in real-provider attachment test.
- Ruff and strict mypy pass; wheel and sdist build successfully.
- Installing the built wheel preserves schema 26 and the recommended pair;
  its targeted model/schema/real-runtime contract suite passes (17 tests).
- Real 0.147 runtime/local-provider contract passes for plain-file and ZIP variants,
  including the application runtime's Side Query path.
- Runtime wheels downloaded successfully for all four target platforms. Download
  availability is not evidence that non-macOS binaries have executed successfully.

## Pre-merge review (2026-10-20)

- Reject missing initialized runtime versions instead of inferring the running
  binary's version from package metadata.
- Malformed typed config responses degrade to an unknown model without exposing
  validation payloads. Ambiguous catalogs cannot bypass plain-text validation.
- Added regression tests for these review findings. Production already runs
  the paired 0.147.0 packages; deploy the merged code and restart via launchd.
