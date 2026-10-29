# #48: image admission after execution-time thread restoration

Image uploads now pass the same storage-integrity checks and enter the durable
queue before runtime/model resolution. The normal execution lifecycle restores a
cold thread. If a loaded thread's settings evidence is stale or absent, an
execution-only resume refreshes it; identity, runtime generation and idle state
must match. Queries never resume a thread and global config is not substituted
for an existing thread's model.

Capabilities are fetched after the resume and must be unambiguous and complete.
For images, the verified model is explicitly passed to this Turn so a later
setting change cannot silently send the image to a different model. This does not
change conversation/project overrides or select an arbitrary catalog default.
Queued overrides remain the request snapshot, not newer conversation preferences.

Unconfirmed model, absent model, ambiguous model, empty/incomplete catalog and
text-only capability have separate stable error codes and final guidance saying
the image request was not sent. There is no automatic replay, silent image drop,
new thread, sandbox expansion or history rewrite. Existing failed uploads must
be resent explicitly after deployment.

Regression coverage includes stale settings after a completed text Turn, cold
runtime restoration, global A versus old thread B, concurrently changed queued
preferences, missing/wrong-generation evidence, Discord image ingestion and
rejected-model paths. The real SDK/runtime 0.147.0 localhost SSE fixture verifies
resume evidence and subsequent image input with the verified model, without paid
provider calls. Production image semantics remain user acceptance after restart.
