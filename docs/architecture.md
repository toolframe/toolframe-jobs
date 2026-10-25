# Durable queue contract

`jobs.queue` v1 is an ordinary business service backed by its own managed SQL
store. Source plugins depend on Jobs; Jobs has no dependency on their handlers.
There is no arbitrary-code dispatch or handler import path accepted from users.

Enqueue identifies `(application_id, source, logical_key)` using Core's admitted
caller context. Its canonical input includes handler, payload, retry policy,
retry count, and base retry delay. Identical input deduplicates across process
restarts; differing input returns `jobs_idempotency_conflict`. Logical keys have
at most 128 characters, fixed handler names 64. Payload, result, progress,
checkpoint, and errors must be finite JSON: at most 65,536 UTF-8 bytes, 4,096
values and 16 nesting levels. Idempotency canonical input shares the byte limit.

A single conditional SQL update claims an eligible queued/retry job. It assigns
an opaque attempt token, worker identity, incremented attempts, fixed deadline,
and lease expiry capped by that deadline. Heartbeats renew only a live lease,
and cannot extend the attempt deadline. Checkpoint, completion, failure, and
cancel acknowledgement compare application, source, running status, current
token and live lease/deadline in the mutation. Concurrent or stale attempts
receive `jobs_stale_attempt`; no state is changed. SQL statements are bounded
and do not hold transactions over business handler execution.

Cancellation is a request. Queued/retry jobs cancel immediately; a running job
retains its lease and `cancel_requested=true` until the handler cooperates. Only
a live current attempt can acknowledge `cancelled`. Successful completion can
race with a request and remain `succeeded` with the request flag preserved.
Crashes, shutdown or timeout must not be reported as acknowledged cancellation.

Known handler failure becomes `failed`; unknown effects become `interrupted`.
Under explicit `safe` policy, failure or expiry can instead enter `retry_wait`
while retries remain and no cancellation was requested. Retries number 0–10
additional attempts, with base delay 0.01–3,600 seconds, exponential backoff capped
at 3,600 seconds. Each claim reconciles at most one expired attempt for its source
and registered handlers. It then claims at most one eligible job. This bounds
maintenance work and prevents another source from consuming or recovering jobs.

Public `list_jobs`/`get_job` require `jobs.read`, filter to the admitted
application, and redact lease owner/token. List defaults to 50 rows, at most 100,
with optional source/status filters. Lists return metadata summaries; progress
above 1,024 stored JSON bytes is flagged `progress_truncated` and available via
`get_job`, along with the full payload, checkpoint, result and error. This keeps
both managed-storage and service list responses below the RPC frame limit.
`request_cancel` requires `jobs.cancel` and returns a strict acknowledgement
containing only `id`, `status`, and `cancel_requested`, even for terminal jobs.
Reading retained content requires a separate `get_job` call with `jobs.read`.
Internal owned reads and cancellation also compare the source. Retention is
explicit: the plugin performs no delete or purge and exposes no visitor purge
method. Provider store retention/backup policy remains an operator concern.
