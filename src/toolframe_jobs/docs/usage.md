# Toolframe Jobs

Toolframe Jobs is a durable, owner-scoped queue for business plugins that need
bounded background work. Install `toolframe-jobs` alongside a managed SQL
provider, enable the `jobs` plugin, and grant `jobs.read` to operators that need
the queue dashboard or read APIs. Grant `jobs.cancel` separately when they may
request cancellation.

The plugin publishes `jobs.queue@1`:

| Method | Scope | Purpose |
| --- | --- | --- |
| `list_jobs(limit=50, status=None, source=None)` | `jobs.read` | Return at most 100 retained job summaries. |
| `get_job(job_id)` | `jobs.read` | Read one retained job. |
| `request_cancel(job_id)` | `jobs.cancel` | Ask a queued or running job to stop cooperatively. |

The three methods are scoped business service methods. The MCP surface publishes
read-only `list_jobs` and `get_job` tools; cancellation is available through the
scoped service only.

## Enqueue from a business plugin

Declare Jobs as a dependency under the alias `jobs`, then call the admitted
internal service from the business plugin. The producer's trusted owner and
application identity are recorded by the Jobs service; callers cannot choose
those fields.

```python
result = await self.context.call(
    "jobs",
    "enqueue",
    {
        "logical_key": "invoice-42",
        "handler": "rebuild_invoice",
        "payload": {"invoice_id": 42},
        "retry_policy": "safe",
        "max_retries": 3,
        "retry_delay": 2.0,
    },
)
job = result
```

`logical_key` is limited to 128 characters and is unique for the producer and
application. Reusing it with identical input deduplicates across restarts;
reusing it with different input is an idempotency conflict. Handler names are
fixed names registered by the worker, up to 64 characters. Payloads, results,
progress, and checkpoints are JSON values subject to the service's bounded
size/depth validation.

## Lifecycle and safety

Jobs move through `queued`, `running`, `retry_wait`, and terminal
`succeeded`, `failed`, `cancelled`, or `interrupted` states. A lost attempt has an unknown outcome. On lease reconciliation it becomes
`retry_wait` only when explicit `safe` policy allows replay and retries remain;
otherwise it becomes terminal `interrupted`. The default policy never blindly
replays an unknown external side effect. Delivery is at least once, so handlers must make
external effects idempotent with the logical key or their own durable receipt.
Exactly-once external effects are not promised.

Cancellation is a request separate from acknowledgement. Queued work can stop
immediately; a running handler must observe its cancellation access and call the
acknowledgement path. A timeout or crash retains an unknown outcome rather than acknowledged cancellation.
Attempt tokens and leases fence stale workers, so an expired worker cannot
complete, checkpoint, heartbeat, or cancel a newer attempt.

Jobs and terminal results remain stored until an explicit operator retention
action removes them. The plugin does not silently purge history. Keep
filters and polling bounded (`limit` is 1–100), and use the dashboard's source
filter when several business plugins share an application.

The dashboard and MCP resources are optional presentation surfaces. The durable
service still requires `toolframe.storage.sql` and the host applies the bundled
v1 migration. PostgreSQL migration SQL is shipped, but live PostgreSQL support
must be validated against the provider and deployment in use.

Job lists return bounded metadata summaries. Use `get_job` for full payload,
checkpoint, result and error. Progress larger than 1,024 JSON bytes is flagged
`progress_truncated` in a list and retained in full in the detail.
