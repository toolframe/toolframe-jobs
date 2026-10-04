# Integrating a bounded runner

Declare `Requirement(service="jobs.queue", version=1)` with alias `jobs` in a
business plugin. Producers call `context.call("jobs", "enqueue", ...)` from an
admitted business operation; Jobs reads the source/application from Core.

```python
from toolframe_jobs.runner import JobRunner

self.runner = JobRunner(
    self.context,
    {"resize": self.resize},
    alias="jobs",
    concurrency=2,
    attempt_timeout=30,
    poll_interval=1,
    lease_seconds=10,
)
self.runner.start()


async def resize(self, attempt):
    await attempt.raise_if_cancelled()
    # Make business effects idempotent under attempt.logical_key.
    completed = (attempt.checkpoint or {}).get("completed", 0)
    await attempt.save_checkpoint(
        {"completed": completed}, progress={"step": completed}
    )
    return {"completed": completed}
```

The handler mapping is fixed in plugin code. `JobAttempt` exposes the job
snapshot, `id`, `logical_key`, payload and checkpoint; its async checkpoint,
heartbeat and cancellation helpers use the fenced token. Catch no
`JobCancelled` inside a handler unless the handler can safely continue: the
runner acknowledges cancellation when that exception escapes. Merely requesting
cancellation does not prove effects stopped.

On quiesce, call `runner.pause()` then quiesce the context and return promptly;
Core drains already admitted attempts. On dispose, await `runner.stop()` before
closing the context. Resume the context then call `runner.start()`. A timer only schedules finite managed claim
and attempt roots; it does not retain an infinite admitted root. Each attempt
uses the configured finite Core timeout; the helper reserves a short part of
that deadline to record timeout outcomes before the root expires; heartbeats are joined before that
attempt ends. Core's `runtime.max_task_timeout` must admit the configured
attempt timeout. Stop prevents further scheduling and cancels active handlers;
when cleanup cannot obtain an admitted root, lease expiry preserves an honest
unknown outcome for later recovery.

Use `retry_policy="never", max_retries=0` for handlers with unknown external
side effects. Set `retry_policy="safe"` and a bounded retry count only when the
business contract makes repeated execution safe. Persist deduplication at the
business effect boundary or use an external system's idempotency key. Checkpoints
help resume work but do not themselves prevent duplicate external effects.
See the counter example for deterministic retained state with an absolute,
idempotent update and a separate job checkpoint.
