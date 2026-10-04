# Durable Counter integration guide

Durable Counter is an ordinary business plugin. It declares the Jobs queue and
managed SQL store as dependencies, then registers one fixed handler name:

```python
JobRunner(
    context,
    {"count": self._count},
    alias="jobs",
    concurrency=1,
    attempt_timeout=30,
    poll_interval=0.2,
    lease_seconds=5,
)
```

The caller never supplies an import path or arbitrary source owner. `submit`
validates the logical key, target, and delay, then calls the admitted Jobs
dependency with `handler="count"`. The Jobs service supplies the trusted owner
and application identity.

## Checkpoint and replay behavior

Each attempt receives a `JobAttempt` whose `job` snapshot includes `id`,
`logical_key`, `payload`, and the retained checkpoint. The handler checks
`raise_if_cancelled()` before and after each bounded delay. It writes the
absolute completed count to its own managed `counter_state` row in a short
transaction, then saves `{"completed": n}` with progress metadata.

On replay, the handler takes the larger of the retained checkpoint and the
business row's count. If a worker stops after committing the business row but
before saving the checkpoint, the next attempt observes that row and does not
increment the same step again. If cancellation arrives while a job is running,
the handler cooperates between steps; the Jobs record distinguishes a request
from the eventual acknowledged outcome.

The state update is an absolute compare-and-set style write guarded by the
stored count. The retained row and the job checkpoint are both durable, so the
example demonstrates idempotent business effects while preserving the Jobs
service's at-least-once delivery semantics. External side effects would need a
similar idempotency key or an outbox of their own.

The plugin pauses scheduling synchronously during quiescence so Core can drain
already admitted attempts. It stops the runner during disposal. A resumed
worker starts scheduling again against the same retained Jobs and counter
state.

## Service methods

The `jobs-counter.counter` service requires `jobs-counter.write` for `submit`
and `jobs-counter.read` for `list_jobs`, `get_job`, and `list_state`. `list_jobs`
and `get_job` call the Jobs service's owner-scoped internal methods; they cannot
read another plugin's jobs. `list_state` reads only this plugin's retained
counter rows. All methods are scoped business methods and are not anonymous
public APIs.
