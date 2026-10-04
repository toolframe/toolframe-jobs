# Durable Counter

Durable Counter is a small, installable Toolframe business plugin that shows a
safe way to use the Jobs service for resumable work. It accepts a logical key,
counts to a bounded target, and retains application state so a retry or worker
restart never increments a completed step twice.

## Quick start

Follow the [Jobs setup](../../README.md) to enable Jobs and a managed storage
provider in a separate application. From the repository root, build and add
the counter example:

```sh
uv build examples/durable-counter --no-sources --out-dir dist
toolframe --home ../jobs-app plugin add wheel:../toolframe-jobs/dist/toolframe_jobs_counter-0.1.0-py3-none-any.whl --enable
```

Source paths are relative to the application home. Include the Jobs wheel and
matching SDK in the application's `packages.find_links` so the counter runtime
can resolve its pinned dependencies. The local wheel workflow also works before
these packages are published.
The Jobs plugin is bound by its `jobs.queue` service name through dependency
admission. Grant an admitted caller `jobs-counter.write` to submit work and
`jobs-counter.read` to inspect owned jobs or retained counter state. The
business service is `jobs-counter.counter`; its scoped methods are `submit`,
`list_jobs`, `get_job`, and `list_state`.

```json
{
  "logical_key": "demo-order-42",
  "steps": 10,
  "delay_seconds": 0.2
}
```

Send the JSON object to the admitted scoped endpoint
`POST /_toolframe/v1/services/jobs-counter/jobs-counter.counter/submit`.

`submit` forwards the request to the owner-fenced Jobs service with the fixed
`count` handler and a safe retry policy. Repeating the same logical key and
payload deduplicates; changing the payload for that key is a conflict.

See [the integration guide](src/toolframe_demo_durable_counter/docs/usage.md)
for the runner contract, interruption behavior, and retry guidance.

## Support limits

The example accepts 1–100 steps and a delay from 0 to 1 second. The worker
runner uses one concurrent worker, a 30-second attempt timeout, five-second
leases, and at most three safe retries with a 0.2-second retry delay. A long
job therefore resumes from its checkpoint across attempts. Delivery is
at-least-once; callers must treat the returned job as durable work and should
not assume exactly-once external effects.

The package owns only its counter table. Jobs retain job records until an explicit
operator action removes them; the plugin performs no hidden purge. PostgreSQL migration
SQL is included, but this example's automated checks exercise managed SQLite.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) and
[NOTICE](NOTICE).
