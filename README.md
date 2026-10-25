# Toolframe Jobs

Toolframe Jobs keeps durable business work in a managed SQL store and provides a
bounded worker helper. Business plugins declare `jobs.queue` as a dependency,
enqueue internally, and pull only their own registered handlers. The package
publishes a queue Dashboard, scoped read/cancel API, two read-only MCP tools, and
an llms document.

## Quick start

Requires Python 3.14+, Core 0.7.0, SDK 0.4.0 and an active SQL storage provider.
The [Entry Register template](https://github.com/toolframe/toolframe-core#readme)
provides storage, Dashboard, MCP and llms providers. With the application running:

```sh
toolframe --home APP plugin add pypi:toolframe-jobs==0.2.0 --enable
toolframe --home APP plugin status jobs --check
```

Try the [separate deterministic counter plugin](examples/durable-counter/README.md)
for submit, checkpoint, stop, resume, and retained business state. Its source can
be added with `path:../toolframe-jobs/examples/durable-counter` from this checkout.

## Support limits

Delivery is **at least once** when a business plugin explicitly opts into safe
retries. Jobs never promises exactly-once external effects. The default retry
policy is `never`; a lost attempt becomes `interrupted` with unknown effects,
rather than being silently replayed. Explicit `safe` retries require the handler
to make business effects idempotent under the stable logical key. A checkpoint
and an external effect cannot generally be committed atomically.

SQLite is exercised through the real managed provider. A PostgreSQL migration is
included using the same portable SQL transition statements; live PostgreSQL is
not a validated support claim. Terminal results remain until an operator takes
an explicit retention action. There is no background purge or automatic restart
of terminal interrupted jobs. Lease expiry is reconciled when that source next
claims work; a stopped source may display an expired running lease until then.

Visitor calls require `jobs.read` or `jobs.cancel`. Internal enqueue/claim/mutate
methods are reached only through admitted plugin dependencies, and derive source
and application identity from Core. Visitors cannot enqueue arbitrary handlers.
Read-only MCP tools expose no mutations.

Read [the runner guide](docs/runner.md) for integration and retry safety,
[the storage contract](docs/architecture.md) for transitions, and
[validation](docs/validation.md) for reproducible checks. Licensed under
[Apache 2.0](LICENSE).

For release validation and Trusted Publisher setup, see [releasing](docs/releasing.md).

For development hooks and contribution rules, see [Contributing](CONTRIBUTING.md).

Plugin dependencies allow compatible SDK 0.x updates from the declared minimum.
See the [SDK compatibility policy](https://github.com/toolframe/toolframe-sdk/blob/main/docs/compatibility.md) for API guarantees and minimum/latest verification.
