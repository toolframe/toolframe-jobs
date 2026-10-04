# Toolframe Jobs

Toolframe Jobs keeps durable business work in a managed SQL store and provides a
bounded worker helper. Business plugins declare `jobs.queue` as a dependency,
enqueue internally, and pull only their own registered handlers. The package
publishes a queue Dashboard, scoped read/cancel API, two read-only MCP tools, and
an llms document.

## Quick start

Requires Python 3.14+, Toolframe SDK 0.3.0, and matching SDK/Core builds supporting
`RuntimeContext.task(..., timeout=seconds)`. Build from this checkout:

```sh
uv build --no-sources
# Local development: build the matching unpublished SDK/provider wheels.
uv build ../toolframe-sdk --out-dir ./build/wheelhouse
uv build ../toolframe-storage-sqlite --out-dir ./build/wheelhouse
toolframe --home ../jobs-app init --name Jobs
# Set [packages] find_links = ["../toolframe-jobs/build/wheelhouse"]
# in ../jobs-app/toolframe.toml, then start in another terminal:
toolframe --home ../jobs-app serve
toolframe --home ../jobs-app plugin add wheel:../toolframe-jobs/build/wheelhouse/toolframe_storage_sqlite-0.3.0-py3-none-any.whl
toolframe --home ../jobs-app plugin add wheel:../toolframe-jobs/dist/toolframe_jobs-0.1.0-py3-none-any.whl
toolframe --home ../jobs-app plugin enable storage-sqlite
toolframe --home ../jobs-app plugin enable jobs
```

Sources and package links are relative to the application home. The sibling
paths in this example are a local development layout; an
installed application can use the matching released provider instead. Bind
`toolframe.storage.sql` to the selected provider when more than one is present.
Install/configure the Dashboard, MCP, and llms providers separately if needed.
The Jobs runtime is headless; the native Dashboard uses the optional `ui` extra
(`toolframe-jobs[ui]`). All page code and migration/document assets ship in the
wheel. Jobs does not require Audit or Backup plugins.

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
