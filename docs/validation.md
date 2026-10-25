# Validation

Build matching SDK/storage wheels into a local wheel directory when those
versions are not published, then run from the repository:

```sh
uv sync --group dev --find-links ./build/wheelhouse
uv run --no-sync pytest
uv run --no-sync ruff check .
uv run --no-sync ruff format --check .
uv build --no-sources
uv run --no-sync python scripts/smoke_wheel.py dist/toolframe_jobs-0.2.1-py3-none-any.whl --find-links ./build/wheelhouse
uv build examples/durable-counter --no-sources
```

Tests use temporary provider stores. Queue tests exercise the real managed
SQLite provider via public SDK storage contracts with a test routing adapter;
they do not prove Core admission or permission enforcement. Runner simulations
exercise scheduling, bounded tasks, cancellation and lifecycle behavior without
an application. Page tests use Streamlit's application test harness.

Installed acceptance requires a separate temporary application, matching Core
and SDK finite-task contract builds, and the counter plugin. Exercise scoped API,
MCP and Dashboard reads, denied mutations for read-only principals, deterministic
submit/dedup/conflict, interrupt/resume and application restart with retained
state. Record exact package revisions and results separately from these
reproducible commands. Do not run acceptance against a user's existing data.

Run installed acceptance against a supplied wheel directory containing matching
Core/SDK and the SQLite, auth, Dashboard auth, Dashboard, MCP auth, MCP and llms
providers:

```sh
uv run --no-sync python scripts/verify_installed.py \
  --wheelhouse ./build/wheelhouse \
  --jobs-wheel dist/toolframe_jobs-0.2.1-py3-none-any.whl \
  --example-wheel dist/toolframe_jobs_counter-0.1.0-py3-none-any.whl \
  --output .local/verify-installed.json
```

The verifier creates its own application home and ports, stages immutable wheels,
and cleans up only its own process groups. It checks scoped API denial, live MCP
reads, Dashboard catalog/reachability, llms publication, interrupted-worker
checkpoint recovery, cooperative cancellation, and reload draining. Browser
pixel rendering and live PostgreSQL remain separate checks.
