"""SQLite-backed contract fixtures for the jobs service.

These fixtures deliberately stop at the public SDK and storage-provider
interfaces.  The peer shim is the small part of Core that a plugin sees: it
forwards a ``service.call`` to the admitted storage alias while assigning the
storage provider its own trusted owner identity.
"""

from __future__ import annotations

import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest
from toolframe_sdk import Migration, RuntimeContext, StoreSchema, call_context
from toolframe_storage_sqlite.plugin import ADMIN_SERVICE, SQL_SERVICE, SqliteProvider

from toolframe_jobs.service import SERVICE_NAME, Jobs

APPLICATION_ID = "test-app"
STORAGE_OWNER = "jobs"


def _migration_schema() -> dict[str, Any]:
    migration_path = (
        Path(__file__).parents[1]
        / "src"
        / "toolframe_jobs"
        / "migrations"
        / "0001_init.sql"
    )
    migration = Migration(revision="v1", sql=migration_path.read_text())
    return StoreSchema(revision="v1", migrations=(migration,)).model_dump()


@contextmanager
def trusted_call(
    owner: str = "alpha",
    *,
    generation: str = "generation-1",
    root: str | None = None,
    deadline: float | None = None,
    application_id: str = APPLICATION_ID,
):
    """Install the Core-issued identity that a plugin call would receive."""

    token = call_context.set(
        {
            "application_id": application_id,
            "owner": owner,
            "owner_generation": generation,
            "root_id": root or uuid.uuid4().hex,
            "chain": [],
            "deadline": deadline if deadline is not None else time.time() + 300,
        }
    )
    try:
        yield
    finally:
        call_context.reset(token)


class StoragePeer:
    """Route an SDK storage call to a real SQLite provider instance."""

    def __init__(self, provider: SqliteProvider):
        self.provider = provider
        self.calls: list[tuple[str, str]] = []
        self.generation = uuid.uuid4().hex

    async def request(self, operation: str, params: dict[str, Any], *, timeout=None):
        if operation != "service.call":
            raise AssertionError(f"unexpected peer operation: {operation}")
        if params.get("alias") != "storage":
            raise AssertionError(f"unexpected storage alias: {params.get('alias')}")

        method = params["method"]
        self.calls.append((SQL_SERVICE, method))
        caller = call_context.get() or {}
        provider_info = {
            "application_id": caller.get("application_id", APPLICATION_ID),
            "owner": STORAGE_OWNER,
            "owner_generation": self.generation,
            "root_id": params.get("root_id")
            or caller.get("root_id")
            or uuid.uuid4().hex,
            "chain": caller.get("chain", []),
            # The test harness gives calls a long deadline.  Keeping that
            # deadline in the provider context exercises the same fencing
            # path as a Core admitted dependency call.
            "deadline": caller.get("deadline", time.time() + 300),
        }
        token = call_context.set(provider_info)
        try:
            return await self.provider.invoke(
                SQL_SERVICE, method, params.get("params", {})
            )
        finally:
            call_context.reset(token)


async def make_provider(state_directory: Path) -> SqliteProvider:
    provider = SqliteProvider(
        RuntimeContext(
            None,
            plugin_id="storage-sqlite",
            generation=uuid.uuid4().hex,
            state_directory=state_directory,
        )
    )
    await provider.activate()
    admin_info = {
        "application_id": APPLICATION_ID,
        "owner": STORAGE_OWNER,
        "management": True,
        "root_id": uuid.uuid4().hex,
        "owner_generation": uuid.uuid4().hex,
        "deadline": time.time() + 300,
    }
    token = call_context.set(admin_info)
    try:
        await provider.invoke(
            ADMIN_SERVICE,
            "ensure_store",
            {"owner": STORAGE_OWNER, "schema": _migration_schema()},
        )
    finally:
        call_context.reset(token)
    return provider


@pytest.fixture
async def provider(tmp_path):
    provider = await make_provider(tmp_path)
    try:
        yield provider
    finally:
        await provider.dispose()


@pytest.fixture
def provider_factory():
    return make_provider


@pytest.fixture
def make_jobs(provider):
    """Create independent Jobs contexts against one managed store."""

    def factory(source: str = "alpha", provider_override=None) -> Jobs:
        selected_provider = provider_override or provider
        peer = StoragePeer(selected_provider)
        context = RuntimeContext(
            peer,
            plugin_id="jobs",
            generation=uuid.uuid4().hex,
        )
        plugin = Jobs(context)
        # Expose the peer for tests that need to prove calls traversed the
        # public SDK route without making it part of the production contract.
        plugin._test_peer = peer
        plugin._test_source = source
        return plugin

    return factory


@pytest.fixture
def jobs(make_jobs):
    return make_jobs("alpha")


@pytest.fixture
def call():
    async def invoke(
        plugin: Jobs,
        method: str,
        *,
        owner: str | None = None,
        generation: str = "generation-1",
        root: str | None = None,
        deadline: float | None = None,
        **params: Any,
    ):
        owner = owner or getattr(plugin, "_test_source", "alpha")
        with trusted_call(
            owner,
            generation=generation,
            root=root,
            deadline=deadline,
        ):
            return await plugin.invoke(SERVICE_NAME, method, params)

    return invoke
