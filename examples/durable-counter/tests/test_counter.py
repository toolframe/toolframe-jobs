from __future__ import annotations

from types import SimpleNamespace

import pytest
from toolframe_sdk import PluginError

from toolframe_demo_durable_counter.plugin import SERVICE_NAME, plugin
from toolframe_demo_durable_counter.service import DurableCounter


class MemoryTransaction:
    def __init__(self, storage):
        self.storage = storage

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    async def execute(self, sql, parameters=(), **kwargs):
        del kwargs
        if sql.startswith("INSERT"):
            key, updated_at = parameters
            self.storage.rows.setdefault(
                key,
                {
                    "logical_key": key,
                    "count": 0,
                    "version": 1,
                    "updated_at": updated_at,
                },
            )
        elif sql.startswith("UPDATE"):
            completed, updated_at, key, guard = parameters
            row = self.storage.rows[key]
            if row["count"] < guard:
                row.update(
                    count=completed,
                    version=row["version"] + 1,
                    updated_at=updated_at,
                )
        return {"rowcount": 1}

    async def query(self, sql, parameters=()):
        key = parameters[0]
        row = self.storage.rows.get(key)
        if row is None:
            return []
        return [{"count": row["count"]}]


class MemoryStorage:
    def __init__(self):
        self.rows = {}

    async def query(self, sql, parameters=()):
        del sql
        key = parameters[0]
        row = self.rows.get(key)
        return [] if row is None else [{"count": row["count"]}]

    def transaction(self):
        return MemoryTransaction(self)


class CounterContext:
    def __init__(self):
        self.store = MemoryStorage()
        self.calls = []

    def storage(self):
        return self.store

    async def call(self, alias, method, params):
        self.calls.append((alias, method, params))
        if method == "enqueue":
            return {"id": "job-1", "logical_key": params["logical_key"]}
        return {"id": params.get("job_id", "job-1")}


class Attempt:
    def __init__(self, *, checkpoint=None):
        self.job = SimpleNamespace(
            logical_key="order-42",
            payload={"steps": 3, "delay_seconds": 0},
        )
        self.checkpoint = checkpoint
        self.checkpoints = []

    async def raise_if_cancelled(self):
        return None

    async def save_checkpoint(self, checkpoint, progress=None):
        self.checkpoints.append((checkpoint, progress))


def test_definition_declares_jobs_and_storage_dependencies():
    assert plugin.id == "jobs-counter"
    assert plugin.factory == "toolframe_demo_durable_counter.service:DurableCounter"
    assert plugin.requires["jobs"].service == "jobs.queue"
    assert plugin.requires["storage"].service == "toolframe.storage.sql"
    assert plugin.services[SERVICE_NAME].methods["submit"].public_api is False
    assert plugin.store.revision == "v1"


@pytest.mark.asyncio
async def test_submit_uses_fixed_handler_and_retry_policy():
    context = CounterContext()
    service = DurableCounter(context)

    result = await service.submit("  order-42  ", steps=3, delay_seconds=0)

    assert result["id"] == "job-1"
    assert context.calls == [
        (
            "jobs",
            "enqueue",
            {
                "logical_key": "order-42",
                "handler": "count",
                "payload": {"steps": 3, "delay_seconds": 0.0},
                "retry_policy": "safe",
                "max_retries": 3,
                "retry_delay": 0.2,
            },
        )
    ]


@pytest.mark.asyncio
async def test_get_job_uses_string_job_identity_and_owned_read():
    context = CounterContext()
    service = DurableCounter(context)

    await service.get_job("a1b2c3")

    assert context.calls == [("jobs", "get_owned", {"job_id": "a1b2c3"})]
    with pytest.raises(PluginError):
        await service.get_job(1)


@pytest.mark.asyncio
async def test_replay_reads_retained_state_before_incrementing_again():
    context = CounterContext()
    service = DurableCounter(context)
    first = Attempt(checkpoint={"completed": 0})

    assert await service._count(first) == {"completed": 3, "logical_key": "order-42"}
    assert context.store.rows["order-42"]["count"] == 3
    assert [item[0] for item in first.checkpoints] == [
        {"completed": 1},
        {"completed": 2},
        {"completed": 3},
    ]

    replay = Attempt(checkpoint={"completed": 0})
    assert await service._count(replay) == {
        "completed": 3,
        "logical_key": "order-42",
    }
    assert replay.checkpoints == []
    assert context.store.rows["order-42"]["count"] == 3


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("logical_key", "   "),
        ("steps", 0),
        ("steps", 101),
        ("delay_seconds", 1.1),
        ("delay_seconds", float("inf")),
    ],
)
@pytest.mark.asyncio
async def test_submit_rejects_out_of_bounds_values(field, value):
    context = CounterContext()
    service = DurableCounter(context)
    params = {"logical_key": "key"}
    params[field] = value
    with pytest.raises(PluginError) as error:
        await service.submit(**params)
    assert error.value.code == "jobs_counter_validation"
