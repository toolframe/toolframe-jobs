"""Concurrent transition and RPC response bounds against real managed SQLite."""

import json
from types import SimpleNamespace

import pytest
from toolframe_sdk import MAX_FRAME_BYTES

from toolframe_jobs import service


async def test_cancellation_racing_expiry_recovery_never_strands_retry(
    monkeypatch, jobs, call
):
    clock = [1000.0]
    monkeypatch.setattr(service, "time", SimpleNamespace(time=lambda: clock[0]))
    queued = await call(
        jobs,
        "enqueue",
        deadline=10**12,
        logical_key="race",
        handler="work",
        payload={},
        retry_policy="safe",
        max_retries=1,
    )
    await call(
        jobs,
        "claim",
        deadline=10**12,
        handlers=["work"],
        worker_id="first",
        lease_seconds=0.1,
    )
    clock[0] += 0.2
    peer = jobs._test_peer
    original = peer.request
    raced = False

    async def request(operation, params, **kwargs):
        nonlocal raced
        result = await original(operation, params, **kwargs)
        sql = params.get("params", {}).get("sql", "")
        if not raced and sql.startswith("SELECT id, attempt_token, attempts"):
            raced = True
            await jobs.request_cancel_owned(queued["id"])
        return result

    monkeypatch.setattr(peer, "request", request)
    assert (
        await call(
            jobs, "claim", deadline=10**12, handlers=["work"], worker_id="second"
        )
        is None
    )
    row = await call(jobs, "get_owned", deadline=10**12, job_id=queued["id"])
    assert raced and row["cancel_requested"]
    assert row["status"] == "interrupted"


async def test_large_retained_rows_list_within_both_rpc_boundaries(
    jobs, call, monkeypatch
):
    peer = jobs._test_peer
    original = peer.request

    async def bounded_request(operation, params, **kwargs):
        result = await original(operation, params, **kwargs)
        assert len(json.dumps(result).encode()) < MAX_FRAME_BYTES
        return result

    monkeypatch.setattr(peer, "request", bounded_request)
    for index in range(20):
        await call(
            jobs,
            "enqueue",
            logical_key=f"large-{index}",
            handler="work",
            payload={"data": "x" * 60000},
        )
    job = await call(jobs, "claim", handlers=["work"], worker_id="worker")
    await call(
        jobs,
        "save_checkpoint",
        job_id=job["id"],
        attempt_token=job["attempt_token"],
        checkpoint={"data": "x" * 60000},
        progress={"data": "x" * 60000},
    )
    result = await call(jobs, "list_owned", limit=100)
    assert result["count"] == 20
    assert len(json.dumps(result).encode()) < MAX_FRAME_BYTES
    summary = next(row for row in result["jobs"] if row["id"] == job["id"])
    assert summary["progress_truncated"] and summary["progress"] is None
    assert "payload" not in summary and "checkpoint" not in summary
    detail = await call(jobs, "get_owned", job_id=job["id"])
    assert len(detail["payload"]["data"]) == 60000
    assert len(detail["progress"]["data"]) == 60000


async def test_invoke_does_not_mask_provider_type_error(monkeypatch, jobs, call):
    async def broken(*args, **kwargs):
        raise TypeError("provider implementation defect")

    monkeypatch.setattr(jobs._test_peer, "request", broken)
    with pytest.raises(TypeError, match="provider implementation defect"):
        await call(jobs, "list_owned")
