"""Behavioral tests for the owner-fenced durable jobs service.

The tests call ``Jobs`` through an SDK ``RuntimeContext`` and a real managed
SQLite provider.  The provider is used through its public service contract;
these are provider-contract tests and do not claim Core permission coverage.
"""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest
from toolframe_sdk import PluginError

import toolframe_jobs.service as jobs_service

LONG_DEADLINE = 10**12


async def enqueue(call, plugin, key="job-1", **params):
    return await call(
        plugin,
        "enqueue",
        logical_key=params.pop("logical_key", key),
        handler=params.pop("handler", "worker"),
        payload=params.pop("payload", {"value": 1}),
        **params,
    )


class FakeClock:
    def __init__(self, value: float = 1_000.0):
        self.value = value

    def time(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


def assert_error(error: PluginError, code: str) -> None:
    assert error.code == code, error.as_dict()


async def claim(call, plugin, *, worker_id="worker-1", handlers=None, **params):
    return await call(
        plugin,
        "claim",
        handlers=handlers or ["worker"],
        worker_id=worker_id,
        **params,
    )


async def fake_call(call, plugin, method, **params):
    return await call(plugin, method, deadline=LONG_DEADLINE, **params)


@pytest.mark.parametrize("status", jobs_service.STATUSES)
async def test_public_cancel_returns_only_acknowledgement(jobs, call, status):
    queued = await enqueue(
        call,
        jobs,
        payload={"private_payload": True},
        retry_policy="safe" if status == "retry_wait" else "never",
        max_retries=1 if status == "retry_wait" else 0,
    )
    job_id = queued["id"]
    if status != "queued":
        running = await claim(call, jobs)
        token = running["attempt_token"]
        await call(
            jobs,
            "save_checkpoint",
            job_id=job_id,
            attempt_token=token,
            checkpoint={"private_checkpoint": True},
            progress={"private_progress": True},
        )
        if status == "succeeded":
            await call(
                jobs,
                "complete",
                job_id=job_id,
                attempt_token=token,
                result={"private_result": True},
            )
        elif status in ("failed", "interrupted", "retry_wait"):
            await call(
                jobs,
                "fail",
                job_id=job_id,
                attempt_token=token,
                error={"private_error": True},
                outcome="unknown" if status == "interrupted" else "failed",
            )
        elif status == "cancelled":
            await call(jobs, "request_cancel_owned", job_id=job_id)
            await call(jobs, "acknowledge_cancel", job_id=job_id, attempt_token=token)

    before = await call(jobs, "get_job", job_id=job_id)
    assert before["status"] == status
    # A public cancellation may cross producer boundaries within the application.
    cancelled = await call(jobs, "request_cancel", owner="beta", job_id=job_id)
    assert cancelled == {
        "id": job_id,
        "status": "cancelled" if status in ("queued", "retry_wait") else status,
        "cancel_requested": status not in ("succeeded", "failed", "interrupted"),
    }
    detail = await call(jobs, "get_job", job_id=job_id)
    for field in ("payload", "checkpoint", "progress", "result", "error"):
        assert detail[field] == before[field]
    assert "attempt_token" not in detail and "lease_owner" not in detail
    if status in jobs_service.TERMINAL:
        assert detail == before


async def test_owned_cancel_retains_snapshot_and_application_fence(jobs, call):
    from conftest import APPLICATION_ID, trusted_call

    queued = await enqueue(call, jobs, payload={"private_payload": True})
    job_id = queued["id"]
    with (
        trusted_call(application_id="other-application"),
        pytest.raises(PluginError) as hidden,
    ):
        await jobs.invoke(
            jobs_service.SERVICE_NAME, "request_cancel", {"job_id": job_id}
        )
    assert_error(hidden.value, "store_identity_mismatch")
    # Also exercise Jobs' application predicate independently of the store fence.
    with trusted_call():
        await jobs.context.sql().execute(
            "UPDATE jobs SET application_id = ? WHERE id = ?",
            ("other-application", job_id),
        )
    try:
        with pytest.raises(PluginError) as hidden_row:
            await call(jobs, "request_cancel", job_id=job_id)
        assert_error(hidden_row.value, "jobs_not_found")
    finally:
        with trusted_call():
            await jobs.context.sql().execute(
                "UPDATE jobs SET application_id = ? WHERE id = ?",
                (APPLICATION_ID, job_id),
            )
    assert not (await call(jobs, "get_job", job_id=job_id))["cancel_requested"]
    owned = await call(jobs, "request_cancel_owned", job_id=job_id)
    assert owned["payload"] == {"private_payload": True}
    assert owned["status"] == "cancelled" and owned["cancel_requested"]
    assert "attempt_token" not in owned and "lease_owner" not in owned


@pytest.mark.asyncio
async def test_state_transitions_checkpoint_heartbeat_and_completion(jobs, call):
    queued = await enqueue(call, jobs, payload={"value": 7})
    assert queued["status"] == "queued"
    assert queued["attempts"] == 0

    running = await fake_call(
        call,
        jobs,
        "claim",
        handlers=["worker"],
        worker_id="worker-1",
        lease_seconds=5,
        attempt_timeout=20,
    )
    assert running["status"] == "running"
    token = running["attempt_token"]
    assert token and running["lease_owner"] == "worker-1"

    checkpointed = await fake_call(
        call,
        jobs,
        "save_checkpoint",
        job_id=running["id"],
        attempt_token=token,
        checkpoint={"cursor": 3},
        progress={"completed": 3},
    )
    assert checkpointed["checkpoint"] == {"cursor": 3}
    assert checkpointed["progress"] == {"completed": 3}

    heartbeated = await fake_call(
        call,
        jobs,
        "heartbeat",
        job_id=running["id"],
        attempt_token=token,
        lease_seconds=5,
    )
    assert heartbeated["lease_expires_at"] >= running["lease_expires_at"]

    completed = await fake_call(
        call,
        jobs,
        "complete",
        job_id=running["id"],
        attempt_token=token,
        result={"answer": 42},
    )
    assert completed["status"] == "succeeded"
    assert completed["result"] == {"answer": 42}
    assert completed["attempt_token"] is None
    retained = await fake_call(call, jobs, "get_owned", job_id=running["id"])
    assert retained["status"] == "succeeded"
    assert retained["checkpoint"] == {"cursor": 3}
    assert "attempt_token" not in retained and "lease_owner" not in retained


@pytest.mark.asyncio
async def test_idempotency_deduplicates_after_service_and_provider_restart(
    provider, provider_factory, make_jobs, call, tmp_path
):
    first_jobs = make_jobs("alpha")
    first = await enqueue(call, first_jobs, key="stable", payload={"x": 1})
    duplicate = await enqueue(call, first_jobs, key="stable", payload={"x": 1})
    assert duplicate["id"] == first["id"]

    second_jobs = make_jobs("alpha")
    persisted = await fake_call(call, second_jobs, "get_owned", job_id=first["id"])
    assert persisted["payload"] == {"x": 1}

    # A fresh provider process opens the same managed store and sees the same
    # row; the test does not reach into SQLite to bypass the provider API.
    restarted_provider = await provider_factory(tmp_path)
    try:
        restarted_jobs = make_jobs("alpha", restarted_provider)
        after_restart = await fake_call(
            call, restarted_jobs, "get_owned", job_id=first["id"]
        )
        assert after_restart["id"] == first["id"]
        with pytest.raises(PluginError) as conflict:
            await enqueue(
                call,
                restarted_jobs,
                key="stable",
                payload={"x": 2},
            )
        assert_error(conflict.value, "jobs_idempotency_conflict")
    finally:
        await restarted_provider.dispose()


@pytest.mark.asyncio
async def test_source_isolation_and_visitor_redaction(make_jobs, jobs, call):
    alpha = await enqueue(call, jobs, key="alpha-job")
    beta_jobs = make_jobs("beta")
    beta = await enqueue(call, beta_jobs, key="beta-job")

    with pytest.raises(PluginError) as hidden:
        await fake_call(call, beta_jobs, "get_owned", job_id=alpha["id"])
    assert_error(hidden.value, "jobs_not_found")

    alpha_running = await fake_call(
        call,
        jobs,
        "claim",
        handlers=["worker"],
        worker_id="alpha-worker",
    )
    assert alpha_running["id"] == alpha["id"]
    token = alpha_running["attempt_token"]

    with pytest.raises(PluginError) as mutation:
        await fake_call(
            call,
            beta_jobs,
            "complete",
            job_id=alpha["id"],
            attempt_token=token,
            result={"bad": True},
        )
    assert_error(mutation.value, "jobs_stale_attempt")

    visitor = await fake_call(call, beta_jobs, "get_job", job_id=alpha["id"])
    assert visitor["id"] == alpha["id"]
    assert visitor["source"] == "alpha"
    assert "attempt_token" not in visitor
    assert "lease_owner" not in visitor
    beta_running = await fake_call(
        call,
        beta_jobs,
        "claim",
        handlers=["worker"],
        worker_id="beta-worker",
    )
    assert beta_running["id"] == beta["id"]
    # Beta's worker can only claim beta's own row; the returned ID proves that
    # claim ownership follows the trusted source identity.
    assert beta["source"] == "beta"

    with pytest.raises(PluginError) as cancel_owned:
        await fake_call(call, beta_jobs, "request_cancel_owned", job_id=alpha["id"])
    assert_error(cancel_owned.value, "jobs_not_found")


@pytest.mark.asyncio
async def test_concurrent_claims_have_one_winner(jobs, call):
    await enqueue(call, jobs, key="single-claim")

    async def attempt(index: int):
        return await fake_call(
            call,
            jobs,
            "claim",
            handlers=["worker"],
            worker_id=f"worker-{index}",
            root=f"claim-root-{index}",
        )

    results = await asyncio.gather(*(attempt(index) for index in range(20)))
    winners = [item for item in results if item is not None]
    assert len(winners) == 1
    assert winners[0]["status"] == "running"
    assert winners[0]["attempts"] == 1


@pytest.mark.asyncio
async def test_expired_safe_attempt_fences_stale_completion(monkeypatch, jobs, call):
    clock = FakeClock()
    # Replacing the module reference avoids freezing the provider/SDK's
    # process clock while exercising service lease calculations.
    monkeypatch.setattr(jobs_service, "time", SimpleNamespace(time=clock.time))
    queued = await fake_call(
        call,
        jobs,
        "enqueue",
        logical_key="fenced",
        handler="worker",
        payload={"x": 1},
        retry_policy="safe",
        max_retries=1,
        retry_delay=1,
    )
    first = await fake_call(
        call,
        jobs,
        "claim",
        handlers=["worker"],
        worker_id="first",
        lease_seconds=0.1,
        attempt_timeout=10,
    )
    assert first["id"] == queued["id"]
    first_token = first["attempt_token"]
    clock.advance(0.2)
    assert (
        await fake_call(
            call,
            jobs,
            "claim",
            handlers=["worker"],
            worker_id="second",
            lease_seconds=0.1,
            attempt_timeout=10,
        )
        is None
    )
    waiting = await fake_call(call, jobs, "get_owned", job_id=queued["id"])
    assert waiting["status"] == "retry_wait"
    clock.advance(1.1)
    second = await fake_call(
        call,
        jobs,
        "claim",
        handlers=["worker"],
        worker_id="second",
        lease_seconds=5,
        attempt_timeout=10,
    )
    assert second["attempts"] == 2
    with pytest.raises(PluginError) as stale:
        await fake_call(
            call,
            jobs,
            "complete",
            job_id=queued["id"],
            attempt_token=first_token,
            result={"stale": True},
        )
    assert_error(stale.value, "jobs_stale_attempt")
    done = await fake_call(
        call,
        jobs,
        "complete",
        job_id=queued["id"],
        attempt_token=second["attempt_token"],
        result={"ok": True},
    )
    assert done["status"] == "succeeded"


@pytest.mark.asyncio
async def test_expiry_policy_distinguishes_never_from_bounded_safe_retry(
    monkeypatch, jobs, call
):
    clock = FakeClock()
    monkeypatch.setattr(jobs_service, "time", SimpleNamespace(time=clock.time))
    never = await fake_call(
        call,
        jobs,
        "enqueue",
        logical_key="never-replay",
        handler="worker",
        payload={},
    )
    await fake_call(
        call,
        jobs,
        "claim",
        handlers=["worker"],
        worker_id="never-worker",
        lease_seconds=0.1,
        attempt_timeout=10,
    )
    clock.advance(0.2)
    assert (
        await fake_call(
            call,
            jobs,
            "claim",
            handlers=["worker"],
            worker_id="recovery",
        )
        is None
    )
    interrupted = await fake_call(call, jobs, "get_owned", job_id=never["id"])
    assert interrupted["status"] == "interrupted"
    assert interrupted["error"]["code"] == "lease_expired"

    safe = await fake_call(
        call,
        jobs,
        "enqueue",
        logical_key="bounded-replay",
        handler="worker",
        payload={},
        retry_policy="safe",
        max_retries=1,
        retry_delay=1,
    )
    first = await fake_call(
        call,
        jobs,
        "claim",
        handlers=["worker"],
        worker_id="safe-1",
        lease_seconds=0.1,
        attempt_timeout=10,
    )
    clock.advance(0.2)
    await fake_call(
        call,
        jobs,
        "claim",
        handlers=["worker"],
        worker_id="safe-recovery",
    )
    assert (await fake_call(call, jobs, "get_owned", job_id=safe["id"]))[
        "status"
    ] == "retry_wait"
    clock.advance(1.1)
    second = await fake_call(
        call,
        jobs,
        "claim",
        handlers=["worker"],
        worker_id="safe-2",
        lease_seconds=0.1,
        attempt_timeout=10,
    )
    assert second["attempts"] == 2
    clock.advance(0.2)
    await fake_call(
        call,
        jobs,
        "claim",
        handlers=["worker"],
        worker_id="safe-final-recovery",
    )
    final = await fake_call(call, jobs, "get_owned", job_id=safe["id"])
    assert final["status"] == "interrupted"
    assert first["attempts"] == 1


@pytest.mark.asyncio
async def test_checkpoint_survives_safe_retry_and_is_visible_to_next_attempt(
    monkeypatch, jobs, call
):
    clock = FakeClock()
    monkeypatch.setattr(jobs_service, "time", SimpleNamespace(time=clock.time))
    queued = await fake_call(
        call,
        jobs,
        "enqueue",
        logical_key="resume",
        handler="worker",
        payload={"steps": 4},
        retry_policy="safe",
        max_retries=1,
        retry_delay=0.5,
    )
    first = await fake_call(
        call,
        jobs,
        "claim",
        handlers=["worker"],
        worker_id="checkpoint-1",
        lease_seconds=0.1,
        attempt_timeout=10,
    )
    await fake_call(
        call,
        jobs,
        "save_checkpoint",
        job_id=queued["id"],
        attempt_token=first["attempt_token"],
        checkpoint={"step": 2},
        progress={"fraction": 0.5},
    )
    clock.advance(0.2)
    await fake_call(
        call,
        jobs,
        "claim",
        handlers=["worker"],
        worker_id="checkpoint-recovery",
    )
    clock.advance(0.6)
    resumed = await fake_call(
        call,
        jobs,
        "claim",
        handlers=["worker"],
        worker_id="checkpoint-2",
        lease_seconds=5,
        attempt_timeout=10,
    )
    assert resumed["checkpoint"] == {"step": 2}
    assert resumed["progress"] == {"fraction": 0.5}


@pytest.mark.asyncio
async def test_cancellation_has_request_and_acknowledgement_states_and_fences_stale_ack(
    monkeypatch, jobs, call
):
    queued = await enqueue(call, jobs, key="cancel-queued")
    cancelled = await fake_call(
        call,
        jobs,
        "request_cancel_owned",
        job_id=queued["id"],
    )
    assert cancelled["status"] == "cancelled"
    assert cancelled["cancel_requested"] is True
    assert (
        await fake_call(
            call,
            jobs,
            "claim",
            handlers=["worker"],
            worker_id="cancel-queued-worker",
        )
        is None
    )

    clock = FakeClock()
    monkeypatch.setattr(jobs_service, "time", SimpleNamespace(time=clock.time))
    running_job = await fake_call(
        call,
        jobs,
        "enqueue",
        logical_key="cancel-running",
        handler="worker",
        payload={},
        retry_policy="safe",
        max_retries=1,
        retry_delay=0.5,
    )
    first = await fake_call(
        call,
        jobs,
        "claim",
        handlers=["worker"],
        worker_id="cancel-1",
        lease_seconds=0.1,
        attempt_timeout=10,
    )
    requested = await fake_call(
        call,
        jobs,
        "request_cancel_owned",
        job_id=running_job["id"],
    )
    assert requested["status"] == "running"
    assert requested["cancel_requested"] is True
    acknowledged = await fake_call(
        call,
        jobs,
        "acknowledge_cancel",
        job_id=running_job["id"],
        attempt_token=first["attempt_token"],
    )
    assert acknowledged["status"] == "cancelled"

    stale_job = await fake_call(
        call,
        jobs,
        "enqueue",
        logical_key="cancel-stale-ack",
        handler="worker",
        payload={},
        retry_policy="safe",
        max_retries=1,
        retry_delay=0.5,
    )
    stale_first = await fake_call(
        call,
        jobs,
        "claim",
        handlers=["worker"],
        worker_id="stale-1",
        lease_seconds=0.1,
        attempt_timeout=10,
    )
    clock.advance(0.2)
    await fake_call(
        call,
        jobs,
        "claim",
        handlers=["worker"],
        worker_id="stale-recovery",
    )
    clock.advance(0.6)
    stale_second = await fake_call(
        call,
        jobs,
        "claim",
        handlers=["worker"],
        worker_id="stale-2",
        lease_seconds=5,
        attempt_timeout=10,
    )
    await fake_call(
        call,
        jobs,
        "request_cancel_owned",
        job_id=stale_job["id"],
    )
    with pytest.raises(PluginError) as stale_ack:
        await fake_call(
            call,
            jobs,
            "acknowledge_cancel",
            job_id=stale_job["id"],
            attempt_token=stale_first["attempt_token"],
        )
    assert_error(stale_ack.value, "jobs_stale_attempt")
    assert (
        await fake_call(
            call,
            jobs,
            "acknowledge_cancel",
            job_id=stale_job["id"],
            attempt_token=stale_second["attempt_token"],
        )
    )["status"] == "cancelled"


@pytest.mark.asyncio
async def test_safe_failures_stop_at_retry_bound(monkeypatch, jobs, call):
    clock = FakeClock()
    monkeypatch.setattr(jobs_service, "time", SimpleNamespace(time=clock.time))
    queued = await fake_call(
        call,
        jobs,
        "enqueue",
        logical_key="bounded-fail",
        handler="worker",
        payload={},
        retry_policy="safe",
        max_retries=2,
        retry_delay=1,
    )
    for attempt in range(1, 4):
        running = await fake_call(
            call,
            jobs,
            "claim",
            handlers=["worker"],
            worker_id=f"fail-{attempt}",
            lease_seconds=5,
            attempt_timeout=10,
        )
        assert running["attempts"] == attempt
        status = await fake_call(
            call,
            jobs,
            "fail",
            job_id=queued["id"],
            attempt_token=running["attempt_token"],
            error={"attempt": attempt},
        )
        assert status["status"] == ("retry_wait" if attempt < 3 else "failed")
        if attempt < 3:
            clock.advance(2 ** (attempt - 1) + 0.1)
    retained = await fake_call(call, jobs, "get_owned", job_id=queued["id"])
    assert retained["status"] == "failed"
    assert retained["error"] == {"attempt": 3}


@pytest.mark.asyncio
async def test_attempt_deadline_rejects_late_mutation_and_then_recovers(
    monkeypatch, jobs, call
):
    clock = FakeClock()
    monkeypatch.setattr(jobs_service, "time", SimpleNamespace(time=clock.time))
    queued = await fake_call(
        call,
        jobs,
        "enqueue",
        logical_key="deadline",
        handler="worker",
        payload={},
    )
    running = await fake_call(
        call,
        jobs,
        "claim",
        handlers=["worker"],
        worker_id="deadline-worker",
        lease_seconds=1,
        attempt_timeout=0.1,
    )
    clock.advance(0.2)
    with pytest.raises(PluginError) as late:
        await fake_call(
            call,
            jobs,
            "complete",
            job_id=queued["id"],
            attempt_token=running["attempt_token"],
            result={"late": True},
        )
    assert_error(late.value, "jobs_stale_attempt")
    state = await fake_call(call, jobs, "get_owned", job_id=queued["id"])
    assert state["status"] == "running"
    clock.advance(1)
    await fake_call(
        call,
        jobs,
        "claim",
        handlers=["worker"],
        worker_id="deadline-recovery",
    )
    assert (await fake_call(call, jobs, "get_owned", job_id=queued["id"]))[
        "status"
    ] == "interrupted"


@pytest.mark.asyncio
async def test_json_bounds_and_invalid_parameters_are_rejected(jobs, call):
    deep_payload: object = "too deep"
    for _ in range(18):
        deep_payload = [deep_payload]
    invalid_payloads = (
        float("nan"),
        {1: "non-string key"},
        [0] * 4097,
        {"deep": deep_payload},
    )
    for index, payload in enumerate(invalid_payloads):
        with pytest.raises(PluginError) as error:
            await enqueue(call, jobs, key=f"invalid-payload-{index}", payload=payload)
        assert_error(error.value, "jobs_validation")

    with pytest.raises(PluginError) as oversized:
        await enqueue(call, jobs, key="oversized", payload={"x": "a" * 70_000})
    assert_error(oversized.value, "jobs_validation")

    checks = (
        {"logical_key": "bad handler", "handler": "Bad"},
        {"logical_key": "bad retry", "retry_policy": "maybe"},
        {"logical_key": "retry without policy", "max_retries": 1},
        {"logical_key": "negative retries", "max_retries": -1},
        {"logical_key": "short delay", "retry_delay": 0},
    )
    for params in checks:
        with pytest.raises(PluginError) as error:
            await enqueue(call, jobs, payload={}, **params)
        assert_error(error.value, "jobs_validation")

    with pytest.raises(PluginError) as bad_claim:
        await fake_call(
            call,
            jobs,
            "claim",
            handlers=[],
            worker_id="worker",
        )
    assert_error(bad_claim.value, "jobs_validation")

    with pytest.raises(PluginError) as bad_limit:
        await fake_call(call, jobs, "list_jobs", limit=0)
    assert_error(bad_limit.value, "jobs_validation")

    with pytest.raises(PluginError) as unknown:
        await fake_call(call, jobs, "missing", value=1)
    assert_error(unknown.value, "method_not_found")
