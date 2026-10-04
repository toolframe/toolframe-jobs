"""Simulation tests for the bounded public Jobs runner contract.

The fake context models only the SDK's public ``task`` and ``call`` methods.
It gives each root a finite ``asyncio.timeout`` so these tests do not depend on
Core or a storage provider process.
"""

from __future__ import annotations

import asyncio
import copy
import time
from collections import deque
from typing import Any

import pytest

from toolframe_jobs.runner import JobRunner


class FakeContext:
    def __init__(self, jobs: list[dict[str, Any]]):
        self.jobs = deque(copy.deepcopy(jobs))
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        self.task_calls: list[dict[str, Any]] = []
        self.cancel_on_heartbeat = False
        self.completed: list[dict[str, Any]] = []
        self.failed: list[dict[str, Any]] = []
        self.acknowledged: list[dict[str, Any]] = []
        self.claim_started: asyncio.Event | None = None
        self.claim_release: asyncio.Event | None = None
        self.heartbeat_error = False
        self.task_errors: list[BaseException] = []

    def task(self, factory, *, name=None, timeout=None):
        assert isinstance(timeout, (int, float)) and timeout > 0
        self.task_calls.append({"name": name, "timeout": timeout})

        async def bounded():
            try:
                async with asyncio.timeout(timeout):
                    return await factory()
            except Exception as error:
                self.task_errors.append(error)
                raise

        return asyncio.create_task(bounded(), name=name)

    async def call(self, alias, method, params=None):
        params = dict(params or {})
        self.calls.append((alias, method, params))
        if method == "claim":
            if self.claim_started is not None:
                self.claim_started.set()
            if self.claim_release is not None:
                await self.claim_release.wait()
            if not self.jobs:
                return None
            return copy.deepcopy(self.jobs.popleft())
        if method == "heartbeat":
            if self.heartbeat_error:
                raise RuntimeError("heartbeat transport unavailable")
            result = self._job(params)
            if self.cancel_on_heartbeat:
                result["cancel_requested"] = True
            return result
        if method == "save_checkpoint":
            result = self._job(params)
            result.update(
                checkpoint=copy.deepcopy(params["checkpoint"]),
                progress=copy.deepcopy(params.get("progress")),
            )
            return result
        if method == "complete":
            result = self._job(params)
            result.update(status="succeeded", result=copy.deepcopy(params["result"]))
            self.completed.append(params)
            return result
        if method == "fail":
            result = self._job(params)
            result.update(status="failed", error=params["error"])
            self.failed.append(params)
            return result
        if method == "acknowledge_cancel":
            result = self._job(params)
            result.update(status="cancelled")
            self.acknowledged.append(params)
            return result
        raise AssertionError(f"unexpected service method: {method}")

    def _job(self, params):
        job_id = params["job_id"]
        for item in list(self.jobs):
            if item["id"] == job_id:
                return copy.deepcopy(item)
        # The fake service returns the fence fields the runner needs.  The
        # production service obtains these from its owner-fenced row.
        return {
            "id": job_id,
            "logical_key": f"key-{job_id}",
            "handler": "work",
            "payload": {},
            "checkpoint": None,
            "attempt_token": params["attempt_token"],
            "cancel_requested": False,
        }


class LeaseLossOrderingContext(FakeContext):
    """Block failure publication so handler cancellation order is observable."""

    def __init__(self, jobs: list[dict[str, Any]]):
        super().__init__(jobs)
        self.fail_started = asyncio.Event()
        self.fail_release = asyncio.Event()

    async def call(self, alias, method, params=None):
        if method == "fail":
            self.fail_started.set()
            # Give a handler waiting on fail_started one scheduling turn before
            # the blocked failure call is released by the test coordinator.
            await asyncio.sleep(0)
            await self.fail_release.wait()
        return await super().call(alias, method, params)


def job(job_id: str, *, payload=None, checkpoint=None):
    return {
        "id": job_id,
        "logical_key": f"logical-{job_id}",
        "handler": "work",
        "payload": payload or {"id": job_id},
        "checkpoint": checkpoint,
        "attempt_token": f"token-{job_id}",
        "attempts": 1,
        "lease_expires_at": time.time() + 100.0,
        "attempt_deadline": time.time() + 200.0,
        "cancel_requested": False,
    }


async def wait_for(predicate, timeout=1.0):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.001)


@pytest.mark.asyncio
async def test_claim_checkpoint_and_completion_use_fenced_public_calls():
    context = FakeContext([job("one", checkpoint={"step": 0})])
    observed = {}

    async def handler(attempt):
        observed["id"] = attempt.id
        observed["key"] = attempt.logical_key
        observed["payload"] = attempt.payload
        observed["checkpoint"] = attempt.checkpoint
        await attempt.save_checkpoint({"step": 1}, {"fraction": 0.5})
        return {"value": 2}

    runner = JobRunner(
        context,
        {"work": handler},
        concurrency=1,
        poll_interval=0.005,
        lease_seconds=0.1,
        attempt_timeout=0.2,
    )
    runner.start()
    await wait_for(lambda: len(context.completed) == 1)
    await runner.stop()

    assert observed == {
        "id": "one",
        "key": "logical-one",
        "payload": {"id": "one"},
        "checkpoint": {"step": 0},
    }
    claim = next(params for _, method, params in context.calls if method == "claim")
    assert claim["handlers"] == ["work"]
    assert claim["worker_id"] == runner.worker_id
    assert claim["lease_seconds"] == runner.lease_seconds
    assert claim["attempt_timeout"] == runner.attempt_timeout
    saved = next(
        params for _, method, params in context.calls if method == "save_checkpoint"
    )
    assert saved["job_id"] == "one"
    assert saved["attempt_token"] == "token-one"
    assert context.completed[0]["result"] == {"value": 2}


@pytest.mark.asyncio
async def test_concurrency_caps_active_handlers():
    context = FakeContext([job("one"), job("two"), job("three")])
    active = 0
    maximum = 0

    async def handler(attempt):
        nonlocal active, maximum
        active += 1
        maximum = max(maximum, active)
        await asyncio.sleep(0.03)
        active -= 1
        return {"id": attempt.id}

    runner = JobRunner(
        context,
        {"work": handler},
        concurrency=2,
        poll_interval=0.005,
        lease_seconds=0.1,
        attempt_timeout=0.2,
    )
    runner.start()
    await wait_for(lambda: len(context.completed) == 3)
    await runner.stop()

    assert maximum == 2
    assert sum(method == "claim" for _, method, _ in context.calls) >= 2


@pytest.mark.asyncio
async def test_cooperative_cancel_acknowledges_only_explicit_exception():
    context = FakeContext([job("cancelled"), job("completed")])
    context.cancel_on_heartbeat = True

    async def handler(attempt):
        if attempt.id == "cancelled":
            await attempt.raise_if_cancelled()
        await attempt.cancellation_requested()
        return {"ok": True}

    runner = JobRunner(
        context,
        {"work": handler},
        concurrency=1,
        poll_interval=0.005,
        lease_seconds=0.1,
        attempt_timeout=0.2,
    )
    runner.start()
    await wait_for(
        lambda: len(context.completed) == 1 and len(context.acknowledged) == 1
    )
    await runner.stop()

    assert context.acknowledged[0]["job_id"] == "cancelled"
    assert context.completed[0]["job_id"] == "completed"


@pytest.mark.asyncio
async def test_attempt_timeout_uses_unknown_outcome_cleanup():
    context = FakeContext([job("slow")])
    release = asyncio.Event()

    async def handler(_attempt):
        await release.wait()
        return {"never": True}

    runner = JobRunner(
        context,
        {"work": handler},
        concurrency=1,
        poll_interval=0.005,
        lease_seconds=0.1,
        attempt_timeout=0.1,
    )
    runner.start()
    await wait_for(lambda: any(item["job_id"] == "slow" for item in context.failed))
    await runner.stop()

    assert context.failed[0]["outcome"] == "unknown"
    assert "timed out" in context.failed[0]["error"]
    assert not context.task_errors
    assert all(call["timeout"] > 0 for call in context.task_calls)


@pytest.mark.asyncio
async def test_stop_halts_timer_and_start_resumes_without_duplicate_schedule():
    context = FakeContext([])
    runner = JobRunner(
        context, {"work": lambda _attempt: asyncio.sleep(0)}, poll_interval=0.01
    )
    runner.start()
    await wait_for(
        lambda: sum(method == "claim" for _, method, _ in context.calls) >= 1
    )
    await runner.stop()
    count = len(context.calls)
    await asyncio.sleep(0.03)
    assert len(context.calls) == count

    runner.start()
    await wait_for(lambda: len(context.calls) > count)
    await runner.stop()

    claims = [params for _, method, params in context.calls if method == "claim"]
    assert len(claims) >= 2
    assert len({params["worker_id"] for params in claims}) == 1


@pytest.mark.asyncio
async def test_pause_lets_admitted_attempt_drain_and_resume_uses_one_schedule():
    context = FakeContext([job("drain")])
    started = asyncio.Event()
    release = asyncio.Event()

    async def handler(_attempt):
        started.set()
        await release.wait()
        return {"done": True}

    runner = JobRunner(
        context,
        {"work": handler},
        concurrency=1,
        poll_interval=0.005,
        lease_seconds=0.1,
        attempt_timeout=0.2,
    )
    runner.start()
    await asyncio.wait_for(started.wait(), 1)
    runner.pause()
    before = len(context.calls)
    await asyncio.sleep(0.03)
    assert len(context.calls) == before
    assert not context.completed

    release.set()
    await wait_for(lambda: len(context.completed) == 1)
    runner.start()
    runner.start()
    await runner.stop()


@pytest.mark.asyncio
async def test_pause_fences_claim_that_returns_after_pause_without_attempt_root():
    context = FakeContext([job("late")])
    context.claim_started = asyncio.Event()
    context.claim_release = asyncio.Event()
    runner = JobRunner(
        context,
        {"work": lambda _attempt: asyncio.sleep(0)},
        concurrency=1,
        poll_interval=0.005,
        lease_seconds=0.1,
        attempt_timeout=0.2,
    )
    runner.start()
    await asyncio.wait_for(context.claim_started.wait(), 1)
    runner.pause()
    context.claim_release.set()
    await wait_for(
        lambda: any(
            method == "fail" and params.get("job_id") == "late"
            for _, method, params in context.calls
        )
    )
    await runner.stop()

    fail = next(params for _, method, params in context.calls if method == "fail")
    assert fail["outcome"] == "unknown"
    assert not any(item["name"] == "jobs.attempt" for item in context.task_calls)


@pytest.mark.asyncio
async def test_error_text_is_bounded_before_failed_transition():
    context = FakeContext([job("bad")])

    async def handler(_attempt):
        raise RuntimeError("x" * 5000)

    runner = JobRunner(context, {"work": handler}, poll_interval=0.005)
    runner.start()
    await wait_for(lambda: len(context.failed) == 1)
    await runner.stop()

    assert len(context.failed[0]["error"]) <= 2048


@pytest.mark.asyncio
async def test_heartbeat_loss_stops_handler_and_records_unknown_without_root_error():
    context = FakeContext([job("lease-lost")])
    context.heartbeat_error = True
    handler_started = asyncio.Event()

    async def handler(_attempt):
        handler_started.set()
        await asyncio.Event().wait()

    runner = JobRunner(
        context,
        {"work": handler},
        poll_interval=0.005,
        lease_seconds=0.1,
        attempt_timeout=0.3,
    )
    runner.start()
    await asyncio.wait_for(handler_started.wait(), 1)
    await wait_for(lambda: len(context.failed) == 1)
    await runner.stop()

    assert context.failed[0]["outcome"] == "unknown"
    assert not context.task_errors


@pytest.mark.asyncio
async def test_heartbeat_loss_cancels_handler_before_publishing_unknown_failure():
    context = LeaseLossOrderingContext([job("ordered-loss")])
    context.heartbeat_error = True
    handler_started = asyncio.Event()
    cancellation_seen = asyncio.Event()
    post_loss_effects: list[str] = []

    async def handler(_attempt):
        handler_started.set()
        try:
            await context.fail_started.wait()
            post_loss_effects.append("effect after lease loss")
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancellation_seen.set()
            raise

    runner = JobRunner(
        context,
        {"work": handler},
        poll_interval=0.005,
        lease_seconds=0.1,
        attempt_timeout=1.0,
    )
    runner.start()
    try:
        await asyncio.wait_for(handler_started.wait(), 1)
        await asyncio.wait_for(context.fail_started.wait(), 1)
        # The fail RPC intentionally remains blocked.  A handler that is
        # still alive can observe its publication event and perform work.
        await asyncio.sleep(0)
        assert cancellation_seen.is_set()
        assert post_loss_effects == []
        context.fail_release.set()
        await wait_for(lambda: len(context.failed) == 1)
    finally:
        context.fail_release.set()
        await runner.stop()

    assert context.failed[0]["outcome"] == "unknown"
    assert not context.completed
    assert not context.acknowledged


@pytest.mark.asyncio
async def test_heartbeat_loss_waits_for_asynchronous_handler_cleanup_before_failure():
    context = LeaseLossOrderingContext([job("cleanup-loss")])
    context.heartbeat_error = True
    handler_started = asyncio.Event()
    cancellation_seen = asyncio.Event()
    cleanup_release = asyncio.Event()
    cleanup_finished = asyncio.Event()

    async def handler(_attempt):
        handler_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancellation_seen.set()
            await cleanup_release.wait()
            cleanup_finished.set()
            raise

    async def release_cleanup_after_cancellation():
        await cancellation_seen.wait()
        cleanup_release.set()

    cleanup_coordinator = asyncio.create_task(release_cleanup_after_cancellation())
    runner = JobRunner(
        context,
        {"work": handler},
        poll_interval=0.005,
        lease_seconds=0.1,
        attempt_timeout=1.0,
    )
    runner.start()
    try:
        await asyncio.wait_for(handler_started.wait(), 1)
        await asyncio.wait_for(context.fail_started.wait(), 1)
        assert cancellation_seen.is_set()
        assert cleanup_finished.is_set()
        context.fail_release.set()
        await wait_for(lambda: len(context.failed) == 1)
    finally:
        cleanup_release.set()
        context.fail_release.set()
        await runner.stop()
        cleanup_coordinator.cancel()
        await asyncio.gather(cleanup_coordinator, return_exceptions=True)

    assert context.failed[0]["outcome"] == "unknown"
    assert not context.completed
    assert not context.acknowledged


@pytest.mark.asyncio
async def test_heartbeat_loss_handler_return_after_cancellation_stays_unknown():
    context = LeaseLossOrderingContext([job("return-after-loss")])
    context.heartbeat_error = True
    handler_started = asyncio.Event()
    cancellation_seen = asyncio.Event()

    async def handler(_attempt):
        handler_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancellation_seen.set()
            return {"must not complete": True}

    runner = JobRunner(
        context,
        {"work": handler},
        poll_interval=0.005,
        lease_seconds=0.1,
        attempt_timeout=1.0,
    )
    runner.start()
    try:
        await asyncio.wait_for(handler_started.wait(), 1)
        await asyncio.wait_for(context.fail_started.wait(), 1)
        assert cancellation_seen.is_set()
        context.fail_release.set()
        await wait_for(lambda: len(context.failed) == 1)
    finally:
        context.fail_release.set()
        await runner.stop()

    assert context.failed[0]["outcome"] == "unknown"
    assert not context.completed
    assert not context.acknowledged


def test_older_sdk_task_contract_fails_before_scheduling():
    class LegacyContext:
        def task(self, factory, *, name=None):
            raise AssertionError("must not schedule")

    with pytest.raises(RuntimeError, match="finite RuntimeContext.task"):
        JobRunner(LegacyContext(), {"work": lambda attempt: None})
