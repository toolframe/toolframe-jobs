"""Bounded worker execution for the Toolframe Jobs service.

``JobRunner`` is deliberately a small orchestration helper.  The Jobs plugin
owns persistence, claiming, lease fencing, retries, and cancellation state;
this module only polls that public service and runs registered Python
handlers.  A handler receives a :class:`JobAttempt` and is responsible for
checking cancellation at a safe point before performing an irreversible
effect.

The runner creates one finite runtime root for each claim poll and one finite
runtime root for each attempt.  Heartbeats are child tasks of an attempt root,
so they inherit that root's deadline.  Cleanup roots are also finite and are
admitted only while the runner is running.  During ``stop()`` no new runtime
root is admitted: an attempt that cannot be fenced before shutdown is left for
the Jobs service's lease expiry and explicit retry policy.

Delivery is at least once.  A successful handler and its external side effect
can be replayed after an expired lease, so handlers should use the logical key
and their own idempotency safeguards.
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import math
import re
import time
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any

type JSON = None | bool | int | float | str | list["JSON"] | dict[str, "JSON"]
type JobHandler = Callable[["JobAttempt"], Awaitable[JSON]]


class JobCancelled(Exception):
    """Raised by :meth:`JobAttempt.raise_if_cancelled` at a safe point.

    The runner acknowledges a cancellation only when this exception escapes
    the handler.  A handler that catches it may continue or return a result;
    the Jobs service remains the authority for the resulting state.
    """


class _LeaseLost(Exception):
    """Internal signal that an attempt must stop before another side effect."""


class _UnknownOutcome(Exception):
    """Internal marker preserving an unknown completion or lease outcome."""


def _snapshot(value: Any) -> Any:
    """Copy JSON-shaped values while tolerating test doubles with odd values."""

    try:
        return copy.deepcopy(value)
    except Exception:  # noqa: BLE001 - a read-only snapshot should not fail a job
        return value


def _positive_number(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TypeError(f"{name} must be a finite positive number")
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise ValueError(f"{name} must be a finite positive number")
    return result


def _bounded_number(value: Any, name: str, minimum: float, maximum: float) -> float:
    result = _positive_number(value, name)
    if result < minimum or result > maximum:
        raise ValueError(f"{name} must be between {minimum} and {maximum} seconds")
    return result


def _sanitize_error(
    error: BaseException, *, fallback: str = "job attempt failed"
) -> str:
    """Return a bounded, single-line failure description for the Jobs service."""

    try:
        message = str(error)
    except Exception:  # noqa: BLE001 - hostile exception implementations exist
        message = ""
    message = re.sub(r"\s+", " ", message).strip()
    if not message:
        message = fallback
    if len(message) > 2048:
        message = message[:2045].rstrip() + "..."
    return message


def _cancel_requested(job: Mapping[str, Any]) -> bool:
    """Accept the stable flag and status spellings used by Jobs responses."""

    for key in ("cancel_requested", "cancellation_requested"):
        if job.get(key) is True:
            return True
    return job.get("status") in {
        "cancel_requested",
        "cancellation_requested",
        "cancelling",
        "cancelled",
    }


def _is_timeout(error: BaseException) -> bool:
    if isinstance(error, TimeoutError):
        return True
    code = getattr(error, "code", None)
    if code in {"task_timeout_exceeded", "deadline_exceeded", "timeout"}:
        return True
    return "timeout" in type(error).__name__.lower()


def _is_fence_error(error: BaseException) -> bool:
    code = getattr(error, "code", None)
    if code in {
        "jobs_stale_attempt",
        "deadline_exceeded",
        "task_timeout_exceeded",
    }:
        return True
    message = str(error).lower()
    return "stale attempt" in message or "lease" in message


@dataclass
class _Cleanup:
    method: str
    params: dict[str, Any]


@dataclass
class _AttemptState:
    job: dict[str, Any]
    generation: int
    task: asyncio.Task[Any] | None = None
    cleanup: _Cleanup | None = None
    admitted: bool = False
    disposed: bool = False
    deferred: bool = False


class JobAttempt:
    """A lease-fenced view of one claimed job attempt.

    ``job`` is returned as a defensive snapshot.  The convenience properties
    expose the stable identity and handler input fields without making a
    caller depend on the service's storage representation.
    """

    def __init__(self, runner: JobRunner, job: Mapping[str, Any]):
        self._runner = runner
        self._job: dict[str, Any] = _snapshot(dict(job))
        self._cancel_requested = _cancel_requested(self._job)

    @property
    def job(self) -> dict[str, Any]:
        """Return the latest service snapshot for this attempt."""

        return _snapshot(self._job)

    @property
    def id(self) -> Any:
        return self._job.get("id")

    @property
    def logical_key(self) -> Any:
        return self._job.get("logical_key")

    @property
    def payload(self) -> Any:
        return _snapshot(self._job.get("payload"))

    @property
    def checkpoint(self) -> Any:
        return _snapshot(self._job.get("checkpoint"))

    @property
    def handler(self) -> Any:
        return self._job.get("handler")

    def _record(self, response: Any) -> Any:
        if isinstance(response, Mapping):
            self._job.update(_snapshot(dict(response)))
            self._cancel_requested = self._cancel_requested or _cancel_requested(
                self._job
            )
        return response

    def _mutation_params(self, **extra: Any) -> dict[str, Any]:
        return {
            "job_id": self._job.get("id"),
            "attempt_token": self._job.get("attempt_token"),
            **extra,
        }

    async def heartbeat(self) -> dict[str, Any]:
        """Refresh the lease and return the service's current job snapshot."""

        try:
            response = await self._runner._attempt_call(
                "heartbeat",
                self._mutation_params(lease_seconds=self._runner.lease_seconds),
            )
        except asyncio.CancelledError:
            raise
        except Exception as error:
            raise _LeaseLost(_sanitize_error(error)) from error
        self._record(response)
        return self.job

    async def save_checkpoint(
        self, checkpoint: JSON, progress: JSON | None = None
    ) -> dict[str, Any]:
        """Persist a resumable checkpoint under the current attempt fence."""

        response = await self._runner._attempt_call(
            "save_checkpoint",
            self._mutation_params(
                checkpoint=_snapshot(checkpoint), progress=_snapshot(progress)
            ),
        )
        self._record(response)
        return self.job

    async def cancellation_requested(self) -> bool:
        """Refresh the lease and report whether cooperative cancellation was requested."""

        response = await self.heartbeat()
        requested = self._cancel_requested or _cancel_requested(response)
        self._cancel_requested = requested
        return requested

    async def raise_if_cancelled(self) -> None:
        """Raise :class:`JobCancelled` when cancellation is currently requested."""

        if await self.cancellation_requested():
            raise JobCancelled("job cancellation requested")

    async def acknowledge_cancel(self) -> dict[str, Any]:
        """Acknowledge a cooperative cancellation explicitly.

        Handlers normally let :class:`JobCancelled` escape instead of calling
        this method directly.  It is public for handlers that implement a
        multi-stage cancellation protocol themselves.
        """

        response = await self._runner._attempt_call(
            "acknowledge_cancel", self._mutation_params()
        )
        self._record(response)
        return self.job

    async def _complete(self, result: JSON) -> dict[str, Any]:
        response = await self._runner._attempt_call(
            "complete", self._mutation_params(result=_snapshot(result))
        )
        self._record(response)
        return self.job

    async def _fail(self, error: str, *, outcome: str = "failed") -> dict[str, Any]:
        if outcome not in {"failed", "unknown"}:
            raise ValueError("outcome must be 'failed' or 'unknown'")
        response = await self._runner._attempt_call(
            "fail", self._mutation_params(error=error, outcome=outcome)
        )
        self._record(response)
        return self.job


class JobRunner:
    """Poll and execute Jobs handlers with bounded runtime roots.

    ``start()`` is synchronous because it only arms an event-loop timer.  It
    must be called from a running loop, normally during plugin activation or
    resume.  ``stop()`` cancels poll, attempt, heartbeat, and cleanup tasks
    with a finite wait.  Once stopping begins, no new ``context.task`` roots
    are admitted; a cancellation or timeout that cannot be fenced in the
    remaining roots is left to lease expiry and the Jobs service's retry
    policy.  Calling ``start()`` after ``stop()`` starts a fresh schedule with
    the same per-run worker identity and no duplicate timer.
    """

    def __init__(
        self,
        context: Any,
        handlers: Mapping[str, JobHandler],
        *,
        alias: str = "jobs",
        concurrency: int = 2,
        attempt_timeout: float = 30.0,
        poll_interval: float = 1.0,
        lease_seconds: float = 10.0,
    ):
        if not isinstance(alias, str) or not alias:
            raise ValueError("alias must be a non-empty string")
        if isinstance(concurrency, bool) or not isinstance(concurrency, int):
            raise TypeError("concurrency must be a positive integer")
        if not 1 <= concurrency <= 100:
            raise ValueError("concurrency must be between 1 and 100")
        if not isinstance(handlers, Mapping):
            raise TypeError("handlers must be a mapping of fixed handler names")
        if not 1 <= len(handlers) <= 100:
            raise ValueError("handlers must contain between 1 and 100 names")
        for name, handler in handlers.items():
            if not isinstance(name, str) or not re.fullmatch(
                r"[a-z][a-z0-9_-]{0,63}", name
            ):
                raise ValueError("handler names must be lowercase bounded slugs")
            if not callable(handler):
                raise TypeError(f"handler {name!r} must be callable")

        task_parameters = inspect.signature(context.task).parameters
        if "timeout" not in task_parameters and not any(
            parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in task_parameters.values()
        ):
            raise RuntimeError(
                "Jobs runner requires the finite RuntimeContext.task(..., timeout=...) contract"
            )
        self.context = context
        self.handlers: Mapping[str, JobHandler] = MappingProxyType(dict(handlers))
        self.handler_names = tuple(sorted(self.handlers))
        self.alias = alias
        self.concurrency = concurrency
        self.attempt_timeout = _bounded_number(
            attempt_timeout, "attempt_timeout", 0.1, 86400.0
        )
        self.poll_interval = _bounded_number(
            poll_interval, "poll_interval", 0.000001, 3600.0
        )
        self.lease_seconds = _bounded_number(
            lease_seconds, "lease_seconds", 0.1, 3600.0
        )
        self.worker_id = f"{alias}-{uuid.uuid4().hex}"

        # Poll and cleanup roots get their own finite deadlines.  They are
        # intentionally independent of handler timeouts so a slow claim does
        # not create an unbounded context root.
        self._poll_timeout = max(
            0.1, min(30.0, self.poll_interval + self.lease_seconds)
        )
        self._cleanup_timeout = max(0.1, min(30.0, self.lease_seconds))
        self._heartbeat_interval = max(0.01, min(5.0, self.lease_seconds / 3.0))

        self._started = False
        self._stopping = False
        self._generation = 0
        self._timer: asyncio.TimerHandle | None = None
        self._poll_task: asyncio.Task[Any] | None = None
        self._attempts: dict[asyncio.Task[Any], _AttemptState] = {}
        self._cleanups: set[asyncio.Task[Any]] = set()
        self._deferred_cleanups: list[_Cleanup] = []
        self._stop_lock = asyncio.Lock()

    @property
    def running(self) -> bool:
        """Whether the polling schedule currently accepts new roots."""

        return self._started and not self._stopping

    def start(self) -> None:
        """Arm polling once; repeated calls while running are harmless."""

        if self._started:
            return
        if self._stopping:
            raise RuntimeError("JobRunner cannot start while stop() is in progress")
        asyncio.get_running_loop()
        self._started = True
        self._generation += 1
        generation = self._generation
        self._drain_deferred_cleanups()
        self._arm_timer(0.0, generation)

    def pause(self) -> None:
        """Pause polling and admissions while already claimed attempts drain.

        This is synchronous so a lifecycle quiesce hook does not wait for
        business handlers.  A task that was claimed but is still waiting for
        runtime admission is cancelled and its unknown outcome is deferred
        until a later resume can admit a finite cleanup root.
        """

        if not self._started:
            return
        self._started = False
        self._generation += 1
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

        for task, state in list(self._attempts.items()):
            if not state.admitted and not task.done():
                state.cleanup = self._unknown_failure(
                    state.job,
                    RuntimeError("claim completed while Jobs runner was paused"),
                )
                state.deferred = True
                self._deferred_cleanups.append(state.cleanup)
                task.cancel()

    def quiesce(self) -> None:
        """Lifecycle alias for :meth:`pause`."""

        self.pause()

    def resume(self) -> None:
        """Lifecycle alias for :meth:`start`."""

        self.start()

    async def stop(self) -> None:
        """Stop scheduling and cancel tracked roots with a bounded wait."""

        async with self._stop_lock:
            self._stopping = True
            self._started = False
            self._generation += 1
            if self._timer is not None:
                self._timer.cancel()
                self._timer = None

            tasks: list[asyncio.Task[Any]] = []
            if self._poll_task is not None:
                tasks.append(self._poll_task)
                self._poll_task.cancel()
                self._poll_task = None
            for task, state in list(self._attempts.items()):
                state.disposed = True
                tasks.append(task)
                task.cancel()
            for task in list(self._cleanups):
                tasks.append(task)
                task.cancel()

            # Cancellation itself should be quick, but a broken test double or
            # transport must not make plugin quiesce/dispose wait forever.
            if tasks:
                timeout = max(0.1, min(5.0, self._cleanup_timeout))
                done, _ = await asyncio.wait(tasks, timeout=timeout)
                for task in done:
                    self._consume_exception(task)

            # Keep unresolved tasks tracked after the bounded wait.  A context
            # implementation may delay cancellation; counting those attempts
            # prevents a subsequent resume from exceeding concurrency while
            # their callbacks finish and remove them.
            self._deferred_cleanups.clear()
            self._stopping = False

    async def _attempt_call(self, method: str, params: dict[str, Any]) -> Any:
        return await self.context.call(self.alias, method, params)

    def _arm_timer(self, delay: float, generation: int) -> None:
        if not self._started or self._stopping or generation != self._generation:
            return
        if self._timer is not None:
            return
        loop = asyncio.get_running_loop()
        self._timer = loop.call_later(delay, self._timer_fired, generation)

    def _timer_fired(self, generation: int) -> None:
        self._timer = None
        if not self._started or self._stopping or generation != self._generation:
            return
        if self._poll_task is not None:
            self._arm_timer(self.poll_interval, generation)
            return
        if len(self._attempts) >= self.concurrency:
            self._arm_timer(self.poll_interval, generation)
            return

        try:
            task = self.context.task(
                lambda: self._poll_once(generation),
                name=f"{self.alias}.poll",
                timeout=self._poll_timeout,
            )
        except Exception:  # noqa: BLE001 - a closing context may reject admission
            # A context that is already closing cannot admit a root.  Keep the
            # schedule finite and retry after the next interval if resumed.
            self._arm_timer(self.poll_interval, generation)
            return
        self._poll_task = task
        task.add_done_callback(lambda finished: self._poll_done(finished, generation))

    async def _poll_once(self, generation: int) -> None:
        try:
            async with asyncio.timeout(self._poll_budget()):
                await self._poll_claims(generation)
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            return

    async def _poll_claims(self, generation: int) -> None:
        available = self.concurrency - len(self._attempts)
        if available <= 0:
            return
        params = {
            "handlers": list(self.handler_names),
            "worker_id": self.worker_id,
            "lease_seconds": self.lease_seconds,
            "attempt_timeout": self.attempt_timeout,
        }
        for _ in range(available):
            if not self.running or generation != self._generation:
                return
            try:
                job = await self.context.call(self.alias, "claim", dict(params))
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - service poll failures are transient
                # Poll failures are transient service/transport conditions;
                # keep them out of the SDK's sticky task health diagnostics.
                return
            if job is None:
                return
            if not isinstance(job, Mapping):
                return
            job = dict(job)
            if not self.running or generation != self._generation:
                # Pause may race with the claim RPC.  Keep this call in the
                # already admitted poll root; waiting for a new cleanup root
                # would leave a live lease silently held during quiesce.
                await self._fence_unadmitted_claim(job)
                return
            self._launch_attempt(job, generation)

    def _launch_attempt(self, job: dict[str, Any], generation: int) -> None:
        if not self._started or self._stopping or generation != self._generation:
            return
        state = _AttemptState(job=_snapshot(job), generation=generation)
        try:
            task = self.context.task(
                lambda: self._run_attempt(state),
                name=f"{self.alias}.attempt",
                timeout=self.attempt_timeout,
            )
        except Exception as error:  # noqa: BLE001 - a broken context rejected admission
            state.cleanup = self._unknown_failure(state.job, error)
            self._schedule_cleanup(state.cleanup)
            return
        state.task = task
        self._attempts[task] = state
        task.add_done_callback(lambda finished: self._attempt_done(finished, state))

    async def _run_attempt(self, state: _AttemptState) -> None:
        if not self.running or state.generation != self._generation:
            return
        state.admitted = True
        attempt = JobAttempt(self, state.job)
        heartbeat = asyncio.create_task(
            self._heartbeat_loop(attempt),
            name=f"{self.alias}.heartbeat",
        )
        handler_task: asyncio.Task[Any] | None = None

        async def cancel_children() -> None:
            if handler_task is not None and not handler_task.done():
                handler_task.cancel()
            if handler_task is not None:
                await asyncio.gather(handler_task, return_exceptions=True)
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

        try:
            async with asyncio.timeout(self._attempt_budget(state)):
                handler_task = asyncio.create_task(
                    self._invoke_handler(attempt),
                    name=f"{self.alias}.handler",
                )
                done, _ = await asyncio.wait(
                    (handler_task, heartbeat),
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if heartbeat in done:
                    heartbeat_error = self._consume_exception(heartbeat)
                    if heartbeat_error is None:
                        heartbeat_error = _LeaseLost(
                            "lease heartbeat ended unexpectedly"
                        )
                    raise heartbeat_error
                result = await handler_task
                try:
                    await attempt._complete(result)
                except asyncio.CancelledError:
                    raise
                except Exception as error:  # noqa: BLE001 - completion can be uncertain
                    # The handler may already have caused an external side
                    # effect; failed completion is an unknown outcome.
                    state.cleanup = self._unknown_failure(state.job, error)
                    try:
                        async with asyncio.timeout(self._mutation_budget(state)):
                            await attempt._fail(
                                state.cleanup.params["error"], outcome="unknown"
                            )
                    except asyncio.CancelledError:
                        raise
                    except Exception as mutation_error:
                        raise _UnknownOutcome(
                            state.cleanup.params["error"]
                        ) from mutation_error
                    else:
                        state.cleanup = None
        except _UnknownOutcome:
            return
        except JobCancelled:
            try:
                async with asyncio.timeout(self._mutation_budget(state)):
                    await attempt.acknowledge_cancel()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - cleanup acknowledgement is best effort
                state.cleanup = _Cleanup(
                    "acknowledge_cancel",
                    attempt._mutation_params(),
                )
            else:
                state.cleanup = None
        except _LeaseLost as error:
            # Stop and join the handler before a failure write can make this
            # job eligible for retry. The admitted root still owns the drain;
            # cancellation alone must not acknowledge the job as cancelled.
            await cancel_children()
            state.cleanup = self._unknown_failure(state.job, error)
            try:
                async with asyncio.timeout(self._mutation_budget(state)):
                    await attempt._fail(
                        state.cleanup.params["error"], outcome="unknown"
                    )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - unknown outcome is handled below
                return
            else:
                state.cleanup = None
        except TimeoutError as error:
            await cancel_children()
            state.cleanup = self._unknown_failure(state.job, error)
            try:
                async with asyncio.timeout(self._mutation_budget(state)):
                    await attempt._fail(
                        state.cleanup.params["error"], outcome="unknown"
                    )
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - unknown outcome is handled below
                return
            else:
                state.cleanup = None
        except asyncio.CancelledError:
            # Shutdown cancellation is intentionally not converted into a
            # failed attempt: lease expiry records that outcome honestly.
            raise
        except Exception as error:  # noqa: BLE001 - handler failures are data
            message = _sanitize_error(error)
            outcome = "unknown" if _is_fence_error(error) else "failed"
            state.cleanup = (
                self._unknown_failure(state.job, error)
                if outcome == "unknown"
                else _Cleanup(
                    "fail",
                    attempt._mutation_params(error=message, outcome=outcome),
                )
            )
            try:
                async with asyncio.timeout(self._mutation_budget(state)):
                    await attempt._fail(message, outcome=outcome)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - unknown outcome is handled below
                return
            else:
                state.cleanup = None
        finally:
            await cancel_children()

    def _attempt_budget(self, state: _AttemptState) -> float:
        """Reserve room inside the SDK root to record unknown outcomes."""

        deadlines = [self.attempt_timeout]
        call_info = getattr(self.context, "call_info", {})
        root_deadline = (
            call_info.get("deadline") if isinstance(call_info, Mapping) else None
        )
        for value in (
            state.job.get("attempt_deadline"),
            root_deadline,
        ):
            try:
                deadline = float(value)
            except TypeError, ValueError:
                continue
            if math.isfinite(deadline):
                # Service and Core deadlines are wall-clock timestamps.
                deadlines.append(max(0.001, deadline - time.time()))
        reserve = self._attempt_reserve()
        return max(0.001, min(deadlines) - reserve)

    def _attempt_reserve(self) -> float:
        return max(0.005, min(0.5, self.attempt_timeout * 0.2))

    def _poll_budget(self) -> float:
        reserve = max(0.005, min(0.5, self._poll_timeout * 0.2))
        return max(0.001, self._poll_timeout - reserve)

    def _mutation_budget(self, _state: _AttemptState) -> float:
        return max(0.001, min(self._cleanup_timeout, self._attempt_reserve() * 0.8))

    def _cleanup_budget(self) -> float:
        reserve = max(0.005, min(0.5, self._cleanup_timeout * 0.2))
        return max(0.001, self._cleanup_timeout - reserve)

    async def _invoke_handler(self, attempt: JobAttempt) -> JSON:
        handler = self.handlers.get(str(attempt.handler))
        if handler is None:
            raise RuntimeError(f"unregistered Jobs handler: {attempt.handler!r}")
        result = handler(attempt)
        if inspect.isawaitable(result):
            result = await result
        return result

    async def _heartbeat_loop(self, attempt: JobAttempt) -> None:
        while True:
            await asyncio.sleep(self._heartbeat_interval)
            try:
                await attempt.heartbeat()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                raise _LeaseLost(_sanitize_error(error)) from error

    def _poll_done(self, task: asyncio.Task[Any], generation: int) -> None:
        if self._poll_task is task:
            self._poll_task = None
        self._consume_exception(task)
        if self._started and not self._stopping and generation == self._generation:
            self._arm_timer(self.poll_interval, generation)

    def _attempt_done(self, task: asyncio.Task[Any], state: _AttemptState) -> None:
        self._attempts.pop(task, None)
        if state.disposed or self._stopping:
            self._consume_exception(task)
            return
        error = self._consume_exception(task)
        cleanup = state.cleanup
        if cleanup is None and error is not None and _is_timeout(error):
            cleanup = self._unknown_failure(state.job, error)
        if cleanup is not None:
            state.cleanup = cleanup
            if self.running:
                self._schedule_cleanup(cleanup)
            elif not state.deferred:
                state.deferred = True
                self._deferred_cleanups.append(cleanup)

    def _consume_exception(self, task: asyncio.Task[Any]) -> BaseException | None:
        if task.cancelled():
            return None
        try:
            error = task.exception()
        except asyncio.CancelledError:
            return None
        return error if isinstance(error, BaseException) else None

    def _unknown_failure(
        self, job: Mapping[str, Any], error: BaseException
    ) -> _Cleanup:
        message = _sanitize_error(error, fallback="job attempt outcome is unknown")
        if _is_timeout(error):
            message = "job attempt timed out before its outcome was recorded"
        params = {
            "job_id": job.get("id"),
            "attempt_token": job.get("attempt_token"),
            "error": message,
            "outcome": "unknown",
        }
        return _Cleanup("fail", params)

    def _schedule_cleanup(self, cleanup: _Cleanup | None) -> None:
        if cleanup is None or not self._started or self._stopping:
            return

        async def cleanup_root() -> None:
            try:
                async with asyncio.timeout(self._cleanup_budget()):
                    await self.context.call(self.alias, cleanup.method, cleanup.params)
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001 - cleanup transport has lease fallback
                # Lease expiry is the honest fallback when cleanup transport
                # fails; expected cleanup errors must not poison runtime health.
                return

        try:
            task = self.context.task(
                cleanup_root,
                name=f"{self.alias}.cleanup",
                timeout=self._cleanup_timeout,
            )
        except Exception:  # noqa: BLE001 - cleanup is best effort and bounded
            return
        self._cleanups.add(task)
        task.add_done_callback(self._cleanup_done)

    def _drain_deferred_cleanups(self) -> None:
        if not self.running or not self._deferred_cleanups:
            return
        pending = self._deferred_cleanups
        self._deferred_cleanups = []
        for cleanup in pending:
            self._schedule_cleanup(cleanup)

    async def _fence_unadmitted_claim(self, job: Mapping[str, Any]) -> None:
        cleanup = self._unknown_failure(
            job,
            RuntimeError("claim completed while Jobs runner was paused"),
        )
        try:
            await self.context.call(self.alias, cleanup.method, cleanup.params)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - lease expiry remains the fallback
            return

    def _cleanup_done(self, task: asyncio.Task[Any]) -> None:
        self._cleanups.discard(task)
        self._consume_exception(task)


__all__ = ["JSON", "JobAttempt", "JobCancelled", "JobHandler", "JobRunner"]
