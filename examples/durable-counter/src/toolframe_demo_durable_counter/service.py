"""Deterministic business service backed by the Toolframe Jobs runner."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from toolframe_jobs.runner import JobAttempt, JobRunner
from toolframe_sdk import Plugin, PluginError

SERVICE_NAME = "jobs-counter.counter"
_MAX_STEPS = 100
_MAX_DELAY_SECONDS = 1.0
_MAX_KEY_LENGTH = 128
_NEXT_ACTION = "Use jobs-counter.submit to enqueue more deterministic work."


def _validation(message: str, *, field: str | None = None) -> PluginError:
    details = {"field": field} if field else {}
    return PluginError("jobs_counter_validation", message, details)


def _validate_logical_key(value: Any) -> str:
    if not isinstance(value, str):
        raise _validation("logical_key must be text.", field="logical_key")
    value = value.strip()
    if not value:
        raise _validation("logical_key cannot be empty.", field="logical_key")
    if len(value) > _MAX_KEY_LENGTH:
        raise _validation(
            f"logical_key must be at most {_MAX_KEY_LENGTH} characters.",
            field="logical_key",
        )
    return value


def _validate_steps(value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _validation("steps must be an integer.", field="steps")
    if not 1 <= value <= _MAX_STEPS:
        raise _validation(f"steps must be between 1 and {_MAX_STEPS}.", field="steps")
    return value


def _validate_delay(value: Any) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _validation(
            "delay_seconds must be a finite number.", field="delay_seconds"
        )
    delay = float(value)
    if not math.isfinite(delay) or not 0 <= delay <= _MAX_DELAY_SECONDS:
        raise _validation(
            f"delay_seconds must be between 0 and {_MAX_DELAY_SECONDS:g}.",
            field="delay_seconds",
        )
    return delay


def _validate_limit(value: Any, *, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise _validation("limit must be an integer.", field="limit")
    if not 1 <= value <= maximum:
        raise _validation(f"limit must be between 1 and {maximum}.", field="limit")
    return value


def _validate_job_id(value: Any) -> str:
    if not isinstance(value, str):
        raise _validation("job_id must be text.", field="job_id")
    value = value.strip()
    if not value:
        raise _validation("job_id cannot be empty.", field="job_id")
    if len(value) > 64:
        raise _validation("job_id must be at most 64 characters.", field="job_id")
    return value


def _timestamp() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _job_field(job: Any, name: str) -> Any:
    """Read a field from the public Job snapshot without accepting client input."""

    if isinstance(job, Mapping):
        return job[name]
    return getattr(job, name)


class DurableCounter(Plugin):
    """Submit and run resumable counter jobs with retained application state."""

    def __init__(self, context):
        super().__init__(context)
        self.runner: JobRunner | None = None

    async def activate(self):
        self.runner = JobRunner(
            self.context,
            {"count": self._count},
            alias="jobs",
            concurrency=1,
            attempt_timeout=30,
            poll_interval=0.2,
            lease_seconds=5,
        )
        self.runner.start()

    async def quiesce(self):
        if self.runner is not None:
            self.runner.pause()
        await super().quiesce()

    async def resume(self):
        await super().resume()
        if self.runner is not None:
            self.runner.start()

    async def dispose(self):
        if self.runner is not None:
            await self.runner.stop()
        await super().dispose()

    async def health(self):
        return {"runtime": "ready", "business": "ready"}

    async def submit(
        self,
        logical_key: str,
        steps: int = 10,
        delay_seconds: float = 0.2,
    ) -> dict[str, Any]:
        logical_key = _validate_logical_key(logical_key)
        steps = _validate_steps(steps)
        delay_seconds = _validate_delay(delay_seconds)
        return await self.context.call(
            "jobs",
            "enqueue",
            {
                "logical_key": logical_key,
                "handler": "count",
                "payload": {
                    "steps": steps,
                    "delay_seconds": delay_seconds,
                },
                "retry_policy": "safe",
                "max_retries": 3,
                "retry_delay": 0.2,
            },
        )

    async def list_jobs(self, limit: int = 50) -> dict[str, Any]:
        limit = _validate_limit(limit, maximum=50)
        return await self.context.call("jobs", "list_owned", {"limit": limit})

    async def get_job(self, job_id: str) -> dict[str, Any]:
        job_id = _validate_job_id(job_id)
        return await self.context.call("jobs", "get_owned", {"job_id": job_id})

    async def list_state(self, limit: int = 50) -> dict[str, Any]:
        limit = _validate_limit(limit, maximum=100)
        rows = await self.context.storage().query(
            "SELECT logical_key, count, version, updated_at "
            "FROM counter_state ORDER BY updated_at DESC, logical_key ASC LIMIT ?",
            (limit,),
        )
        return {
            "states": rows,
            "count": len(rows),
            "limit": limit,
            "nextAction": _NEXT_ACTION,
        }

    async def _read_count(self, logical_key: str) -> int:
        rows = await self.context.storage().query(
            "SELECT count FROM counter_state WHERE logical_key = ?", (logical_key,)
        )
        if not rows:
            return 0
        count = rows[0].get("count")
        if isinstance(count, bool) or not isinstance(count, int) or count < 0:
            raise PluginError(
                "jobs_counter_storage",
                "Counter state contains an invalid count.",
                {"logical_key": logical_key},
            )
        return count

    async def _advance_count(self, logical_key: str, completed: int) -> int:
        """Persist an absolute progress value so replay cannot double increment."""

        storage = self.context.storage()
        updated_at = _timestamp()
        async with storage.transaction() as transaction:
            await transaction.execute(
                "INSERT INTO counter_state "
                "(logical_key, count, version, updated_at) VALUES (?, 0, 1, ?) "
                "ON CONFLICT(logical_key) DO NOTHING",
                (logical_key, updated_at),
            )
            await transaction.execute(
                "UPDATE counter_state SET count = ?, version = version + 1, "
                "updated_at = ? WHERE logical_key = ? AND count < ?",
                (completed, updated_at, logical_key, completed),
            )
            rows = await transaction.query(
                "SELECT count FROM counter_state WHERE logical_key = ?", (logical_key,)
            )
            if not rows:
                raise PluginError(
                    "jobs_counter_storage",
                    "Counter state could not be read after its update.",
                    {"logical_key": logical_key},
                )
            count = rows[0].get("count")
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise PluginError(
                    "jobs_counter_storage",
                    "Counter state contains an invalid count.",
                    {"logical_key": logical_key},
                )
            return count

    async def _count(self, attempt: JobAttempt) -> dict[str, Any]:
        job = attempt.job
        logical_key = _validate_logical_key(_job_field(job, "logical_key"))
        payload = _job_field(job, "payload")
        if not isinstance(payload, Mapping):
            raise PluginError(
                "jobs_counter_payload", "Counter job payload must be an object."
            )
        steps = _validate_steps(payload.get("steps"))
        delay_seconds = _validate_delay(payload.get("delay_seconds"))

        checkpoint = attempt.checkpoint or {}
        if not isinstance(checkpoint, Mapping):
            raise PluginError(
                "jobs_counter_checkpoint", "Counter job checkpoint must be an object."
            )
        checkpoint_completed = checkpoint.get("completed", 0)
        if (
            isinstance(checkpoint_completed, bool)
            or not isinstance(checkpoint_completed, int)
            or not 0 <= checkpoint_completed <= steps
        ):
            raise PluginError(
                "jobs_counter_checkpoint",
                "Counter job checkpoint has an invalid completed value.",
            )

        completed = min(
            steps, max(checkpoint_completed, await self._read_count(logical_key))
        )
        for next_step in range(completed + 1, steps + 1):
            await attempt.raise_if_cancelled()
            await asyncio.sleep(delay_seconds)
            await attempt.raise_if_cancelled()
            completed = min(steps, await self._advance_count(logical_key, next_step))
            await attempt.save_checkpoint(
                {"completed": completed},
                progress={"completed": completed, "total": steps},
            )

        await attempt.raise_if_cancelled()
        return {"completed": steps, "logical_key": logical_key}

    async def invoke(self, service: str, method: str, params: dict) -> Any:
        if service != SERVICE_NAME:
            raise PluginError("method_not_found", f"Unknown method {service}/{method}")
        if method == "submit":
            return await self.submit(**params)
        if method == "list_jobs":
            return await self.list_jobs(**params)
        if method == "get_job":
            return await self.get_job(**params)
        if method == "list_state":
            return await self.list_state(**params)
        raise PluginError("method_not_found", f"Unknown method {service}/{method}")


__all__ = ["SERVICE_NAME", "DurableCounter"]
