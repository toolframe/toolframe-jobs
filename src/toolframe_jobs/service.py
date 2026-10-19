"""Durable queue transitions through Toolframe's managed SQL contract."""

from __future__ import annotations

import hashlib
import inspect
import json
import math
import re
import time
import uuid
from typing import Any

from toolframe_sdk import Plugin, PluginError

SERVICE_NAME = "jobs.queue"
STATUSES = (
    "queued",
    "running",
    "retry_wait",
    "succeeded",
    "failed",
    "cancelled",
    "interrupted",
)
TERMINAL = ("succeeded", "failed", "cancelled", "interrupted")
MAX_JSON_BYTES = 65536
_JSON_COLUMNS = ("payload", "result", "progress", "checkpoint", "error")
_INTERNAL = frozenset(
    (
        "enqueue",
        "claim",
        "get_owned",
        "list_owned",
        "heartbeat",
        "save_checkpoint",
        "complete",
        "fail",
        "acknowledge_cancel",
        "request_cancel_owned",
    )
)
_PUBLIC = frozenset(("list_jobs", "get_job", "request_cancel"))


def invalid(message: str) -> PluginError:
    return PluginError("jobs_validation", message)


def text(
    value: Any, field: str, maximum: int = 128, *, pattern: str | None = None
) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise invalid(f"{field} must be nonempty text of at most {maximum} characters.")
    if pattern and not re.fullmatch(pattern, value):
        raise invalid(f"{field} has an invalid format.")
    return value


def number(value: Any, field: str, minimum: float, maximum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise invalid(
            f"{field} must be a finite number between {minimum} and {maximum}."
        )
    try:
        value = float(value)
    except ValueError, OverflowError:
        raise invalid(f"{field} must be finite.") from None
    if not math.isfinite(value) or not minimum <= value <= maximum:
        raise invalid(
            f"{field} must be a finite number between {minimum} and {maximum}."
        )
    return value


def integer(value: Any, field: str, minimum: int, maximum: int) -> int:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not minimum <= value <= maximum
    ):
        raise invalid(f"{field} must be an integer between {minimum} and {maximum}.")
    return value


def encode(value: Any, field: str) -> str:
    """Bound nesting, item count and wire size before persisting JSON."""
    pending = [(value, 0)]
    count = 0
    while pending:
        item, depth = pending.pop()
        count += 1
        if depth > 16 or count > 4096:
            raise invalid(f"{field} exceeds JSON nesting or item limits.")
        if isinstance(item, dict):
            if any(not isinstance(key, str) for key in item):
                raise invalid(f"{field} must have string JSON object keys.")
            pending.extend((child, depth + 1) for child in item.values())
        elif isinstance(item, list):
            pending.extend((child, depth + 1) for child in item)
        elif item is not None and not isinstance(item, (str, int, float, bool)):
            raise invalid(f"{field} must be JSON data.")
    try:
        result = json.dumps(
            value, sort_keys=True, separators=(",", ":"), allow_nan=False
        )
    except ValueError, TypeError, OverflowError:
        raise invalid(f"{field} must be finite JSON data.") from None
    if len(result.encode("utf-8")) > MAX_JSON_BYTES:
        raise invalid(f"{field} exceeds {MAX_JSON_BYTES} UTF-8 bytes.")
    return result


def decode(row: dict, *, worker: bool = False) -> dict:
    result = dict(row)
    result.pop("input_hash", None)
    for field in _JSON_COLUMNS:
        if field in result:
            result[field] = (
                json.loads(result[field]) if result[field] is not None else None
            )
    if "progress_truncated" in result:
        result["progress_truncated"] = bool(result["progress_truncated"])
    result["cancel_requested"] = bool(result["cancel_requested"])
    if not worker:
        result.pop("attempt_token", None)
        result.pop("lease_owner", None)
    return result


class Jobs(Plugin):
    """An ordinary storage-dependent service; workers pull through dependencies."""

    def identity(self) -> tuple[str, str]:
        info = self.context.call_info
        if not all(
            info.get(field)
            for field in ("application_id", "owner", "owner_generation", "root_id")
        ):
            raise PluginError(
                "jobs_identity_missing",
                "Jobs calls require a Core-issued application and owner identity.",
            )
        if info.get("deadline", 0) <= time.time():
            raise PluginError(
                "deadline_exceeded", "The admitted call deadline has expired."
            )
        return (
            text(info["application_id"], "application identity", 128),
            text(info["owner"], "source owner", 64, pattern=r"[a-z][a-z0-9-]{0,63}"),
        )

    async def _row(self, job_id: str, *, owned: bool, worker: bool = False) -> dict:
        job_id = text(job_id, "job_id", 64)
        application, source = self.identity()
        sql = "SELECT * FROM jobs WHERE id = ? AND application_id = ?"
        params: tuple = (job_id, application)
        if owned:
            sql += " AND source = ?"
            params += (source,)
        rows = await self.context.sql().query(sql, params)
        if not rows:
            raise PluginError(
                "jobs_not_found",
                "Job was not found in the admitted source/application.",
            )
        return decode(rows[0], worker=worker)

    async def enqueue(
        self,
        logical_key: str,
        handler: str,
        payload: Any,
        retry_policy: str = "never",
        max_retries: int = 0,
        retry_delay: float = 1.0,
    ) -> dict:
        application, source = self.identity()
        logical_key = text(logical_key, "logical_key")
        handler = text(handler, "handler", 64, pattern=r"[a-z][a-z0-9_-]{0,63}")
        if retry_policy not in ("never", "safe"):
            raise invalid("retry_policy must be never or safe.")
        max_retries = integer(max_retries, "max_retries", 0, 10)
        if retry_policy == "never" and max_retries:
            raise invalid("max_retries requires the explicit safe retry policy.")
        retry_delay = number(retry_delay, "retry_delay", 0.01, 3600)
        payload_json = encode(payload, "payload")
        fingerprint = hashlib.sha256(
            encode(
                {
                    "handler": handler,
                    "payload": payload,
                    "retry_policy": retry_policy,
                    "max_retries": max_retries,
                    "retry_delay": retry_delay,
                },
                "input",
            ).encode()
        ).hexdigest()
        now = time.time()
        storage = self.context.sql()
        result = await storage.execute_returning(
            "INSERT INTO jobs (id, application_id, source, logical_key, input_hash, handler, payload, max_retries, retry_policy, retry_delay, status, created_at, updated_at, eligible_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'queued', ?, ?, ?) ON CONFLICT (application_id, source, logical_key) DO NOTHING RETURNING *",
            (
                uuid.uuid4().hex,
                application,
                source,
                logical_key,
                fingerprint,
                handler,
                payload_json,
                max_retries,
                retry_policy,
                retry_delay,
                now,
                now,
                now,
            ),
        )
        rows = result.get("rows", [])
        if not rows:
            rows = await storage.query(
                "SELECT * FROM jobs WHERE application_id = ? AND source = ? AND logical_key = ?",
                (application, source, logical_key),
            )
        if not rows:
            raise PluginError("jobs_storage", "Enqueued job could not be read back.")
        if rows[0]["input_hash"] != fingerprint:
            raise PluginError(
                "jobs_idempotency_conflict",
                "This logical key already names a job with different input.",
            )
        return decode(rows[0])

    async def get_owned(self, job_id: str) -> dict:
        return await self._row(job_id, owned=True)

    async def get_job(self, job_id: str) -> dict:
        return await self._row(job_id, owned=False)

    async def _list(self, limit: int, status: str | None, source: str | None) -> dict:
        limit = integer(limit, "limit", 1, 100)
        application, _ = self.identity()
        clauses = ["application_id = ?"]
        params: list = [application]
        if status is not None:
            if status not in STATUSES:
                raise invalid("Unknown job status.")
            clauses.append("status = ?")
            params.append(status)
        if source is not None:
            source = text(source, "source", 64, pattern=r"[a-z][a-z0-9-]{0,63}")
            clauses.append("source = ?")
            params.append(source)
        rows = await self.context.sql().query(
            "SELECT id, application_id, source, logical_key, handler, attempts, max_retries, "
            "retry_policy, retry_delay, status, cancel_requested, lease_expires_at, "
            "attempt_deadline, created_at, updated_at, eligible_at, "
            "CASE WHEN length(progress) <= 1024 THEN progress ELSE NULL END AS progress, "
            "CASE WHEN length(progress) > 1024 THEN 1 ELSE 0 END AS progress_truncated "
            "FROM jobs WHERE "
            + " AND ".join(clauses)
            + " ORDER BY created_at DESC, id DESC LIMIT ?",
            (*params, limit),
        )
        return {
            "jobs": [decode(row) for row in rows],
            "count": len(rows),
            "limit": limit,
        }

    async def list_jobs(
        self, limit: int = 50, status: str | None = None, source: str | None = None
    ) -> dict:
        return await self._list(limit, status, source)

    async def list_owned(self, limit: int = 50, status: str | None = None) -> dict:
        _, source = self.identity()
        return await self._list(limit, status, source)

    async def _recover_one(
        self, application: str, source: str, handlers: list[str], now: float
    ) -> None:
        placeholders = ",".join("?" for _ in handlers)
        rows = await self.context.sql().query(
            f"SELECT id, attempt_token, attempts, max_retries, retry_policy, retry_delay, cancel_requested FROM jobs WHERE application_id = ? AND source = ? AND status = 'running' AND lease_expires_at <= ? AND handler IN ({placeholders}) ORDER BY lease_expires_at, id LIMIT 1",
            (application, source, now, *handlers),
        )
        if not rows:
            return
        row = rows[0]
        retry = (
            row["retry_policy"] == "safe"
            and row["attempts"] <= row["max_retries"]
            and not row["cancel_requested"]
        )
        error = encode(
            {
                "code": "lease_expired",
                "message": "Attempt ended without a fenced acknowledgement; external effects may be unknown.",
            },
            "error",
        )
        delay = min(
            3600, row["retry_delay"] * 2 ** min(10, max(0, row["attempts"] - 1))
        )
        await self.context.sql().execute(
            "UPDATE jobs SET status = CASE WHEN cancel_requested = 1 THEN 'interrupted' ELSE ? END, eligible_at = ?, updated_at = ?, error = ?, attempt_token = NULL, lease_owner = NULL, lease_expires_at = NULL, attempt_deadline = NULL WHERE id = ? AND application_id = ? AND source = ? AND status = 'running' AND attempt_token = ? AND lease_expires_at <= ?",
            (
                "retry_wait" if retry else "interrupted",
                now + delay if retry else now,
                now,
                error,
                row["id"],
                application,
                source,
                row["attempt_token"],
                now,
            ),
        )

    async def claim(
        self,
        handlers: list[str],
        worker_id: str,
        lease_seconds: float = 10.0,
        attempt_timeout: float = 30.0,
    ) -> dict | None:
        application, source = self.identity()
        if not isinstance(handlers, list) or not 1 <= len(handlers) <= 100:
            raise invalid("handlers must contain between 1 and 100 registered names.")
        handlers = list(
            dict.fromkeys(
                text(item, "handler", 64, pattern=r"[a-z][a-z0-9_-]{0,63}")
                for item in handlers
            )
        )
        worker_id = text(worker_id, "worker_id")
        lease_seconds = number(lease_seconds, "lease_seconds", 0.1, 3600)
        attempt_timeout = number(attempt_timeout, "attempt_timeout", 0.1, 86400)
        now = time.time()
        await self._recover_one(application, source, handlers, now)
        placeholders = ",".join("?" for _ in handlers)
        result = await self.context.sql().execute_returning(
            f"UPDATE jobs SET status = 'running', attempts = attempts + 1, lease_owner = ?, attempt_token = ?, lease_expires_at = ?, attempt_deadline = ?, updated_at = ? WHERE id = (SELECT id FROM jobs WHERE application_id = ? AND source = ? AND status IN ('queued', 'retry_wait') AND cancel_requested = 0 AND eligible_at <= ? AND handler IN ({placeholders}) ORDER BY eligible_at, created_at, id LIMIT 1) AND application_id = ? AND source = ? AND status IN ('queued', 'retry_wait') AND cancel_requested = 0 AND eligible_at <= ? RETURNING *",
            (
                worker_id,
                uuid.uuid4().hex,
                now + min(lease_seconds, attempt_timeout),
                now + attempt_timeout,
                now,
                application,
                source,
                now,
                *handlers,
                application,
                source,
                now,
            ),
        )
        rows = result.get("rows", [])
        return decode(rows[0], worker=True) if rows else None

    async def _transition(
        self,
        job_id: str,
        attempt_token: str,
        assignments: str,
        values: tuple,
        *,
        extra: str = "",
    ) -> dict:
        application, source = self.identity()
        job_id = text(job_id, "job_id", 64)
        attempt_token = text(attempt_token, "attempt_token", 64)
        now = time.time()
        result = await self.context.sql().execute_returning(
            "UPDATE jobs SET "
            + assignments
            + ", updated_at = ? WHERE id = ? AND application_id = ? AND source = ? AND status = 'running' AND attempt_token = ? AND lease_expires_at > ? AND attempt_deadline > ?"
            + extra
            + " RETURNING *",
            (*values, now, job_id, application, source, attempt_token, now, now),
        )
        rows = result.get("rows", [])
        if not rows:
            raise PluginError(
                "jobs_stale_attempt",
                "Attempt no longer owns a live lease; no state was changed.",
            )
        return decode(rows[0], worker=True)

    async def heartbeat(
        self, job_id: str, attempt_token: str, lease_seconds: float = 10.0
    ) -> dict:
        lease_seconds = number(lease_seconds, "lease_seconds", 0.1, 3600)
        now = time.time()
        return await self._transition(
            job_id,
            attempt_token,
            "lease_expires_at = CASE WHEN attempt_deadline < ? THEN attempt_deadline ELSE ? END",
            (now + lease_seconds, now + lease_seconds),
        )

    async def save_checkpoint(
        self, job_id: str, attempt_token: str, checkpoint: Any, progress: Any = None
    ) -> dict:
        return await self._transition(
            job_id,
            attempt_token,
            "checkpoint = ?, progress = ?",
            (encode(checkpoint, "checkpoint"), encode(progress, "progress")),
        )

    async def complete(self, job_id: str, attempt_token: str, result: Any) -> dict:
        return await self._transition(
            job_id,
            attempt_token,
            "status = 'succeeded', result = ?, error = NULL, lease_owner = NULL, attempt_token = NULL, lease_expires_at = NULL, attempt_deadline = NULL",
            (encode(result, "result"),),
        )

    async def fail(
        self, job_id: str, attempt_token: str, error: Any, outcome: str = "failed"
    ) -> dict:
        if outcome not in ("failed", "unknown"):
            raise invalid("outcome must be failed or unknown.")
        error_json = encode(error, "error")
        # Read only to choose policy; the update compares this exact token and live lease.
        job = await self._row(job_id, owned=True, worker=True)
        retry = (
            job["retry_policy"] == "safe"
            and job["attempts"] <= job["max_retries"]
            and not job["cancel_requested"]
        )
        status = (
            "retry_wait"
            if retry
            else ("interrupted" if outcome == "unknown" else "failed")
        )
        delay = min(
            3600, job["retry_delay"] * 2 ** min(10, max(0, job["attempts"] - 1))
        )
        # Recheck cancellation in SQL because it can race with the policy read.
        return await self._transition(
            job_id,
            attempt_token,
            "status = CASE WHEN cancel_requested = 1 THEN ? ELSE ? END, eligible_at = ?, error = ?, lease_owner = NULL, attempt_token = NULL, lease_expires_at = NULL, attempt_deadline = NULL",
            (
                "interrupted" if outcome == "unknown" else "failed",
                status,
                time.time() + delay if retry else time.time(),
                error_json,
            ),
        )

    async def acknowledge_cancel(self, job_id: str, attempt_token: str) -> dict:
        return await self._transition(
            job_id,
            attempt_token,
            "status = 'cancelled', lease_owner = NULL, attempt_token = NULL, lease_expires_at = NULL, attempt_deadline = NULL",
            (),
            extra=" AND cancel_requested = 1",
        )

    async def _cancel(self, job_id: str, *, owned: bool) -> dict:
        application, source = self.identity()
        job_id = text(job_id, "job_id", 64)
        clause = " AND source = ?" if owned else ""
        params = (time.time(), job_id, application, *((source,) if owned else ()))
        await self.context.sql().execute(
            "UPDATE jobs SET cancel_requested = 1, status = CASE WHEN status IN ('queued', 'retry_wait') THEN 'cancelled' ELSE status END, updated_at = ? WHERE id = ? AND application_id = ? AND status IN ('queued', 'retry_wait', 'running')"
            + clause,
            params,
        )
        return await self._row(job_id, owned=owned)

    async def request_cancel(self, job_id: str) -> dict:
        return await self._cancel(job_id, owned=False)

    async def request_cancel_owned(self, job_id: str) -> dict:
        return await self._cancel(job_id, owned=True)

    async def health(self) -> dict:
        await self.context.sql().query("SELECT id FROM jobs LIMIT 1")
        return {"runtime": "ready", "business": "ready"}

    async def invoke(self, service: str, method: str, params: dict) -> Any:
        if service != SERVICE_NAME or method not in _INTERNAL | _PUBLIC:
            raise PluginError("method_not_found", f"Unknown method {service}/{method}")
        handler = getattr(self, method)
        try:
            inspect.signature(handler).bind(**params)
        except TypeError:
            raise invalid("Invalid method arguments.") from None
        return await handler(**params)
