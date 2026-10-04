"""Serializable definition for the durable Jobs business plugin.

The definition deliberately contains only public SDK contracts.  The worker
implementation lives in ``service.py`` and is loaded by the host after the
definition has been validated.
"""

from __future__ import annotations

from importlib.resources import files
from typing import Any

from toolframe_sdk import (
    AuthorizationSpec,
    MethodSpec,
    Migration,
    PluginDefinition,
    Requirement,
    Resource,
    ScopeSpec,
    ServiceSpec,
    StoreSchema,
)

SERVICE_NAME = "jobs.queue"
PACKAGE = "toolframe_jobs"

STATUSES = (
    "queued",
    "running",
    "retry_wait",
    "succeeded",
    "failed",
    "cancelled",
    "interrupted",
)
RETRY_POLICIES = ("never", "safe")

_JOB_ID = {
    "type": "string",
    "minLength": 1,
    "maxLength": 64,
    "pattern": r"^[0-9a-fA-F]{1,64}$",
}
_TOKEN = {
    "type": "string",
    "minLength": 1,
    "maxLength": 64,
}
_SOURCE = {
    "type": "string",
    "minLength": 1,
    "maxLength": 64,
    "pattern": r"^[a-z][a-z0-9-]{0,63}$",
}
_LOGICAL_KEY = {"type": "string", "minLength": 1, "maxLength": 128}
_HANDLER = {
    "type": "string",
    "minLength": 1,
    "maxLength": 64,
    "pattern": r"^[a-z][a-z0-9_-]{0,63}$",
}
_JSON = {}
_ERROR = {}
_STATUS = {"type": "string", "enum": list(STATUSES)}
_RETRY_POLICY = {"type": "string", "enum": list(RETRY_POLICIES)}
_LEASE_SECONDS = {"type": "number", "minimum": 0.1, "maximum": 3600}
_ATTEMPT_TIMEOUT = {"type": "number", "minimum": 0.1, "maximum": 86400}


def _object(
    properties: dict[str, Any], *, required: tuple[str, ...] = ()
) -> dict[str, Any]:
    """Return a strict object schema used by every declared method."""

    return {
        "type": "object",
        "properties": properties,
        "required": list(required),
        "additionalProperties": False,
    }


def _with_nullable(schema: dict[str, Any]) -> dict[str, Any]:
    return {"anyOf": [schema, {"type": "null"}]}


_JOB = {"type": "object"}
_CLAIM = _with_nullable(_JOB)
_LIST_RESULT = _object(
    {
        "jobs": {"type": "array", "items": _JOB},
        "count": {"type": "integer", "minimum": 0},
        "limit": {"type": "integer", "minimum": 1, "maximum": 100},
    },
    required=("jobs", "count", "limit"),
)
_LIST_INPUT = _object(
    {
        "limit": {
            "type": "integer",
            "minimum": 1,
            "maximum": 100,
            "default": 50,
        },
        "status": _with_nullable(_STATUS),
        "source": _with_nullable(_SOURCE),
    }
)
_JOB_INPUT = _object({"job_id": _JOB_ID}, required=("job_id",))
_ENQUEUE_INPUT = _object(
    {
        "logical_key": _LOGICAL_KEY,
        "handler": _HANDLER,
        "payload": _JSON,
        "retry_policy": {
            **_RETRY_POLICY,
            "default": "never",
        },
        "max_retries": {
            "type": "integer",
            "minimum": 0,
            "maximum": 10,
            "default": 0,
        },
        "retry_delay": {
            "type": "number",
            "minimum": 0.01,
            "maximum": 3600,
            "default": 1.0,
        },
    },
    required=("logical_key", "handler", "payload"),
)
_CLAIM_INPUT = _object(
    {
        "handlers": {
            "type": "array",
            "items": _HANDLER,
            "minItems": 1,
            "maxItems": 100,
            "uniqueItems": True,
        },
        "worker_id": {"type": "string", "minLength": 1, "maxLength": 128},
        "lease_seconds": {**_LEASE_SECONDS, "default": 10},
        "attempt_timeout": {**_ATTEMPT_TIMEOUT, "default": 30},
    },
    required=("handlers", "worker_id"),
)
_LIST_OWNED_INPUT = _object(
    {
        "limit": {
            "type": "integer",
            "minimum": 1,
            "maximum": 100,
            "default": 50,
        },
        "status": _with_nullable(_STATUS),
    }
)
_LEASE_INPUT = _object(
    {
        "job_id": _JOB_ID,
        "attempt_token": _TOKEN,
        "lease_seconds": {**_LEASE_SECONDS, "default": 10},
    },
    required=("job_id", "attempt_token"),
)
_CHECKPOINT_INPUT = _object(
    {
        "job_id": _JOB_ID,
        "attempt_token": _TOKEN,
        "checkpoint": _JSON,
        "progress": _JSON,
    },
    required=("job_id", "attempt_token", "checkpoint"),
)
_ATTEMPT_INPUT = _object(
    {"job_id": _JOB_ID, "attempt_token": _TOKEN},
    required=("job_id", "attempt_token"),
)
_RESULT_INPUT = _object(
    {
        "job_id": _JOB_ID,
        "attempt_token": _TOKEN,
        "result": _JSON,
    },
    required=("job_id", "attempt_token", "result"),
)
_FAIL_INPUT = _object(
    {
        "job_id": _JOB_ID,
        "attempt_token": _TOKEN,
        "error": _ERROR,
        "outcome": {"type": "string", "enum": ["failed", "unknown"]},
    },
    required=("job_id", "attempt_token", "error"),
)


_MIGRATION_SQL = (
    files(PACKAGE).joinpath("migrations", "0001_init.sql").read_text(encoding="utf-8")
)
_MIGRATION_POSTGRESQL_SQL = (
    files(PACKAGE)
    .joinpath("migrations", "0001_init.postgresql.sql")
    .read_text(encoding="utf-8")
)


plugin = PluginDefinition(
    id="jobs",
    factory="toolframe_jobs.service:Jobs",
    authorization=AuthorizationSpec(
        scopes={
            "jobs.read": ScopeSpec(title="Read jobs"),
            "jobs.cancel": ScopeSpec(title="Request job cancellation"),
        }
    ),
    requires={"storage": Requirement(service="toolframe.storage.sql", version=1)},
    services={
        SERVICE_NAME: ServiceSpec(
            version=1,
            methods={
                "list_jobs": MethodSpec(
                    audience="business",
                    required_scopes=("jobs.read",),
                    read_only=True,
                    description="List the newest jobs visible to the caller.",
                    input_schema=_LIST_INPUT,
                    output_schema=_LIST_RESULT,
                ),
                "get_job": MethodSpec(
                    audience="business",
                    required_scopes=("jobs.read",),
                    read_only=True,
                    description="Read one retained job by ID.",
                    input_schema=_JOB_INPUT,
                    output_schema=_JOB,
                ),
                "request_cancel": MethodSpec(
                    audience="business",
                    required_scopes=("jobs.cancel",),
                    description="Request cooperative cancellation of a job.",
                    input_schema=_JOB_INPUT,
                    output_schema=_JOB,
                ),
                "enqueue": MethodSpec(
                    audience="internal",
                    description="Enqueue a job owned by the admitted producer.",
                    input_schema=_ENQUEUE_INPUT,
                    output_schema=_JOB,
                ),
                "claim": MethodSpec(
                    audience="internal",
                    description="Atomically claim one eligible job for a worker.",
                    input_schema=_CLAIM_INPUT,
                    output_schema=_CLAIM,
                ),
                "get_owned": MethodSpec(
                    audience="internal",
                    read_only=True,
                    description="Read one job owned by the admitted producer.",
                    input_schema=_JOB_INPUT,
                    output_schema=_JOB,
                ),
                "list_owned": MethodSpec(
                    audience="internal",
                    read_only=True,
                    description="List jobs owned by the admitted producer.",
                    input_schema=_LIST_OWNED_INPUT,
                    output_schema=_LIST_RESULT,
                ),
                "heartbeat": MethodSpec(
                    audience="internal",
                    description="Renew a current attempt lease.",
                    input_schema=_LEASE_INPUT,
                    output_schema=_JOB,
                ),
                "save_checkpoint": MethodSpec(
                    audience="internal",
                    description="Persist a checkpoint under the current attempt fence.",
                    input_schema=_CHECKPOINT_INPUT,
                    output_schema=_JOB,
                ),
                "complete": MethodSpec(
                    audience="internal",
                    description="Complete a current attempt with a result.",
                    input_schema=_RESULT_INPUT,
                    output_schema=_JOB,
                ),
                "fail": MethodSpec(
                    audience="internal",
                    description="Record a failed or interrupted current attempt.",
                    input_schema=_FAIL_INPUT,
                    output_schema=_JOB,
                ),
                "acknowledge_cancel": MethodSpec(
                    audience="internal",
                    description="Acknowledge cancellation from a cooperative worker.",
                    input_schema=_ATTEMPT_INPUT,
                    output_schema=_JOB,
                ),
                "request_cancel_owned": MethodSpec(
                    audience="internal",
                    description="Request cancellation for an owned job.",
                    input_schema=_JOB_INPUT,
                    output_schema=_JOB,
                ),
            },
        )
    },
    resources=(
        Resource(
            id="main",
            kind="dashboard.page",
            title="Jobs",
            required_scopes=("jobs.read",),
            data={"entrypoint": "toolframe_jobs.page:render", "order": 20},
        ),
        Resource(
            id="list_jobs",
            kind="mcp.tool",
            title="List jobs",
            data={
                "service": SERVICE_NAME,
                "method": "list_jobs",
                "description": "List retained jobs with bounded status and source filters.",
                "input_schema": _LIST_INPUT,
                "output_schema": _LIST_RESULT,
            },
        ),
        Resource(
            id="get_job",
            kind="mcp.tool",
            title="Get job",
            data={
                "service": SERVICE_NAME,
                "method": "get_job",
                "description": "Read one retained job by ID.",
                "input_schema": _JOB_INPUT,
                "output_schema": _JOB,
            },
        ),
        Resource(
            id="usage",
            kind="llms.document",
            title="Jobs usage",
            data={"package": PACKAGE, "path": "docs/usage.md"},
        ),
    ),
    store=StoreSchema(
        revision="v1",
        migrations=(
            Migration(
                revision="v1",
                sql=_MIGRATION_SQL,
                dialects={"postgresql": _MIGRATION_POSTGRESQL_SQL},
            ),
        ),
    ),
)


__all__ = ["SERVICE_NAME", "plugin"]
