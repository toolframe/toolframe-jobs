"""Serializable definition for the Durable Counter business example."""

from __future__ import annotations

from importlib.resources import files

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

SERVICE_NAME = "jobs-counter.counter"
_PACKAGE = "toolframe_demo_durable_counter"

_JOB_SCHEMA = {
    "type": "object",
    "description": "A job record returned by the admitted Jobs service.",
    "additionalProperties": True,
}
_JOBS_RESULT_SCHEMA = {
    "type": "object",
    "description": "The owner-scoped result returned by the Jobs service.",
    "additionalProperties": True,
}
_STATE_ROW_SCHEMA = {
    "type": "object",
    "properties": {
        "logical_key": {"type": "string", "minLength": 1, "maxLength": 128},
        "count": {"type": "integer", "minimum": 0},
        "version": {"type": "integer", "minimum": 1},
        "updated_at": {"type": "string"},
    },
    "required": ["logical_key", "count", "version", "updated_at"],
    "additionalProperties": False,
}
_STATE_RESULT_SCHEMA = {
    "type": "object",
    "properties": {
        "states": {"type": "array", "items": _STATE_ROW_SCHEMA},
        "count": {"type": "integer", "minimum": 0},
        "limit": {"type": "integer", "minimum": 1, "maximum": 100},
        "nextAction": {"type": "string"},
    },
    "required": ["states", "count", "limit", "nextAction"],
    "additionalProperties": False,
}


def _list_input(*, default: int, maximum: int) -> dict:
    return {
        "type": "object",
        "properties": {
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": maximum,
                "default": default,
            }
        },
        "additionalProperties": False,
    }


_MIGRATION_SQL = (
    files(_PACKAGE).joinpath("migrations", "0001_init.sql").read_text(encoding="utf-8")
)


plugin = PluginDefinition(
    id="jobs-counter",
    factory="toolframe_demo_durable_counter.service:DurableCounter",
    authorization=AuthorizationSpec(
        scopes={
            "jobs-counter.read": ScopeSpec(title="Read counter jobs and state"),
            "jobs-counter.write": ScopeSpec(title="Submit counter jobs"),
        }
    ),
    requires={
        "jobs": Requirement(service="jobs.queue", version=1),
        "storage": Requirement(service="toolframe.storage.sql", version=1),
    },
    services={
        SERVICE_NAME: ServiceSpec(
            methods={
                "submit": MethodSpec(
                    audience="business",
                    required_scopes=("jobs-counter.write",),
                    description="Submit an owner-scoped resumable counter job.",
                    input_schema={
                        "type": "object",
                        "properties": {
                            "logical_key": {
                                "type": "string",
                                "minLength": 1,
                                "maxLength": 128,
                            },
                            "steps": {
                                "type": "integer",
                                "minimum": 1,
                                "maximum": 100,
                                "default": 10,
                            },
                            "delay_seconds": {
                                "type": "number",
                                "minimum": 0,
                                "maximum": 1,
                                "default": 0.2,
                            },
                        },
                        "required": ["logical_key"],
                        "additionalProperties": False,
                    },
                    output_schema=_JOB_SCHEMA,
                ),
                "list_jobs": MethodSpec(
                    audience="business",
                    required_scopes=("jobs-counter.read",),
                    description="List counter jobs owned by this application.",
                    input_schema=_list_input(default=50, maximum=50),
                    output_schema=_JOBS_RESULT_SCHEMA,
                    read_only=True,
                ),
                "get_job": MethodSpec(
                    audience="business",
                    required_scopes=("jobs-counter.read",),
                    description="Read one counter job owned by this application.",
                    input_schema={
                        "type": "object",
                        "properties": {
                            "job_id": {
                                "type": "string",
                                "minLength": 1,
                                "maxLength": 64,
                            }
                        },
                        "required": ["job_id"],
                        "additionalProperties": False,
                    },
                    output_schema=_JOB_SCHEMA,
                    read_only=True,
                ),
                "list_state": MethodSpec(
                    audience="business",
                    required_scopes=("jobs-counter.read",),
                    description="List retained deterministic counter state.",
                    input_schema=_list_input(default=50, maximum=100),
                    output_schema=_STATE_RESULT_SCHEMA,
                    read_only=True,
                ),
            }
        )
    },
    resources=(
        Resource(
            id="list_jobs",
            kind="mcp.tool",
            title="List counter jobs",
            required_scopes=("jobs-counter.read",),
            data={
                "service": SERVICE_NAME,
                "method": "list_jobs",
                "description": "List counter jobs owned by this application.",
                "input_schema": _list_input(default=50, maximum=50),
                "output_schema": _JOBS_RESULT_SCHEMA,
            },
        ),
        Resource(
            id="list_state",
            kind="mcp.tool",
            title="List counter state",
            required_scopes=("jobs-counter.read",),
            data={
                "service": SERVICE_NAME,
                "method": "list_state",
                "description": "List durable counter values owned by this application.",
                "input_schema": _list_input(default=50, maximum=100),
                "output_schema": _STATE_RESULT_SCHEMA,
            },
        ),
        Resource(
            id="usage",
            kind="llms.document",
            title="Durable Counter",
            data={"package": _PACKAGE, "path": "docs/usage.md"},
        ),
    ),
    store=StoreSchema(
        revision="v1",
        migrations=(
            Migration(
                revision="v1",
                sql=_MIGRATION_SQL,
                dialects={
                    "postgresql": files(_PACKAGE)
                    .joinpath("migrations", "0001_init.postgresql.sql")
                    .read_text(encoding="utf-8")
                },
            ),
        ),
    ),
)


__all__ = ["SERVICE_NAME", "plugin"]
