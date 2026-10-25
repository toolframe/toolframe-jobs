"""Native Jobs dashboard tests using a simulated PageContext service facade."""

from __future__ import annotations

import inspect
from typing import Any

import pytest
from toolframe_sdk import PluginError

streamlit_testing = pytest.importorskip("streamlit.testing.v1")
AppTest = streamlit_testing.AppTest

from toolframe_jobs.page import render

JOB_ID = "a" * 32


def job(
    status: str = "running",
    *,
    cancel_requested: bool = False,
    lease_expires_at: float | None = None,
    **overrides: Any,
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "id": JOB_ID,
        "source": "alpha",
        "application_id": "application-1",
        "logical_key": "counter-1",
        "handler": "count",
        "payload": {"steps": 3},
        "result": {"completed": 3},
        "progress": {"completed": 2, "total": 3},
        "checkpoint": {"completed": 2},
        "attempts": 1,
        "max_retries": 2,
        "retry_policy": "safe",
        "status": status,
        "error": None,
        "cancel_requested": cancel_requested,
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:01Z",
        "lease_expires_at": lease_expires_at,
    }
    result.update(overrides)
    return result


class FakeService:
    def __init__(
        self, rows: list[dict[str, Any]], *, load_error: PluginError | None = None
    ):
        self.rows = {str(row["id"]): dict(row) for row in rows}
        self.load_error = load_error

    def _row(self, job_id: str) -> dict[str, Any]:
        import streamlit as st

        row = dict(self.rows[job_id])
        cancelled = st.session_state.get("jobs-test-cancelled", set())
        if job_id in cancelled:
            row["cancel_requested"] = True
            if row["status"] == "queued":
                row["status"] = "cancelled"
        return row

    def call(self, method: str, params: dict[str, Any] | None = None, *, timeout=30):
        del timeout
        import streamlit as st

        params = dict(params or {})
        if method == "list_jobs":
            if self.load_error is not None:
                raise self.load_error
            st.session_state["jobs-test-list-params"] = params
            rows = [self._row(job_id) for job_id in self.rows]
            return {
                "jobs": rows,
                "count": len(rows),
                "limit": params.get("limit", 50),
            }
        if method == "get_job":
            return self._row(str(params["job_id"]))
        if method == "request_cancel":
            job_id = str(params["job_id"])
            cancelled = st.session_state.setdefault("jobs-test-cancelled", set())
            cancelled.add(job_id)
            st.session_state["jobs-test-cancel-calls"] = (
                st.session_state.get("jobs-test-cancel-calls", 0) + 1
            )
            row = self._row(job_id)
            return {key: row[key] for key in ("id", "status", "cancel_requested")}
        raise AssertionError(f"Unexpected service method: {method}")


class FakeServices:
    def __init__(self, service: FakeService):
        self.service = service

    def require(self, name: str, *, version: int = 1) -> FakeService:
        assert name == "jobs.queue"
        assert version == 1
        return self.service


class FakeContext:
    def __init__(self, service: FakeService, scopes: set[str]):
        self.services = FakeServices(service)
        self.scopes = scopes

    def can(self, scope: str) -> bool:
        return scope in self.scopes


def app_empty() -> None:
    render(FakeContext(FakeService([]), {"jobs.read"}))


def app_load_error() -> None:
    render(
        FakeContext(
            FakeService(
                [], load_error=PluginError("jobs_storage", "Store unavailable.")
            ),
            {"jobs.read"},
        )
    )


def app_read_only() -> None:
    render(FakeContext(FakeService([job()]), {"jobs.read"}))


def app_details() -> None:
    render(
        FakeContext(
            FakeService(
                [
                    job(
                        "succeeded",
                        progress={"completed": 3, "total": 3},
                        checkpoint={"completed": 3},
                    )
                ]
            ),
            {"jobs.read"},
        )
    )


def app_cancel_queued() -> None:
    render(
        FakeContext(
            FakeService([job("queued", attempts=0)]), {"jobs.read", "jobs.cancel"}
        )
    )


def app_running_requested() -> None:
    render(
        FakeContext(FakeService([job("running", cancel_requested=True)]), {"jobs.read"})
    )


def app_cancelled() -> None:
    render(
        FakeContext(
            FakeService([job("cancelled", cancel_requested=True)]), {"jobs.read"}
        )
    )


def app_succeeded_after_cancel() -> None:
    render(
        FakeContext(
            FakeService([job("succeeded", cancel_requested=True)]), {"jobs.read"}
        )
    )


def app_interrupted_after_cancel() -> None:
    render(
        FakeContext(
            FakeService([job("interrupted", cancel_requested=True)]), {"jobs.read"}
        )
    )


def app_expired_lease() -> None:
    render(
        FakeContext(FakeService([job("running", lease_expires_at=0)]), {"jobs.read"})
    )


def run_app(app) -> Any:
    source = "\n".join(
        (
            "from typing import Any",
            "from toolframe_sdk import PluginError",
            "from toolframe_jobs.page import render",
            f"JOB_ID = {JOB_ID!r}",
            inspect.getsource(job),
            inspect.getsource(FakeService),
            inspect.getsource(FakeServices),
            inspect.getsource(FakeContext),
            inspect.getsource(app),
            f"{app.__name__}()",
        )
    )
    return AppTest.from_string(source, default_timeout=60).run()


def messages(ui: Any, collection: str) -> list[str]:
    return [item.value for item in getattr(ui, collection)]


def test_page_shows_helpful_empty_state():
    ui = run_app(app_empty)

    assert not ui.exception
    assert any("No jobs match these filters" in value for value in messages(ui, "info"))
    assert not any(button.label == "Request cancellation" for button in ui.button)


def test_page_shows_load_error_guidance():
    ui = run_app(app_load_error)

    assert not ui.exception
    assert messages(ui, "error") == [
        "Unable to load jobs: Store unavailable. Refresh the page and try again."
    ]


def test_page_read_only_scope_hides_cancellation_control():
    ui = run_app(app_read_only)

    assert not ui.exception
    assert any(button.label == "Refresh jobs" for button in ui.button)
    assert not any(button.label == "Request cancellation" for button in ui.button)
    assert any("jobs.cancel scope" in value for value in messages(ui, "caption"))


def test_page_shows_queue_details_and_json_fields():
    ui = run_app(app_details)

    assert not ui.exception
    assert len(ui.dataframe) == 1
    assert ui.subheader[0].value.startswith("counter-1 · succeeded")
    assert len(ui.json) == 3
    assert any("Payload" in item.value for item in ui.markdown)
    assert any("Checkpoint" in item.value for item in ui.markdown)
    assert any("Result" in item.value for item in ui.markdown)


def test_page_cancellation_requires_scope_and_confirms_queued_request():
    read_only = run_app(app_read_only)
    assert not any(
        button.label == "Request cancellation" for button in read_only.button
    )

    ui = run_app(app_cancel_queued)
    next(
        button for button in ui.button if button.label == "Request cancellation"
    ).click()
    ui = ui.run()

    assert not ui.exception
    assert ui.session_state["jobs-test-cancel-calls"] == 1
    assert any(
        "cancelled before it was claimed" in value for value in messages(ui, "success")
    )


@pytest.mark.parametrize(
    ("app", "collection", "expected"),
    [
        (
            app_running_requested,
            "info",
            "running worker must observe it and acknowledge it",
        ),
        (app_cancelled, "success", "Job cancelled. The worker acknowledged"),
        (
            app_succeeded_after_cancel,
            "warning",
            "completed before cancellation was acknowledged",
        ),
        (app_interrupted_after_cancel, "warning", "external effects may be unknown"),
    ],
)
def test_page_explains_cancellation_outcome(app, collection: str, expected: str):
    ui = run_app(app)

    assert not ui.exception
    assert any(expected in value for value in messages(ui, collection))


def test_page_warns_about_expired_running_lease():
    ui = run_app(app_expired_lease)

    assert not ui.exception
    assert any(
        "lease has expired" in value and "next claim from this source" in value
        for value in messages(ui, "warning")
    )
