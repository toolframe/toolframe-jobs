"""Native Streamlit dashboard for the durable Jobs service."""

from __future__ import annotations

import time
from typing import Any

from toolframe_sdk import PageContext, PluginError

from .plugin import SERVICE_NAME, STATUSES

_TERMINAL_STATUSES = {"succeeded", "failed", "cancelled", "interrupted"}


def _show_error(action: str, error: PluginError) -> None:
    """Show a bounded, actionable service error without exposing internals."""

    import streamlit as st

    message = getattr(error, "message", str(error))
    if action == "load":
        st.error(f"Unable to load jobs: {message} Refresh the page and try again.")
    else:
        st.error(
            f"Unable to request cancellation: {message} Refresh the job before retrying."
        )


def _job_from_result(result: Any) -> dict[str, Any] | None:
    if not isinstance(result, dict):
        return None
    candidate = result.get("job", result)
    return candidate if isinstance(candidate, dict) else None


def _job_label(job: dict[str, Any]) -> str:
    job_id = str(job.get("id", "unknown"))
    logical_key = job.get("logical_key") or "(no logical key)"
    status = job.get("status") or "unknown"
    return f"{logical_key} · {status} · {job_id[:12]}"


def _lease_expired(job: dict[str, Any]) -> bool:
    """Recognize an expired running lease from the service's numeric timestamp."""

    if job.get("lease_expired") is True:
        return True
    expires_at = job.get("lease_expires_at")
    return (
        isinstance(expires_at, (int, float))
        and not isinstance(expires_at, bool)
        and expires_at <= time.time()
    )


def _display_rows(jobs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "id": job.get("id"),
            "source": job.get("source"),
            "application_id": job.get("application_id"),
            "logical_key": job.get("logical_key"),
            "handler": job.get("handler"),
            "status": job.get("status"),
            "progress": "Open details"
            if job.get("progress_truncated")
            else job.get("progress"),
            "attempts": job.get("attempts"),
            "max_retries": job.get("max_retries"),
            "cancel_requested": bool(job.get("cancel_requested", False)),
            "updated_at": job.get("updated_at"),
        }
        for job in jobs
    ]


def render(context: PageContext) -> None:
    """Render queue filters, bounded status rows, and one retained job detail."""

    import streamlit as st

    st.title("Jobs")
    service = context.services.require(SERVICE_NAME, version=1)

    filter_column, source_column, limit_column = st.columns((1, 1, 0.6))
    with filter_column:
        status_choice = st.selectbox(
            "Status",
            ["All", *STATUSES],
            key="jobs-filter-status",
        )
    with source_column:
        source = st.text_input(
            "Source",
            key="jobs-filter-source",
            max_chars=64,
            help="Optional owner/source slug, up to 64 characters.",
        )
    with limit_column:
        limit = st.number_input(
            "Rows",
            min_value=1,
            max_value=100,
            value=50,
            step=1,
            key="jobs-filter-limit",
        )

    status = None if status_choice == "All" else status_choice
    params = {
        "limit": int(limit),
        "status": status,
        "source": source.strip() or None,
    }
    if st.button("Refresh jobs", key="jobs-refresh"):
        st.rerun()

    try:
        result = service.call("list_jobs", params)
    except PluginError as error:
        _show_error("load", error)
        return
    if not isinstance(result, dict):
        st.error("Unable to load jobs: the service returned an invalid response.")
        return

    jobs = [job for job in result.get("jobs", []) if isinstance(job, dict)]
    count = result.get("count", len(jobs))
    metric_column, hint_column = st.columns(2)
    metric_column.metric("Matching jobs", count)
    hint_column.caption(
        "Cancellation is a request; a running worker must acknowledge it."
    )

    if not jobs:
        st.info(
            "No jobs match these filters. Enqueue a job from its business plugin or "
            "clear the status/source filters."
        )
        return

    st.dataframe(_display_rows(jobs), hide_index=True, width="stretch")

    selected_id = st.selectbox(
        "Job details",
        [str(job.get("id")) for job in jobs],
        format_func=lambda value: _job_label(
            next((job for job in jobs if str(job.get("id")) == value), {"id": value})
        ),
        key="jobs-selected-id",
    )
    try:
        detail_result = service.call("get_job", {"job_id": selected_id})
    except PluginError as error:
        _show_error("load", error)
        return
    job = _job_from_result(detail_result)
    if job is None:
        st.warning("This job is no longer available. Refresh the list to continue.")
        return

    st.subheader(_job_label(job))
    detail_left, detail_right = st.columns(2)
    detail_left.write(f"**Source:** {job.get('source', 'unknown')}")
    detail_left.write(f"**Application:** {job.get('application_id', 'unknown')}")
    detail_left.write(f"**Handler:** {job.get('handler', 'unknown')}")
    detail_left.write(f"**Status:** {job.get('status', 'unknown')}")
    detail_right.write(f"**Attempts:** {job.get('attempts', 0)}")
    detail_right.write(f"**Max retries:** {job.get('max_retries', 0)}")
    detail_right.write(f"**Progress:** {job.get('progress')!r}")
    detail_right.write(f"**Updated:** {job.get('updated_at', 'unknown')}")

    if job.get("status") == "running" and _lease_expired(job):
        st.warning(
            "This running lease has expired. The next claim from this source will "
            "reconcile it; until then, the job may continue to appear as running."
        )

    if job.get("cancel_requested"):
        status = job.get("status")
        if status == "running":
            st.info(
                "Cancellation requested. The running worker must observe it and "
                "acknowledge it before the job becomes cancelled."
            )
        elif status == "cancelled":
            if job.get("attempts", 0):
                st.success(
                    "Job cancelled. The worker acknowledged the cancellation request."
                )
            else:
                st.success(
                    "Job cancelled before it was claimed; no worker acknowledgement "
                    "was needed."
                )
        elif status == "succeeded":
            st.warning("Job completed before cancellation was acknowledged.")
        elif status == "interrupted":
            st.warning(
                "Cancellation was requested, but the attempt was interrupted before "
                "acknowledgement; external effects may be unknown."
            )
        elif status == "queued":
            st.info("Cancellation was requested for queued work before it was claimed.")
        else:
            st.warning(
                f"Cancellation was requested, but the job ended with status {status!r}."
            )
    elif job.get("status") not in _TERMINAL_STATUSES:
        if context.can("jobs.cancel"):
            if st.button("Request cancellation", key=f"jobs-cancel-{selected_id}"):
                try:
                    service.call("request_cancel", {"job_id": selected_id})
                except PluginError as error:
                    _show_error("cancel", error)
                else:
                    st.success(
                        "Cancellation requested. A running worker must acknowledge it."
                    )
                    st.rerun()
        else:
            st.caption("Requesting cancellation requires the jobs.cancel scope.")

    payload_column, checkpoint_column = st.columns(2)
    with payload_column:
        st.markdown("**Payload**")
        st.json(job.get("payload"))
    with checkpoint_column:
        st.markdown("**Checkpoint**")
        st.json(job.get("checkpoint"))
    st.markdown("**Result**")
    st.json(job.get("result"))
    if job.get("error"):
        st.error(f"Job error: {job['error']}")


__all__ = ["render"]
