#!/usr/bin/env python3
"""Verify the installed Jobs package through Core's CLI and HTTP surfaces.

The verifier builds an isolated Python environment from supplied wheels, starts
one disposable Toolframe application, and drives only its public HTTP APIs.
Core, provider, worker, and temporary application state are kept under one
ignored diagnostics directory.  Process cleanup is scoped to process groups
belonging to this run, including worker groups that Core starts separately.
"""

from __future__ import annotations

import argparse
import http.cookiejar
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_PLUGINS = (
    "sql-sqlite",
    "auth",
    "jobs",
    "jobs-counter",
    "dashboard-auth",
    "streamlit-dashboard",
    "mcp-auth",
    "fastmcp",
    "llms-txt",
)


class VerificationError(RuntimeError):
    """A public acceptance check failed."""


@dataclass(frozen=True)
class Response:
    status: int
    headers: dict[str, str]
    body: bytes

    @property
    def text(self) -> str:
        return self.body.decode("utf-8", errors="replace")

    def json(self) -> Any:
        try:
            return json.loads(self.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise VerificationError(
                f"HTTP {self.status} did not return JSON: {self.text[:500]}"
            ) from error


class HttpSession:
    """Small stdlib HTTP client with an isolated cookie jar."""

    def __init__(self, base: str, *, default_headers: dict[str, str] | None = None):
        self.base = base.rstrip("/")
        self.default_headers = dict(default_headers or {})
        self.cookies = http.cookiejar.CookieJar()
        self.opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self.cookies)
        )

    def request(
        self,
        method: str,
        path: str,
        payload: Any | None = None,
        *,
        headers: dict[str, str] | None = None,
        timeout: float = 20,
    ) -> Response:
        body = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(
            self.base + path,
            data=body,
            method=method,
            headers={
                "Accept": "application/json",
                **self.default_headers,
                **({"Content-Type": "application/json"} if body is not None else {}),
                **(headers or {}),
            },
        )
        try:
            with self.opener.open(request, timeout=timeout) as response:
                return Response(
                    response.status,
                    {key.lower(): value for key, value in response.headers.items()},
                    response.read(),
                )
        except urllib.error.HTTPError as error:
            return Response(
                error.code,
                {key.lower(): value for key, value in error.headers.items()},
                error.read(),
            )

    def get(self, path: str, *, headers=None, timeout=20) -> Response:
        return self.request("GET", path, headers=headers, timeout=timeout)

    def post(self, path: str, payload=None, *, headers=None, timeout=20) -> Response:
        return self.request("POST", path, payload, headers=headers, timeout=timeout)


def reserve_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def run_command(
    command: list[str], *, env=None, cwd=None, timeout=300
) -> subprocess.CompletedProcess:
    result = subprocess.run(
        command,
        env=env,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if result.returncode:
        raise VerificationError(
            f"command failed ({result.returncode}): {' '.join(command)}\n"
            f"{result.stdout[-2000:]}{result.stderr[-4000:]}"
        )
    return result


def wheel_named(directory: Path, prefix: str) -> Path:
    matches = sorted(directory.glob(prefix + "-*.whl"))
    if not matches:
        raise VerificationError(f"Missing required wheel {prefix}-*.whl in {directory}")
    return matches[-1]


def stage_wheels(
    wheelhouse: Path, selected: list[Path], destination: Path
) -> dict[str, Path]:
    destination.mkdir(parents=True, exist_ok=True)
    staged: dict[str, Path] = {}
    for source in sorted(wheelhouse.glob("*.whl")):
        target = destination / source.name
        shutil.copy2(source, target)
        staged[source.name] = target
    for source in selected:
        source = source.resolve()
        target = destination / source.name
        if source != target:
            shutil.copy2(source, target)
        staged[source.name] = target
    return staged


def plugin_block(
    name: str, source: Path, *, config: dict[str, Any] | None = None
) -> str:
    block = (
        f"[plugins.{name}]\n"
        f"source = {json.dumps('wheel:' + str(source))}\n"
        "enabled = true\n"
        "watch = false\n"
    )
    if config:
        block += f"\n[plugins.{name}.config]\n"
        for key, value in config.items():
            block += f"{key} = {json.dumps(value)}\n"
    return block


def write_composition(home: Path, port: int, wheels: dict[str, Path]) -> None:
    home.mkdir(parents=True, exist_ok=True)
    lines = [
        "schema_version = 1",
        "required_capabilities = []",
        "",
        "[application]",
        f"id = {json.dumps(str(uuid.uuid4()))}",
        'name = "Installed Jobs Acceptance"',
        'host = "127.0.0.1"',
        f"port = {port}",
        'public_url = ""',
        "",
        "[runtime]",
        "drain_timeout = 30",
        "activation_timeout = 60",
        "disposal_timeout = 15",
        "health_interval = 2",
        "health_ttl = 10",
        "watch_debounce_ms = 250",
        "",
        "[packages]",
        f"find_links = [{json.dumps(str(wheels['__wheelhouse__']))}]",
        'index_url = "https://pypi.org/simple"',
        "offline = false",
        "",
        "[access]",
        'mode = "public"',
        "local_control = false",
        'auth_provider = "auth"',
        'dashboard_auth = "dashboard-auth"',
        'oidc_provider = ""',
        'failure_mode = "closed"',
        "",
        "[mcp_access]",
        'mode = "protected"',
        'auth_provider = "mcp-auth"',
        "",
        "[bindings]",
        '"toolframe.sql" = "sql-sqlite"',
        '"jobs/sql" = "sql-sqlite"',
        '"jobs-counter/sql" = "sql-sqlite"',
        "",
    ]
    for name in EXPECTED_PLUGINS:
        lines.append(plugin_block(name, wheels[name]))
        lines.append("")
    (home / "toolframe.toml").write_text("\n".join(lines))


def process_inventory() -> list[dict[str, int | str]]:
    result = subprocess.run(
        ["ps", "-axo", "pid=,ppid=,pgid=,command="],
        capture_output=True,
        text=True,
        check=True,
    )
    rows = []
    for line in result.stdout.splitlines():
        fields = line.strip().split(None, 3)
        if len(fields) == 4:
            rows.append(
                {
                    "pid": int(fields[0]),
                    "ppid": int(fields[1]),
                    "pgid": int(fields[2]),
                    "command": fields[3],
                }
            )
    return rows


def owned_groups(pid: int) -> set[int]:
    rows = process_inventory()
    by_parent: dict[int, list[dict[str, int | str]]] = {}
    for row in rows:
        by_parent.setdefault(int(row["ppid"]), []).append(row)
    pending = [pid]
    seen = set()
    descendants = []
    while pending:
        current = pending.pop()
        for row in by_parent.get(current, []):
            child = int(row["pid"])
            if child not in seen:
                seen.add(child)
                pending.append(child)
                descendants.append(row)
    groups = {pid}
    groups.update(int(row["pgid"]) for row in descendants)
    return groups


def kill_groups(groups: set[int], sig: signal.Signals) -> None:
    for group in sorted(groups):
        try:
            os.killpg(group, sig)
        except ProcessLookupError:
            pass
        except PermissionError as error:
            raise VerificationError(
                f"Cannot signal owned process group {group}"
            ) from error


class CoreProcess:
    def __init__(
        self, command: Path, home: Path, log_path: Path, environment: dict[str, str]
    ):
        self.command = command
        self.home = home
        self.log_path = log_path
        self.environment = environment
        self.process: subprocess.Popen | None = None
        self.groups: set[int] = set()

    def start(self, base_url: str) -> HttpSession:
        self.log_path.parent.mkdir(parents=True, exist_ok=True)
        log = self.log_path.open("w")
        try:
            self.process = subprocess.Popen(
                [str(self.command), "--home", str(self.home), "serve"],
                cwd=self.home.parent,
                env=self.environment,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
        finally:
            log.close()
        self.groups = owned_groups(self.process.pid)
        client = HttpSession(base_url)
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise VerificationError(
                    f"Core exited during startup ({self.process.returncode}); see {self.log_path}"
                )
            try:
                health = client.get("/healthz", timeout=5)
            except OSError, urllib.error.URLError:
                health = None
            if health is not None and health.status == 200:
                token_path = self.home / ".toolframe" / "management.token"
                if token_path.exists():
                    token = token_path.read_text().strip()
                    if token:
                        client.default_headers = bearer(token)
                try:
                    status = client.get("/_toolframe/v1/status", timeout=10)
                except OSError, urllib.error.URLError:
                    status = None
                if status is not None and status.status == 200:
                    document = status.json()
                    active = {
                        row["id"]
                        for row in document.get("plugins", [])
                        if row.get("runtime", {}).get("state") == "active"
                    }
                    operations = document.get("operations", [])
                    failed = [
                        row
                        for row in operations
                        if row.get("status") in {"failed", "failed_recovered"}
                    ]
                    if failed:
                        raise VerificationError(
                            f"startup operation failed: {json.dumps(failed[-1])}"
                        )
                    if set(EXPECTED_PLUGINS) <= active:
                        return client
            time.sleep(0.25)
        raise VerificationError(f"Core readiness timed out; see {self.log_path}")

    def stop(self, *, abrupt: bool = False) -> None:
        if self.process is None:
            return
        self.groups.update(owned_groups(self.process.pid))
        if abrupt:
            kill_groups(self.groups, signal.SIGKILL)
        else:
            try:
                self.process.terminate()
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=45)
            except subprocess.TimeoutExpired:
                self.groups.update(owned_groups(self.process.pid))
                kill_groups(self.groups, signal.SIGKILL)
                self.process.wait(timeout=10)
            self.groups.update(owned_groups(self.process.pid))
            # Core normally drains and disposes every worker.  A worker that
            # survives a graceful shutdown is still part of this run's groups.
            if len(self.groups) > 1:
                kill_groups(self.groups - {self.process.pid}, signal.SIGTERM)
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.groups.update(owned_groups(self.process.pid))
            kill_groups(self.groups, signal.SIGKILL)
            self.process.wait(timeout=10)
        self.process = None


def bearer(token: str) -> dict[str, str]:
    return {"Authorization": "Bearer " + token}


def require(response: Response, status: int, label: str) -> Any:
    if response.status != status:
        raise VerificationError(
            f"{label}: expected HTTP {status}, got {response.status}: {response.text[:1000]}"
        )
    return response.json() if response.body else None


def error_code(response: Response) -> str | None:
    try:
        body = response.json()
    except VerificationError:
        return None
    error = body.get("error", {}) if isinstance(body, dict) else {}
    return error.get("code") if isinstance(error, dict) else None


def service_path(owner: str, service: str, method: str) -> str:
    return f"/_toolframe/v1/services/{owner}/{service}/{method}"


def call_service(
    client: HttpSession,
    owner: str,
    service: str,
    method: str,
    payload: dict[str, Any],
    *,
    headers: dict[str, str] | None = None,
    timeout: float = 30,
) -> Response:
    return client.post(
        service_path(owner, service, method), payload, headers=headers, timeout=timeout
    )


def operation(
    client: HttpSession, action: str, params: dict[str, Any], *, timeout=120
) -> dict[str, Any]:
    response = client.post(
        "/_toolframe/v1/operations", {"action": action, "params": params}
    )
    created = require(response, 202, f"submit {action}")
    identifier = created["id"]
    phases: set[str] = set()
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        current = require(
            client.get(f"/_toolframe/v1/operations/{identifier}"),
            200,
            f"poll {action}",
        )
        phases.add(current.get("phase", ""))
        if current.get("status") not in {"queued", "running", "cancelling"}:
            if current.get("status") != "succeeded":
                raise VerificationError(f"{action} failed: {json.dumps(current)}")
            current["observed_phases"] = sorted(phases)
            return current
        time.sleep(0.1)
    raise VerificationError(f"Timed out waiting for {action} operation {identifier}")


def poll_job(
    client: HttpSession,
    job_id: str,
    *,
    status: str | None = None,
    timeout: float = 30,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        response = call_service(
            client,
            "jobs-counter",
            "jobs-counter.counter",
            "get_job",
            {"job_id": job_id},
        )
        last = require(response, 200, "get counter job")
        if status is None or last.get("status") == status:
            return last
        time.sleep(0.2)
    raise VerificationError(f"Timed out waiting for job {job_id}: {json.dumps(last)}")


def parse_rpc(response: Response) -> dict[str, Any]:
    content_type = response.headers.get("content-type", "")
    if "text/event-stream" not in content_type:
        body = response.json()
        if not isinstance(body, dict):
            raise VerificationError(f"MCP response is not an object: {body!r}")
        return body
    for line in response.text.splitlines():
        if line.startswith("data: "):
            body = json.loads(line[6:])
            if isinstance(body, dict):
                return body
    raise VerificationError(
        f"MCP SSE response did not contain a JSON event: {response.text[:500]}"
    )


def mcp_rpc(
    client: HttpSession,
    token: str,
    method: str,
    params: dict[str, Any] | None,
    identifier: int | None,
    session_id: str | None,
) -> tuple[dict[str, Any] | None, str | None]:
    headers = {
        "Authorization": "Bearer " + token,
        "Accept": "application/json, text/event-stream",
        "MCP-Protocol-Version": "2025-11-25",
    }
    if session_id:
        headers["Mcp-Session-Id"] = session_id
    payload = {"jsonrpc": "2.0", "method": method, "params": params or {}}
    if identifier is not None:
        payload["id"] = identifier
    response = client.post("/mcp", payload, headers=headers, timeout=30)
    if response.status not in {200, 202}:
        raise VerificationError(
            f"MCP {method} failed with HTTP {response.status}: {response.text[:1000]}"
        )
    new_session = response.headers.get("mcp-session-id", session_id)
    if not response.body:
        return None, new_session
    return parse_rpc(response), new_session


def verify(args: argparse.Namespace) -> dict[str, Any]:
    wheelhouse = args.wheelhouse.resolve()
    jobs_wheel = args.jobs_wheel.resolve()
    example_wheel = args.example_wheel.resolve()
    for path in (wheelhouse, jobs_wheel, example_wheel):
        if not path.exists():
            raise VerificationError(f"Required path does not exist: {path}")

    diagnostics = Path(
        tempfile.mkdtemp(
            prefix="verify-installed-",
            dir=str(ROOT / ".local") if (ROOT / ".local").exists() else None,
        )
    )
    home = diagnostics / "app"
    venv = diagnostics / "venv"
    staging = diagnostics / "wheelhouse"
    report: dict[str, Any] = {
        "status": "failed",
        "diagnostics": str(diagnostics),
        "checks": {},
        "limitations": [
            "This verifier exercises SQLite and does not establish live PostgreSQL support.",
            "Dashboard HTTP reachability/catalog is checked; pixel rendering remains a separate UI check.",
        ],
    }
    processes: list[CoreProcess] = []
    try:
        selected = [jobs_wheel, example_wheel]
        for prefix in (
            "toolframe_core",
            "toolframe_auth",
            "toolframe_sql_sqlite",
            "toolframe_dashboard_auth",
            "toolframe_streamlit_dashboard",
            "toolframe_fastmcp",
            "toolframe_mcp_auth",
            "toolframe_llms_txt",
            "toolframe_sdk",
        ):
            selected.append(wheel_named(wheelhouse, prefix))
        staged = stage_wheels(wheelhouse, selected, staging)
        staged["__wheelhouse__"] = staging
        plugin_by_prefix = {
            "sql-sqlite": "toolframe_sql_sqlite",
            "auth": "toolframe_auth",
            "jobs": jobs_wheel.name.removesuffix(".whl").split("-")[0],
            "jobs-counter": example_wheel.name.removesuffix(".whl").split("-")[0],
            "dashboard-auth": "toolframe_dashboard_auth",
            "streamlit-dashboard": "toolframe_streamlit_dashboard",
            "mcp-auth": "toolframe_mcp_auth",
            "fastmcp": "toolframe_fastmcp",
            "llms-txt": "toolframe_llms_txt",
        }
        plugin_wheels = {
            name: staged[
                next(
                    filename
                    for filename in staged
                    if filename.startswith(prefix + "-") and filename.endswith(".whl")
                )
            ]
            for name, prefix in plugin_by_prefix.items()
        }
        write_composition(
            home, reserve_port(), {**plugin_wheels, "__wheelhouse__": staging}
        )

        uv = shutil.which("uv")
        if not uv:
            raise VerificationError(
                "uv is required to create the isolated verification venv"
            )
        run_command([uv, "venv", "--python", sys.executable, str(venv)], timeout=120)
        install = [
            uv,
            "pip",
            "install",
            "--python",
            str(venv / "bin/python"),
            "--find-links",
            str(staging),
            str(staging / wheel_named(staging, "toolframe_core").name),
            str(plugin_wheels["sql-sqlite"]),
            str(plugin_wheels["auth"]),
            str(plugin_wheels["jobs"]),
            str(plugin_wheels["jobs-counter"]),
            str(plugin_wheels["dashboard-auth"]),
            str(plugin_wheels["streamlit-dashboard"]),
            str(plugin_wheels["mcp-auth"]),
            str(plugin_wheels["fastmcp"]),
            str(plugin_wheels["llms-txt"]),
        ]
        run_command(install, timeout=600)
        core = venv / "bin" / "toolframe-core"
        if not core.exists():
            raise VerificationError(f"Installed Core CLI not found: {core}")
        environment = {
            key: value
            for key, value in os.environ.items()
            if not key.startswith("TOOLFRAME_")
            and key not in {"PYTHONPATH", "PYTHONHOME"}
        }
        environment.update(
            {
                "PATH": str(venv / "bin") + os.pathsep + os.environ.get("PATH", ""),
                "TOOLFRAME_UV": str(Path(uv).resolve()),
                "TOOLFRAME_RUNTIME_ROOT": str(diagnostics / "runtime"),
            }
        )
        port = int(
            __import__("tomllib").loads((home / "toolframe.toml").read_text())[
                "application"
            ]["port"]
        )
        base = f"http://127.0.0.1:{port}"
        core_process = CoreProcess(
            core, home, diagnostics / "core-first.log", environment
        )
        processes.append(core_process)
        management = core_process.start(base)
        report["checks"]["startup"] = {
            "plugins": EXPECTED_PLUGINS,
            "status": "ready",
        }

        operator_token = (home / ".toolframe" / "management.token").read_text().strip()
        operator_headers = bearer(operator_token)
        composition = require(
            management.get("/_toolframe/v1/composition", headers=operator_headers),
            200,
            "read composition",
        )
        password = "jobs-read-only-2026"
        created_password = management.post(
            "/_toolframe/v1/auth/create",
            {
                "expected_revision": composition["revision"],
                "label": "Jobs read-only acceptance",
                "password": password,
                "scopes": ["jobs.read", "jobs-counter.read"],
                "all_business": False,
                "expires_at": None,
            },
            headers=operator_headers,
        )
        credential = require(created_password, 200, "create read-only password")
        if credential.get("access", {}).get("mode") != "protected":
            raise VerificationError(
                f"password creation did not protect access: {credential}"
            )
        scoped = HttpSession(base)
        login = scoped.post(
            "/_toolframe/auth/login",
            {"password": password},
            headers={"Accept": "application/json"},
        )
        require(login, 200, "read-only password login")
        report["checks"]["scoped_access"] = "protected password session established"

        # A scoped read principal can inspect Jobs and Counter but cannot mutate.
        require(
            call_service(scoped, "jobs", "jobs.queue", "list_jobs", {}),
            200,
            "scoped Jobs list",
        )
        require(
            call_service(
                scoped, "jobs-counter", "jobs-counter.counter", "list_jobs", {}
            ),
            200,
            "scoped Counter list",
        )
        for label, response in (
            (
                "scoped cancel denial",
                call_service(
                    scoped,
                    "jobs",
                    "jobs.queue",
                    "request_cancel",
                    {"job_id": "0"},
                ),
            ),
            (
                "scoped submit denial",
                call_service(
                    scoped,
                    "jobs-counter",
                    "jobs-counter.counter",
                    "submit",
                    {"logical_key": "denied", "steps": 1, "delay_seconds": 0},
                ),
            ),
            (
                "scoped internal enqueue denial",
                call_service(
                    scoped,
                    "jobs",
                    "jobs.queue",
                    "enqueue",
                    {
                        "logical_key": "denied",
                        "handler": "count",
                        "payload": {},
                    },
                ),
            ),
        ):
            require(response, 403, label)
        anonymous = HttpSession(base)
        for path, payload in (
            ("/api/jobs/jobs.queue/request_cancel", {"job_id": "0"}),
            (
                "/api/jobs-counter/jobs-counter.counter/submit",
                {"logical_key": "anonymous", "steps": 1},
            ),
        ):
            require(anonymous.post(path, payload), 403, f"anonymous denial {path}")
        report["checks"]["permission_boundaries"] = (
            "read-only and anonymous mutations denied with HTTP 403"
        )

        logical_key = "acceptance-" + uuid.uuid4().hex
        submitted = require(
            call_service(
                management,
                "jobs-counter",
                "jobs-counter.counter",
                "submit",
                {"logical_key": logical_key, "steps": 5, "delay_seconds": 0.1},
                headers=operator_headers,
            ),
            200,
            "submit deterministic counter",
        )
        job_id = submitted["id"]
        duplicate = require(
            call_service(
                management,
                "jobs-counter",
                "jobs-counter.counter",
                "submit",
                {"logical_key": logical_key, "steps": 5, "delay_seconds": 0.1},
                headers=operator_headers,
            ),
            200,
            "deduplicate deterministic counter",
        )
        if duplicate["id"] != job_id:
            raise VerificationError("same logical key/input did not deduplicate")
        conflict_response = call_service(
            management,
            "jobs-counter",
            "jobs-counter.counter",
            "submit",
            {"logical_key": logical_key, "steps": 6, "delay_seconds": 0.1},
            headers=operator_headers,
        )
        if conflict_response.status not in {400, 409}:
            raise VerificationError(
                f"idempotency conflict returned {conflict_response.status}: {conflict_response.text[:500]}"
            )
        completed = poll_job(management, job_id, status="succeeded", timeout=30)
        if completed.get("result", {}).get("completed") != 5:
            raise VerificationError(
                f"deterministic result was not retained: {completed}"
            )
        scoped_detail = require(
            call_service(
                scoped,
                "jobs-counter",
                "jobs-counter.counter",
                "get_job",
                {"job_id": job_id},
            ),
            200,
            "scoped Counter detail",
        )
        if "attempt_token" in scoped_detail or "lease_owner" in scoped_detail:
            raise VerificationError("public job detail leaked worker fencing fields")
        report["checks"]["submit_idempotency_and_redaction"] = {
            "job_id": job_id,
            "status": completed["status"],
            "same_input_id": duplicate["id"],
            "conflict_status": conflict_response.status,
        }

        # Verify the published read-only MCP catalog and live Jobs calls.
        mcp_key_response = management.post(
            "/_toolframe/v1/mcp-auth/create",
            {
                "label": "Jobs MCP read-only acceptance",
                "scopes": ["jobs.read", "jobs-counter.read"],
                "expires_at": None,
            },
            headers=operator_headers,
        )
        mcp_key = require(mcp_key_response, 200, "create read-only MCP key")["token"]
        mcp_session = None
        initialize, mcp_session = mcp_rpc(
            anonymous,
            mcp_key,
            "initialize",
            {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "toolframe-jobs-verifier", "version": "1"},
            },
            1,
            mcp_session,
        )
        if initialize and "error" in initialize:
            raise VerificationError(f"MCP initialize failed: {initialize}")
        _, mcp_session = mcp_rpc(
            anonymous, mcp_key, "notifications/initialized", {}, None, mcp_session
        )
        listing, mcp_session = mcp_rpc(
            anonymous, mcp_key, "tools/list", {}, 2, mcp_session
        )
        tools = (listing or {}).get("result", {}).get("tools", [])
        tool_names = {tool.get("name") for tool in tools}
        expected_tools = {
            "jobs__list_jobs",
            "jobs__get_job",
            "jobs-counter__list_jobs",
            "jobs-counter__list_state",
        }
        if not expected_tools <= tool_names:
            raise VerificationError(
                f"MCP catalog missing Jobs read tools: {sorted(tool_names)}"
            )
        mcp_list, mcp_session = mcp_rpc(
            anonymous,
            mcp_key,
            "tools/call",
            {"name": "jobs__list_jobs", "arguments": {}},
            3,
            mcp_session,
        )
        mcp_detail, mcp_session = mcp_rpc(
            anonymous,
            mcp_key,
            "tools/call",
            {"name": "jobs__get_job", "arguments": {"job_id": job_id}},
            4,
            mcp_session,
        )
        for label, result in (("MCP list", mcp_list), ("MCP get", mcp_detail)):
            if (
                not result
                or "error" in result
                or result.get("result", {}).get("isError")
            ):
                raise VerificationError(f"{label} failed: {result}")
        report["checks"]["mcp_read_only"] = {
            "tool_names": sorted(tool_names),
            "list_and_get": "passed",
        }

        status = require(
            management.get("/_toolframe/v1/status", headers=operator_headers),
            200,
            "status",
        )
        edges = status.get("dependency_graph", {}).get("edges", [])
        if not any(
            edge.get("source") == "jobs"
            and edge.get("label") == "dashboard.page"
            and "main" in edge.get("resources", [])
            and edge.get("state") == "resolved"
            for edge in edges
        ):
            raise VerificationError(
                "Jobs dashboard page was not in the resolved catalog"
            )
        if (
            status.get("presentations", {}).get("dashboard", {}).get("state")
            != "available"
        ):
            raise VerificationError(
                f"Dashboard publication unavailable: {status.get('presentations')}"
            )
        dashboard = management.get("/", headers=operator_headers, timeout=30)
        if dashboard.status != 200:
            raise VerificationError(
                f"Dashboard entrance: expected HTTP 200, got {dashboard.status}: "
                f"{dashboard.text[:500]}"
            )
        llms_index = management.get("/llms.txt", headers=operator_headers)
        if llms_index.status != 200 or "Jobs" not in llms_index.text:
            raise VerificationError(
                f"Jobs llms index unavailable: {llms_index.status} {llms_index.text[:500]}"
            )
        llms_document = management.get("/llms/jobs/usage.md", headers=operator_headers)
        if llms_document.status != 200 or "Jobs" not in llms_document.text:
            raise VerificationError("Jobs llms usage document unavailable")
        report["checks"]["presentations"] = {
            "dashboard": "available",
            "dashboard_http": dashboard.status,
            "llms": "jobs usage document available",
        }

        # Abruptly kill only process groups observed as descendants of this Core,
        # then let the safe retry policy recover after its five-second lease.
        restart_key = "restart-" + uuid.uuid4().hex
        restart_job = require(
            call_service(
                management,
                "jobs-counter",
                "jobs-counter.counter",
                "submit",
                {"logical_key": restart_key, "steps": 12, "delay_seconds": 0.2},
                headers=operator_headers,
            ),
            200,
            "submit restart job",
        )
        restart_id = restart_job["id"]
        deadline = time.monotonic() + 15
        observed_progress = 0
        while time.monotonic() < deadline:
            current = poll_job(management, restart_id, timeout=5)
            observed_progress = int(
                (current.get("progress") or {}).get("completed", 0) or 0
            )
            if current.get("status") == "running" and observed_progress >= 1:
                break
            time.sleep(0.2)
        else:
            raise VerificationError(
                f"restart job did not reach a checkpoint: {current}"
            )
        core_process.stop(abrupt=True)
        restarted = CoreProcess(
            core, home, diagnostics / "core-restarted.log", environment
        )
        processes[0] = restarted
        management = restarted.start(base)
        resumed = poll_job(management, restart_id, status="succeeded", timeout=30)
        state = require(
            call_service(
                management,
                "jobs-counter",
                "jobs-counter.counter",
                "list_state",
                {},
                headers=operator_headers,
            ),
            200,
            "retained counter state after restart",
        )
        row = next(row for row in state["states"] if row["logical_key"] == restart_key)
        if row["count"] != 12 or resumed["attempts"] < 2:
            raise VerificationError(
                f"safe restart did not retain/replay correctly: job={resumed}, state={row}"
            )
        report["checks"]["abrupt_restart_resume"] = {
            "job_id": restart_id,
            "checkpoint_before_kill": observed_progress,
            "attempts": resumed["attempts"],
            "retained_count": row["count"],
        }

        cancel_key = "cancel-" + uuid.uuid4().hex
        cancel_job = require(
            call_service(
                management,
                "jobs-counter",
                "jobs-counter.counter",
                "submit",
                {"logical_key": cancel_key, "steps": 30, "delay_seconds": 0.2},
                headers=operator_headers,
            ),
            200,
            "submit cooperative cancellation job",
        )
        cancel_id = cancel_job["id"]
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            running = poll_job(management, cancel_id, timeout=5)
            if (
                running.get("status") == "running"
                and (running.get("progress") or {}).get("completed", 0) >= 1
            ):
                break
            time.sleep(0.2)
        else:
            raise VerificationError(f"cancel job did not reach a safe point: {running}")
        cancel_response = call_service(
            management,
            "jobs",
            "jobs.queue",
            "request_cancel",
            {"job_id": cancel_id},
            headers=operator_headers,
        )
        require(cancel_response, 200, "request cooperative cancellation")
        cancelled = poll_job(management, cancel_id, status="cancelled", timeout=20)
        if not cancelled.get("cancel_requested"):
            raise VerificationError(
                f"cancelled job did not retain request flag: {cancelled}"
            )
        report["checks"]["cooperative_cancellation"] = {
            "job_id": cancel_id,
            "status": cancelled["status"],
            "progress": cancelled.get("progress"),
        }

        reload_key = "reload-" + uuid.uuid4().hex
        reload_job = require(
            call_service(
                management,
                "jobs-counter",
                "jobs-counter.counter",
                "submit",
                {"logical_key": reload_key, "steps": 8, "delay_seconds": 0.2},
                headers=operator_headers,
            ),
            200,
            "submit reload drain job",
        )
        reload_id = reload_job["id"]
        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            current = poll_job(management, reload_id, timeout=5)
            if current.get("status") == "running":
                break
            time.sleep(0.2)
        else:
            raise VerificationError(
                f"reload job did not start before quiescence: {current}"
            )
        reload_operation = operation(
            management, "reload", {"id": "jobs-counter"}, timeout=60
        )
        drained = "draining" in reload_operation.get("observed_phases", [])
        reloaded = poll_job(management, reload_id, status="succeeded", timeout=30)
        if not drained:
            raise VerificationError(
                f"reload did not expose a draining phase: {reload_operation}"
            )
        report["checks"]["reload_drain"] = {
            "job_id": reload_id,
            "operation": reload_operation["id"],
            "observed_phases": reload_operation["observed_phases"],
            "status": reloaded["status"],
        }
        report["status"] = "passed"
        return report
    except Exception as error:
        report["error"] = str(error)
        report["log_files"] = sorted(str(path) for path in diagnostics.glob("*.log"))
        raise
    finally:
        for process in reversed(processes):
            try:
                process.stop()
            except (OSError, subprocess.SubprocessError, VerificationError) as error:
                report.setdefault("cleanup_errors", []).append(str(error))
        output = args.output.resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, default=str) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wheelhouse", type=Path, required=True)
    parser.add_argument("--jobs-wheel", type=Path, required=True)
    parser.add_argument("--example-wheel", type=Path, required=True)
    parser.add_argument(
        "--output", type=Path, default=ROOT / ".local" / "verify-installed.json"
    )
    args = parser.parse_args()
    try:
        report = verify(args)
    except Exception as error:  # noqa: BLE001
        print(json.dumps({"status": "failed", "error": str(error)}, indent=2))
        return 1
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
