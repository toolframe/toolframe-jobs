"""Install and inspect a Jobs wheel in a clean headless environment."""

import argparse
import json
import subprocess
import tempfile
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("wheel", type=Path)
    parser.add_argument("--find-links", type=Path)
    args = parser.parse_args()
    wheel = args.wheel.resolve()
    with tempfile.TemporaryDirectory(prefix="jobs-wheel-") as temporary:
        environment = Path(temporary) / "venv"
        subprocess.run(["uv", "venv", "--python", "3.14", str(environment)], check=True)
        python = environment / "bin" / "python"
        command = ["uv", "pip", "install", "--python", str(python), str(wheel)]
        if args.find_links:
            command += ["--find-links", str(args.find_links.resolve())]
        subprocess.run(command, check=True)
        check = """
import importlib.metadata as metadata
import importlib.util
from importlib.resources import files
from toolframe_jobs.plugin import plugin
from toolframe_jobs.runner import JobRunner, JobAttempt
assert plugin.id == "jobs"
assert plugin.store.revision == "v1"
assert plugin.requires["storage"].service == "toolframe.storage.sql"
assert importlib.util.find_spec("streamlit") is None
assert files("toolframe_jobs").joinpath("docs/usage.md").is_file()
assert files("toolframe_jobs").joinpath("migrations/0001_init.postgresql.sql").is_file()
entry = next(e for e in metadata.entry_points(group="toolframe.plugins") if e.name == "jobs")
assert entry.load() == plugin
assert metadata.version("toolframe-jobs") == "0.1.0"
assert "toolframe-sdk==0.3.0" in metadata.requires("toolframe-jobs")
assert JobRunner and JobAttempt
print("clean-wheel headless entrypoint/assets/dependencies: passed")
"""
        subprocess.run([str(python), "-I", "-c", check], cwd=temporary, check=True)
    print(json.dumps({"wheel": wheel.name, "status": "passed"}))


if __name__ == "__main__":
    main()
