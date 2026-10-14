"""Validate publishable artifacts and their installed behavior in fresh environments."""

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tarfile
import tempfile
import tomllib
import zipfile
from email.parser import BytesParser
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PYTHON = "3.14.7"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--wheelhouse", action="append", type=Path, default=[])
    parser.add_argument("--release-ref", default="")
    args = parser.parse_args()
    document = tomllib.loads((ROOT / "pyproject.toml").read_text())
    project = document["project"]
    if args.release_ref and args.release_ref != f"refs/tags/v{project['version']}":
        parser.error(f"Release requires refs/tags/v{project['version']}")
    output = args.output.resolve()
    if output.exists() and any(output.iterdir()):
        parser.error("--output must be absent or empty")
    links = [
        part
        for path in args.wheelhouse
        for part in ("--find-links", str(path.resolve()))
    ]
    env = {
        key: value
        for key, value in os.environ.items()
        if key
        not in {"VIRTUAL_ENV", "PYTHONPATH", "PYTHONHOME", "UV_PROJECT_ENVIRONMENT"}
    }
    env.update(PYTHONDONTWRITEBYTECODE="1", PYTHONNOUSERSITE="1")
    with tempfile.TemporaryDirectory(prefix=f"{project['name']}-release-") as temporary:
        work = Path(temporary)

        def run(*command, cwd=ROOT):
            command = list(map(str, command))
            print("+ " + " ".join(command), flush=True)
            subprocess.run(command, cwd=cwd, env=env, check=True)

        def environment(name):
            path = work / name
            run("uv", "venv", "--python", PYTHON, path)
            return path / "bin/python"

        dist = work / "dist"
        run("uv", "build", "--no-sources", "--python", PYTHON, "--out-dir", dist, ROOT)
        (wheel,) = dist.glob("*.whl")
        (sdist,) = dist.glob("*.tar.gz")
        with zipfile.ZipFile(wheel) as bundle:
            names = bundle.namelist()
            metadata = BytesParser().parsebytes(
                bundle.read(next(n for n in names if n.endswith(".dist-info/METADATA")))
            )
            assert metadata["Name"] == project["name"]
            assert metadata["Version"] == project["version"]
            assert metadata["License-Expression"] == project["license"]
            assert "byfb" in metadata["Author-email"]
            assert len(metadata.get_all("Project-URL", [])) >= 3
            for name in project["license-files"]:
                assert (
                    bundle.read(
                        next(n for n in names if n.endswith("/licenses/" + name))
                    )
                    == (ROOT / name).read_bytes()
                )
        with tarfile.open(sdist) as bundle:
            source_names = bundle.getnames()
        forbidden = {".git", ".local", ".issues", ".worktrees", ".venv", "__pycache__"}
        assert not any(
            forbidden.intersection(Path(name).parts) for name in names + source_names
        )
        run(
            "uv",
            "tool",
            "run",
            "--python",
            PYTHON,
            "--from",
            "twine==7.0.0",
            "twine",
            "check",
            "--strict",
            wheel,
            sdist,
        )

        extras = sorted(project.get("optional-dependencies", {}))
        requirement = str(wheel) + ("[" + ",".join(extras) + "]" if extras else "")
        groups = document.get("dependency-groups", {})
        development = [*groups.get("dev", []), *groups.get("integration", [])]
        assert all(isinstance(item, str) for item in development)
        python = environment("tests")
        run(
            "uv",
            "pip",
            "install",
            "--python",
            python,
            "--no-sources",
            *links,
            requirement,
            *development,
        )
        run("uv", "pip", "check", "--python", python)
        run(python, "-I", "-m", "ruff", "check", "src", "tests", "scripts")
        run(
            python,
            "-I",
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
        )

        smoke = """
import importlib, importlib.metadata as m, json, pathlib, sys
name, version = sys.argv[1:]
assert m.version(name) == version
distribution = m.distribution(name)
direct = json.loads(distribution.read_text('direct_url.json') or '{}')
assert not direct.get('dir_info', {}).get('editable', False)
module = importlib.import_module(name.replace('-', '_'))
location = pathlib.Path(module.__file__).resolve()
assert location.is_relative_to(pathlib.Path(sys.prefix).resolve())
for entry in distribution.entry_points:
    if entry.group == "toolframe.plugins":
        entry.load()
print(json.dumps({'name': name, 'version': version, 'module': str(location),
                  'entry_points': len(distribution.entry_points)}))
"""
        for name, artifact in (("wheel-consumer", wheel), ("sdist-consumer", sdist)):
            python = environment(name)
            run(
                "uv",
                "pip",
                "install",
                "--python",
                python,
                "--no-sources",
                *links,
                artifact,
            )
            run("uv", "pip", "check", "--python", python)
            run(
                python, "-I", "-c", smoke, project["name"], project["version"], cwd=work
            )
        output.mkdir(parents=True, exist_ok=True)
        for artifact in (wheel, sdist):
            shutil.copy2(artifact, output / artifact.name)
        print(
            json.dumps(
                {
                    "package": project["name"],
                    "version": project["version"],
                    "passed": True,
                    "sha256": {
                        p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                        for p in (wheel, sdist)
                    },
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
