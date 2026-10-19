# Releasing toolframe-jobs

The distribution name is `toolframe-jobs`. The version is defined in `pyproject.toml`.
Release tags must exactly match `v<project.version>`. Ordinary CI never uploads
packages. The manual `publish.yml` workflow reruns validation and publishes only
artifacts produced by that successful run.

## Validate

Run from the repository with Python 3.14.7 and uv 0.12.8:

```sh
python3.14 scripts/validate_release.py --output build/release-dist
```

The output must be new or empty. Before dependencies are published, append
`--wheelhouse /path/to/matching-wheels`. The check builds wheel and sdist, checks
metadata and licenses, runs Ruff and package tests against an installed wheel,
and installs each distribution into a separate fresh consumer environment.
Optional database/service integrations require their documented fixtures.

Publish `toolframe-sdk==0.4.0` first. The coordinated order and native CLI channels are documented in
[Toolframe's release guide](https://github.com/toolframe/toolframe-core/blob/main/docs/releasing.md).

## Trusted Publishers

Create GitHub environments named `testpypi` and `pypi`. On each corresponding
package index, configure a normal or pending Trusted Publisher with these values:

| Field | Value |
| --- | --- |
| Project | `toolframe-jobs` |
| GitHub owner | `toolframe` |
| GitHub repository | `toolframe-jobs` |
| Workflow filename | `publish.yml` |
| Environment | `testpypi` or `pypi`, matching the index |

If publishing from another owner, use that actual owner in the publisher binding.
Follow [PyPI's Trusted Publishing guide](https://docs.pypi.org/trusted-publishers/using-a-publisher/).
Only the publishing job receives `id-token: write`; no PyPI API-token secret is
needed. Configure environment reviewers and release-tag protections as appropriate.

Jobs that fetch sibling repositories support the optional read-only
`TOOLFRAME_READ_TOKEN` GitHub secret. It must be able to read those repositories
while they are private. Public siblings require no extra token; secrets are not
available to untrusted fork PRs. `TOOLFRAME_REPOSITORY_OWNER` overrides the trusted
sibling owner where the workflow exposes that variable.

## Publish and verify

After the release commit and matching tag are available remotely, manually run
`publish.yml` at `v0.1.0`, selecting `testpypi` first and then `pypi`.
A branch or mismatched tag is rejected. Downloaded files are the artifacts from
that run; the publishing job does not rebuild them. Published files cannot be
replaced under the same version.

After PyPI publication, verify a clean installation:

```sh
uv venv --python 3.14.7 build/consumer
uv pip install --python build/consumer/bin/python 'toolframe-jobs==0.2.0'
uv pip check --python build/consumer/bin/python
```

TestPyPI does not mirror dependencies. Test the specific artifact against an
explicit dependency source; a local or TestPyPI check alone does not demonstrate
that production PyPI resolution works. Keep logs, hashes and exact revisions in
CI artifacts or ignored local evidence directories.
