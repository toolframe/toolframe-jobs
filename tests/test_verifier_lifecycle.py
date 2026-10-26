"""Installed acceptance saves a failed report before retaining its scratch home."""

import argparse
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
spec = importlib.util.spec_from_file_location(
    "jobs_acceptance_verifier", ROOT / "scripts/verify_installed.py"
)
verifier = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = verifier
spec.loader.exec_module(verifier)
helper = sys.modules["verification_workspace"]


class InstalledVerifierLifecycleTests(unittest.TestCase):
    def test_missing_dependency_preserves_report_and_only_latest_failed_environment(
        self,
    ):
        scratch = ROOT / "build/tests"
        scratch.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            root = Path(directory)
            wheels = root / "wheelhouse"
            wheels.mkdir()
            jobs, example = wheels / "jobs.whl", wheels / "example.whl"
            jobs.touch()
            example.touch()
            output = root / ".local/acceptance.json"
            args = argparse.Namespace(
                wheelhouse=wheels, jobs_wheel=jobs, example_wheel=example, output=output
            )
            with (
                patch.object(helper, "ROOT", root),
                patch.object(helper, "idle", return_value=True),
                patch.dict("os.environ", {"TOOLFRAME_KEEP_TEST_WORKSPACE": "0"}),
            ):
                for _ in range(2):
                    with self.assertRaisesRegex(
                        verifier.VerificationError, "Missing required wheel"
                    ):
                        verifier.verify(args)
            report = json.loads(output.read_text())
            self.assertEqual(report["status"], "failed")
            self.assertTrue(Path(report["diagnostics"]).is_dir())
            self.assertEqual(len(list((root / "build/verification").iterdir())), 1)
            self.assertEqual(
                len(list(output.parent.glob("acceptance.json.logs/*/workspace.json"))),
                2,
            )
            self.assertTrue(jobs.is_file() and example.is_file())


if __name__ == "__main__":
    unittest.main()
