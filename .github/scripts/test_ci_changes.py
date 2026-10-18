import os
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from ci_changes import JOBS, main, select_jobs


class ChangeSelectionTest(unittest.TestCase):
    def test_components_and_shared_inputs(self):
        cases = [
            (["frontend/src/main.tsx"], {"frontend", "packaging"}),
            (["frontend/package-lock.json"], {"frontend", "packaging"}),
            (["backend/go.sum"], {"backend", "integration", "packaging"}),
            (["Dockerfile"], {"packaging"}),
            (["deploy/helm/kaflux/Chart.yaml"], {"packaging"}),
            (["docker-compose.yaml"], {"integration", "packaging"}),
            (["deploy/docker-compose.integration.yaml"], {"integration", "packaging"}),
            (["docs/openapi.yaml"], {"packaging"}),
            (["README.md", "docs/demo.gif"], set()),
            ([".github/workflows/ci.yaml"], set(JOBS)),
            (["new-build-input"], set(JOBS)),
            (["backend/a.go", "frontend/a.ts"], set(JOBS)),
        ]
        for paths, expected in cases:
            with self.subTest(paths=paths):
                self.assertEqual(select_jobs(paths), expected)

    def test_main_push_always_runs_every_job(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            with patch.dict(os.environ, {"EVENT": "push", "GITHUB_OUTPUT": str(output)}):
                main()
            self.assertEqual(output.read_text().count("=true\n"), 4)

    def test_failed_diff_runs_every_job(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            with patch.dict(os.environ, {
                "EVENT": "pull_request", "BASE_SHA": "base", "HEAD_SHA": "head",
                "GITHUB_OUTPUT": str(output),
            }), patch("ci_changes.subprocess.check_output", side_effect=subprocess.CalledProcessError(1, "git")):
                main()
            self.assertEqual(output.read_text().count("=true\n"), 4)

    def test_rename_out_of_backend_keeps_backend_checks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            def git(*args):
                return subprocess.check_output(["git", "-C", directory, *args]).decode().strip()
            git("init", "-q")
            git("config", "user.email", "test@example.invalid")
            git("config", "user.name", "CI test")
            (root / "backend").mkdir()
            (root / "backend/a.go").write_text("package example\n")
            git("add", ".")
            git("commit", "-qm", "test: initial")
            base = git("rev-parse", "HEAD")
            (root / "docs").mkdir()
            git("mv", "backend/a.go", "docs/a.go")
            git("commit", "-qm", "test: move")
            output = root / "output"
            with patch.dict(os.environ, {
                "EVENT": "pull_request", "BASE_SHA": base, "HEAD_SHA": git("rev-parse", "HEAD"),
                "GITHUB_OUTPUT": str(output),
            }), patch("ci_changes.subprocess.check_output", wraps=subprocess.check_output) as command:
                previous = os.getcwd()
                try:
                    os.chdir(directory)
                    main()
                finally:
                    os.chdir(previous)
                self.assertIn("--no-renames", command.call_args.args[0])
            self.assertIn("backend=true", output.read_text())
            self.assertIn("frontend=false", output.read_text())


if __name__ == "__main__":
    unittest.main()
