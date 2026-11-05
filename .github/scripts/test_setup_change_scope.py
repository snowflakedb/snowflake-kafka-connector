"""Offline contracts for setup-only CI routing; no Snowflake credentials."""

import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import yaml

import setup_change_scope as scope

ROOT = Path(__file__).resolve().parents[2]


def workflow(name):
    return yaml.load(
        (ROOT / ".github/workflows" / name).read_text(), Loader=yaml.BaseLoader
    )


class ScopeTests(unittest.TestCase):
    def test_only_exact_setup_paths_are_exempt(self):
        self.assertEqual(
            scope.SETUP_ONLY,
            {
                ".github/workflows/spcs-infrastructure.yml",
                "test/spcs/setup_ci.py",
                "test/spcs/test_setup_ci.py",
                "test/spcs/README.md",
                ".github/scripts/test_spcs_workflow.py",
            },
        )
        for path in scope.SETUP_ONLY:
            self.assertFalse(scope.needs_live_tests([path]))
        for path in (
            "src/Main.java",
            "test/lib/config.py",
            "test/lib/crypto.py",
            "test/spcs/run_spcs_release.py",
            "test/spcs/new_helper.py",
            ".github/workflows/IntegrationTest.yml",
            ".github/workflows/end-to-end.yaml",
            ".github/scripts/setup_change_scope.py",
            ".github/scripts/test_setup_change_scope.py",
            ".github/scripts/decrypt_secret.sh",
            "test/spcs/INFRASTRUCTURE.md",
        ):
            with self.subTest(path=path):
                self.assertTrue(scope.needs_live_tests(["test/spcs/setup_ci.py", path]))
        self.assertTrue(scope.needs_live_tests([]))

    def test_non_pr_and_missing_evidence_run_normally(self):
        for event in ("push", "schedule", "workflow_dispatch", ""):
            self.assertTrue(scope.classify(event, "", "", ROOT))
        for base, head in (("", ""), ("-option", "a" * 40), ("a" * 40, "bad")):
            with patch.object(scope.subprocess, "run") as run:
                self.assertTrue(scope.classify("pull_request", base, head, ROOT))
                run.assert_not_called()
        for result in (
            subprocess.CompletedProcess([], 0, b"test/spcs/setup_ci.py", b""),
            subprocess.CompletedProcess([], 0, b"\xff\0", b""),
            subprocess.CompletedProcess([], 0, b"test/spcs/setup_ci.py\0", b"warning"),
            subprocess.CompletedProcess([], 0, b"", b""),
        ):
            with patch.object(scope.subprocess, "run", return_value=result):
                self.assertTrue(
                    scope.classify("pull_request", "a" * 40, "b" * 40, ROOT)
                )
        for error in (
            OSError(),
            subprocess.TimeoutExpired("git", 30),
            subprocess.CalledProcessError(1, "git"),
        ):
            with patch.object(scope.subprocess, "run", side_effect=error):
                self.assertTrue(
                    scope.classify("pull_request", "a" * 40, "b" * 40, ROOT)
                )

    def test_real_git_merge_base_renames_deletions_and_multiple_commits(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)

            def git(*args):
                return subprocess.run(
                    [
                        "git",
                        "-C",
                        directory,
                        "-c",
                        "user.name=Fixture",
                        "-c",
                        "user.email=fixture@example.invalid",
                        "-c",
                        "commit.gpgsign=false",
                        *args,
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                    env={
                        "PATH": os.environ["PATH"],
                        "HOME": directory,
                        "GIT_CONFIG_NOSYSTEM": "1",
                        "GIT_CONFIG_GLOBAL": os.devnull,
                    },
                ).stdout.strip()

            def commit(path, content):
                target = root / path
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text(content)
                git("add", "--all")
                git("commit", "-m", "fixture")
                return git("rev-parse", "HEAD")

            git("init", "-b", "base")
            base = commit("runtime.py", "original")
            git("checkout", "-b", "candidate")
            head = commit("test/spcs/setup_ci.py", "setup")
            self.assertFalse(scope.classify("pull_request", base, head, root))
            head = commit("test/spcs/README.md", "docs")
            self.assertFalse(scope.classify("pull_request", base, head, root))
            git("checkout", "base")
            advanced_base = commit("base_only.java", "upstream change")
            git("checkout", "candidate")
            self.assertFalse(scope.classify("pull_request", advanced_base, head, root))
            git("mv", "runtime.py", "test/spcs/test_setup_ci.py")
            git("commit", "-m", "rename")
            renamed = git("rev-parse", "HEAD")
            self.assertTrue(
                scope.classify("pull_request", advanced_base, renamed, root)
            )
            git("rm", "test/spcs/README.md")
            git("commit", "-m", "delete setup documentation")
            deleted = git("rev-parse", "HEAD")
            self.assertFalse(scope.classify("pull_request", renamed, deleted, root))
            self.assertTrue(scope.classify("pull_request", base, deleted, root))
            self.assertTrue(scope.classify("pull_request", "f" * 40, deleted, root))

    def test_output_protocol(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            with patch.dict(os.environ, {"GITHUB_OUTPUT": str(output)}, clear=True):
                with patch.object(scope, "classify", return_value=False):
                    self.assertEqual(scope.main(), 0)
            self.assertEqual(output.read_text(), "run_live=false\n")

    def test_workflow_live_steps_are_gated_and_contracts_independent(self):
        for filename, jobs in (
            ("IntegrationTest.yml", ("build_and_test",)),
            ("end-to-end.yaml", ("functional", "fault_injection")),
        ):
            document = workflow(filename)
            classifier = document["jobs"]["change-scope"]
            self.assertEqual(classifier["if"], "github.event_name == 'pull_request'")
            self.assertEqual(classifier["permissions"], {"contents": "read"})
            self.assertEqual(classifier["steps"][0]["with"]["fetch-depth"], "0")
            self.assertNotIn("secrets.", str(classifier))
            for name in jobs:
                job = document["jobs"][name]
                self.assertEqual(job["needs"], "change-scope")
                self.assertIn("!cancelled()", job["if"])
                self.assertNotIn("run_live", job["if"])
                for guard in (
                    "needs.change-scope.result != 'success'",
                    "needs.change-scope.outputs.run_live != 'false'",
                ):
                    self.assertIn(guard, job["env"]["RUN_LIVE"])
                for step in job["steps"]:
                    if step.get("name") == "Report setup-only exemption":
                        self.assertEqual(step["if"], "env.RUN_LIVE == 'false'")
                        self.assertIn("not executed", step["run"])
                    else:
                        self.assertIn("env.RUN_LIVE != 'false'", step["if"])
        contracts = workflow("end-to-end.yaml")["jobs"]["spcs-contracts"]
        self.assertNotIn("needs", contracts)
        self.assertNotIn("if", contracts)
        self.assertNotIn("secrets.", str(contracts))


if __name__ == "__main__":
    unittest.main()
