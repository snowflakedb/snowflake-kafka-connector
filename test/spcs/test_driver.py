"""Offline safety contracts. No credentials, Docker, or Snowflake required."""

import argparse
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch
import contextlib
import io
import signal
import subprocess

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_spcs_release as runner


class SmokeTests(unittest.TestCase):
    def test_exact_data_and_successful_job_pass(self):
        runner.verify("DONE", "SPCS_SMOKE_EXIT=0\n", [str(n) for n in range(1, 101)])

    def test_running_failed_and_unknown_jobs_fail(self):
        for status in ("RUNNING", "FAILED", "UNKNOWN", None):
            with self.subTest(status=status), self.assertRaises(RuntimeError):
                runner.verify(status, "SPCS_SMOKE_EXIT=0\n", [])

    def test_missing_failed_and_duplicate_completion_fail(self):
        for logs in (
            "",
            "SPCS_SMOKE_EXIT=1\n",
            "SPCS_SMOKE_EXIT=0\nSPCS_SMOKE_EXIT=0\n",
        ):
            with self.subTest(logs=logs), self.assertRaises(RuntimeError):
                runner.verify("DONE", logs, [str(n) for n in range(1, 101)])

    def test_short_duplicate_and_wrong_values_fail(self):
        for values in (
            [str(n) for n in range(1, 100)],
            ["1"] * 100,
            [str(n) for n in range(100)],
        ):
            with self.subTest(values=values), self.assertRaises(RuntimeError):
                runner.verify("DONE", "SPCS_SMOKE_EXIT=0\n", values)

    def test_wrong_account_stops_before_any_other_statement(self):
        args = argparse.Namespace(
            expected_account="EXPECTED", role="SYSADMIN", exclusive_pool=True
        )
        with patch.object(
            runner, "sql", return_value=[{"ACCOUNT": "OTHER"}]
        ) as execute:
            with self.assertRaisesRegex(RuntimeError, "Wrong account"):
                runner.preflight(args)
            self.assertEqual(execute.call_count, 1)

    def test_busy_pool_rejected(self):
        args = argparse.Namespace(
            expected_account="EXPECTED",
            role="SYSADMIN",
            exclusive_pool=True,
            pool="POOL",
        )
        with patch.object(
            runner,
            "sql",
            side_effect=[
                [{"ACCOUNT": "EXPECTED", "ROLE": "SYSADMIN"}],
                [{"name": "POOL", "num_services": 1, "num_jobs": 0}],
            ],
        ):
            with self.assertRaisesRegex(RuntimeError, "unused"):
                runner.preflight(args)

    def test_timeout(self):
        with (
            patch.object(runner.time, "monotonic", side_effect=[0, 100]),
            patch.object(runner, "sql") as execute,
        ):
            with self.assertRaises(TimeoutError):
                runner.wait_for_job(argparse.Namespace(timeout=60), "JOB")
            execute.assert_not_called()

    def test_cleanup_stops_if_job_deletion_fails(self):
        with patch.object(
            runner, "sql", side_effect=RuntimeError("drop failed")
        ) as execute:
            with self.assertRaises(RuntimeError):
                runner.cleanup(None, "DB.RUN", "DB.RUN.JOB", "@DB.S.ST/RUN/")
            self.assertEqual(execute.call_count, 1)

    def test_cleanup_never_suspends_other_work(self):
        with patch.object(
            runner,
            "sql",
            side_effect=[
                [],
                [],
                [],
                [{"name": "POOL", "num_services": 1, "num_jobs": 0}],
            ],
        ) as execute:
            with self.assertRaisesRegex(RuntimeError, "other work"):
                runner.cleanup(
                    argparse.Namespace(pool="POOL"),
                    "DB.RUN",
                    "DB.RUN.JOB",
                    "@DB.S.ST/RUN/",
                )
            self.assertFalse(
                any("ALTER COMPUTE" in call.args[1] for call in execute.call_args_list)
            )

    def test_cleanup_order_and_pool_restoration(self):
        with patch.object(
            runner,
            "sql",
            side_effect=[
                [],
                [],
                [],
                [{"name": "POOL", "num_services": 0, "num_jobs": 0}],
                [],
                [],
                [],
                [{"state": "SUSPENDED", "num_services": 0, "num_jobs": 0}],
            ],
        ) as execute:
            runner.cleanup(
                argparse.Namespace(pool="POOL"), "DB.RUN", "DB.RUN.JOB", "@DB.S.ST/RUN/"
            )
            statements = [call.args[1] for call in execute.call_args_list]
            self.assertTrue(statements[0].startswith("DROP SERVICE"))
            self.assertIn("ALTER COMPUTE POOL POOL SUSPEND", statements)
            self.assertEqual(statements[-1], "DESCRIBE COMPUTE POOL POOL")

    def test_cleanup_requires_resource_absence(self):
        with patch.object(
            runner,
            "sql",
            side_effect=[
                [],
                [],
                [],
                [{"name": "POOL", "num_services": 0, "num_jobs": 0}],
                [],
                [{"name": "RUN"}],
            ],
        ):
            with self.assertRaisesRegex(RuntimeError, "still present"):
                runner.cleanup(
                    argparse.Namespace(pool="POOL"),
                    "DB.RUN",
                    "DB.RUN.JOB",
                    "@DB.S.ST/RUN/",
                )

    def test_cleanup_requires_confirmed_pool_suspension(self):
        with (
            patch.object(
                runner,
                "sql",
                side_effect=[
                    [],
                    [],
                    [],
                    [{"name": "POOL", "num_services": 0, "num_jobs": 0}],
                    [],
                    [],
                    [],
                ],
            ),
            patch.object(runner.time, "monotonic", side_effect=[0, 61]),
        ):
            with self.assertRaisesRegex(TimeoutError, "not confirmed"):
                runner.cleanup(
                    argparse.Namespace(pool="POOL"),
                    "DB.RUN",
                    "DB.RUN.JOB",
                    "@DB.S.ST/RUN/",
                )

    def test_error_summary_excludes_credentials(self):
        result = runner.error_summary(RuntimeError("390115 (08001): password=secret"))
        self.assertEqual(result, {"type": "RuntimeError", "sql_codes": ["390115"]})

    def test_identifiers_reject_sql_fragments(self):
        for text in ("", "a.b", "x; DROP", "x'", "x\n"):
            with self.subTest(text=text), self.assertRaises(ValueError):
                runner.identifier(text)

    def test_spec_has_no_public_endpoints_or_credentials(self):
        args = argparse.Namespace(image="/db/s/r/i@sha256:" + "a" * 64, warehouse="WH")
        spec = json.loads(runner.specification(args, "@DB.S.ST/RUN/"))
        self.assertNotIn("endpoints", spec["spec"])
        container = spec["spec"]["containers"][0]
        self.assertEqual(container["command"], ["/bin/bash"])
        self.assertEqual(container["args"][0], "-c")
        self.assertEqual(
            spec["spec"]["containers"][0]["env"], {"SPCS_QUERY_WAREHOUSE": "WH"}
        )

    def test_evidence_is_private(self):
        scratch = runner.HERE / ".scratch"
        scratch.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as directory:
            path = Path(directory) / "evidence.json"
            runner.save(path, {"passed": False})
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)


class CliTests(unittest.TestCase):
    def setUp(self):
        scratch = runner.HERE / ".scratch"
        scratch.mkdir(exist_ok=True)
        directory = tempfile.TemporaryDirectory(dir=scratch)
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        jar = self.directory / "kc.jar"
        jar.write_bytes(b"test artifact")
        self.arguments = [
            "--connection",
            "fixture",
            "--expected-account",
            "EXPECTED",
            "--pool",
            "POOL",
            "--stage",
            "DB.S.ST",
            "--warehouse",
            "WH",
            "--image",
            "/db/s/repo/smoke@sha256:" + "a" * 64,
            "--jar",
            str(jar),
            "--evidence",
            str(self.directory / "result.json"),
            "--exclusive-pool",
        ]

    def test_cleanup_policy_defaults_to_warn(self):
        self.assertEqual(runner.parse_args(self.arguments).cleanup_policy, "warn")

    def test_strict_cleanup_policy_is_selectable(self):
        args = runner.parse_args(self.arguments + ["--cleanup-policy", "fail"])
        self.assertEqual(args.cleanup_policy, "fail")

    def test_invalid_cleanup_policy_is_rejected(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            runner.parse_args(self.arguments + ["--cleanup-policy", "ignore"])

    def test_exclusive_pool_acknowledgment_is_required(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            runner.parse_args(self.arguments[:-1])

    def test_existing_evidence_or_log_is_not_overwritten(self):
        for suffix in (".json", ".log"):
            with self.subTest(suffix=suffix):
                path = self.directory / ("result" + suffix)
                path.write_text("previous evidence")
                with (
                    contextlib.redirect_stderr(io.StringIO()),
                    self.assertRaises(SystemExit),
                ):
                    runner.parse_args(self.arguments)
                self.assertEqual(path.read_text(), "previous evidence")
                path.unlink()


class RecoveryTests(unittest.TestCase):
    def setUp(self):
        self.stack = contextlib.ExitStack()
        self.addCleanup(self.stack.close)
        scratch = runner.HERE / ".scratch"
        scratch.mkdir(exist_ok=True)
        directory = Path(
            self.stack.enter_context(tempfile.TemporaryDirectory(dir=scratch))
        )
        jar = directory / "kc.jar"
        jar.write_bytes(b"fake test artifact")
        self.args = argparse.Namespace(
            database="DB",
            stage="DB.S.ST",
            pool="POOL",
            image="image",
            warehouse="WH",
            jar=jar,
            evidence=directory / "result.json",
            cleanup_policy="warn",
            egress=None,
        )
        self.stack.enter_context(
            patch.object(runner, "preflight", return_value={"ACCOUNT": "EXPECTED"})
        )
        self.stack.enter_context(
            patch.object(
                runner,
                "make_payload",
                side_effect=lambda path, jar: path.write_bytes(b"payload"),
            )
        )
        self.events = []
        self.created = False
        self.create_error = None
        self.marker_mismatch = False
        self.cleanup_error = None
        self.stack.enter_context(patch.object(runner, "sql", side_effect=self.sql))
        self.wait = self.stack.enter_context(
            patch.object(runner, "wait_for_job", return_value="DONE")
        )
        self.stack.enter_context(
            patch.object(runner, "cleanup", side_effect=self.cleanup)
        )
        self.stack.enter_context(contextlib.redirect_stdout(io.StringIO()))
        self.stack.enter_context(contextlib.redirect_stderr(io.StringIO()))

    def sql(self, args, statement, **kwargs):
        if statement.startswith("CREATE SCHEMA"):
            self.created = True
            self.schema = statement.split()[2]
            self.marker = statement.split("'")[1]
            if self.create_error:
                raise self.create_error
        if statement.startswith("SHOW SCHEMAS"):
            return [
                {
                    "name": self.schema.split(".")[1],
                    "comment": "other" if self.marker_mismatch else self.marker,
                }
            ]
        if "GET_SERVICE_LOGS" in statement:
            self.events.append("logs")
            return [
                {"LOGS": "secret=do-not-share\n1 passed in 2.0s\nSPCS_SMOKE_EXIT=0\n"}
            ]
        if statement.startswith('SELECT "number"'):
            return [{"number": str(value)} for value in range(1, 101)]
        return []

    def cleanup(self, *args):
        self.events.append("cleanup")
        if self.cleanup_error:
            raise self.cleanup_error

    def evidence(self):
        return json.loads(self.args.evidence.read_text())

    def test_success_and_cleanup_pass(self):
        result = runner.run(self.args)
        self.assertEqual(result["outcome"], "PASS")
        self.assertTrue(result["test_passed"])
        self.assertTrue(result["cleanup_complete"])

    def test_cleanup_warning_preserves_test_pass(self):
        self.cleanup_error = RuntimeError("cleanup failed with secret")
        result = runner.run(self.args)
        self.assertTrue(result["passed"])
        self.assertFalse(result["cleanup_complete"])
        self.assertEqual(result["outcome"], "PASS_WITH_CLEANUP_WARNING")
        self.assertNotIn("secret", json.dumps(result))

    def test_strict_cleanup_fails_without_reclassifying_test(self):
        self.args.cleanup_policy = "fail"
        self.cleanup_error = RuntimeError("cleanup failed")
        with self.assertRaisesRegex(RuntimeError, "strict"):
            runner.run(self.args)
        self.assertTrue(self.evidence()["test_passed"])
        self.assertFalse(self.evidence()["passed"])

    def test_timeout_collects_logs_before_cleanup_and_never_passes(self):
        self.wait.side_effect = TimeoutError("deadline")
        self.cleanup_error = RuntimeError("cleanup also failed")
        with self.assertRaisesRegex(RuntimeError, "TimeoutError"):
            runner.run(self.args)
        self.assertEqual(self.events, ["logs", "cleanup"])
        self.assertFalse(self.evidence()["passed"])
        self.assertEqual(self.evidence()["error"]["type"], "TimeoutError")
        self.assertIn("cleanup_error", self.evidence())

    def test_ambiguous_create_reconciles_ownership_before_cleanup(self):
        self.create_error = subprocess.TimeoutExpired("snow", 90)
        with self.assertRaises(RuntimeError):
            runner.run(self.args)
        self.assertEqual(self.events, ["cleanup"])
        self.assertTrue(self.evidence()["cleanup_complete"])
        self.assertFalse(self.evidence()["passed"])

    def test_ambiguous_create_never_deletes_foreign_schema(self):
        self.create_error = subprocess.TimeoutExpired("snow", 90)
        self.marker_mismatch = True
        with self.assertRaises(RuntimeError):
            runner.run(self.args)
        self.assertNotIn("cleanup", self.events)
        self.assertFalse(self.evidence()["cleanup_complete"])

    def test_sigterm_runs_cleanup_and_restores_handler(self):
        previous = signal.getsignal(signal.SIGTERM)
        self.wait.side_effect = lambda *args: signal.raise_signal(signal.SIGTERM)
        with self.assertRaisesRegex(RuntimeError, "InterruptedError"):
            runner.run(self.args)
        self.assertEqual(self.events, ["logs", "cleanup"])
        self.assertFalse(self.evidence()["passed"])
        self.assertEqual(signal.getsignal(signal.SIGTERM), previous)

    def test_json_diagnostics_never_include_raw_log_text(self):
        runner.run(self.args)
        evidence = self.evidence()
        self.assertNotIn("do-not-share", json.dumps(evidence))
        self.assertEqual(evidence["diagnostics"]["pytest_counts"], ["1 passed"])
        self.assertEqual(
            self.args.evidence.with_suffix(".log").stat().st_mode & 0o777, 0o600
        )

    def test_failed_job_never_passes_under_warn_policy(self):
        self.wait.return_value = "FAILED"
        with self.assertRaises(RuntimeError):
            runner.run(self.args)
        self.assertFalse(self.evidence()["test_passed"])
        self.assertFalse(self.evidence()["passed"])

    def test_diagnostics_failure_cannot_prevent_cleanup(self):
        self.wait.side_effect = TimeoutError("deadline")
        with patch.object(
            runner, "capture_diagnostics", side_effect=RuntimeError("unavailable")
        ):
            with self.assertRaises(RuntimeError):
                runner.run(self.args)
        self.assertEqual(self.events, ["cleanup"])
        self.assertIn("diagnostics_error", self.evidence())
        self.assertTrue(self.evidence()["cleanup_complete"])

    def test_keyboard_interrupt_cleans_up_and_fails(self):
        self.wait.side_effect = KeyboardInterrupt()
        with self.assertRaisesRegex(RuntimeError, "KeyboardInterrupt"):
            runner.run(self.args)
        self.assertEqual(self.events, ["logs", "cleanup"])
        self.assertFalse(self.evidence()["passed"])

    def test_policy_matrix(self):
        for policy in ("warn", "fail"):
            for test_passed in (False, True):
                for cleanup_complete in (False, True):
                    with self.subTest(
                        policy=policy,
                        test_passed=test_passed,
                        cleanup_complete=cleanup_complete,
                    ):
                        evidence = dict(
                            test_passed=test_passed, cleanup_complete=cleanup_complete
                        )
                        runner.finalize_result(evidence, policy)
                        self.assertEqual(
                            evidence["passed"],
                            test_passed and (cleanup_complete or policy == "warn"),
                        )

    def test_exclusivity_required_before_sql(self):
        with patch.object(runner, "preflight", wraps=original_preflight):
            with self.assertRaisesRegex(ValueError, "exclusive"):
                runner.preflight(argparse.Namespace(exclusive_pool=False))


original_preflight = runner.preflight


if __name__ == "__main__":
    unittest.main()
