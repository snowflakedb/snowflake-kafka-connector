"""Offline functional pacing contracts; run directly, without E2E fixtures."""

import importlib.util
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch

from _pytest.config.argparsing import Parser

TEST_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TEST_ROOT))


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


pacing = load_module("pacing_conftest", TEST_ROOT / "conftest.py")
compatibility = load_module(
    "compatibility_conftest", TEST_ROOT / "tests/compatibility/conftest.py"
)


class PacingTests(unittest.TestCase):
    def test_cli_default_and_bounds(self):
        parser = Parser()
        pacing.pytest_addoption(parser)
        self.assertEqual(parser.parse([]).functional_test_delay, 0)
        for delay in (0, 5, 60):
            self.assertEqual(
                parser.parse(
                    ["--functional-test-delay", str(delay)]
                ).functional_test_delay,
                delay,
            )
        import pytest

        for invalid in ("-1", "61", "1.5", "nan", "bad"):
            with self.subTest(invalid=invalid), self.assertRaises(pytest.UsageError):
                parser.parse(["--functional-test-delay", invalid])

    def check_pacing(self, delay, markers, expected):
        item = Mock(nodeid="test_functional[v4]")
        item.config.getoption.return_value = delay
        item.get_closest_marker.side_effect = lambda name: name in markers
        with patch.object(pacing.time, "sleep") as sleep:
            pacing.pytest_runtest_setup(item)
        if expected:
            sleep.assert_called_once_with(delay)
        else:
            sleep.assert_not_called()

    def test_default_does_not_sleep(self):
        self.check_pacing(0, (), False)

    def test_functional_case_is_paced(self):
        self.check_pacing(5, ("compatibility",), True)

    def test_nonfunctional_workloads_are_not_paced(self):
        for marker in ("pressure", "soak", "fault_injection", "spcs"):
            with self.subTest(marker=marker):
                self.check_pacing(5, (marker,), False)

    def test_report_explains_slower_functional_runtime(self):
        config = Mock()
        config.getoption.return_value = 5
        header = pacing.pytest_report_header(config)
        self.assertIn("5s/case", header)
        self.assertIn("assertion deadlines unchanged", header)

    def test_hook_runs_before_fixture_setup(self):
        self.assertTrue(pacing.pytest_runtest_setup.pytest_impl["tryfirst"])


class CompatibilityPollingTests(unittest.TestCase):
    def run_case(self, counts, failures, timeout=60):
        driver = Mock()
        driver.createConnector.return_value = {"name": "case_v4"}
        driver.select_number_of_records.side_effect = counts
        driver.get_failed_tasks.side_effect = failures
        driver.snowflake_conn.cursor.return_value.execute.return_value.fetchall.return_value = []
        clock = SimpleNamespace(now=0)
        sleeps = []

        def sleep(seconds):
            sleeps.append(seconds)
            clock.now += seconds

        with (
            patch.object(compatibility, "_build_mode_config", return_value={}),
            patch.object(
                compatibility.time, "monotonic", side_effect=lambda: clock.now
            ),
            patch.object(compatibility.time, "sleep", side_effect=sleep),
        ):
            fixture = compatibility.ingest_one_type_abort.__wrapped__(
                driver, "_v4", "v4-compat", Mock(return_value="case_v4")
            )
            run = next(fixture)
            try:
                result = run("case", "NUMBER", [1], timeout=timeout)
            finally:
                fixture.close()
        driver.sendBytesData.assert_called_once()
        driver.closeConnector.assert_called_once_with("case_v4")
        return result, sleeps, clock.now

    def test_success_does_not_wait(self):
        result, sleeps, elapsed = self.run_case([1], [[]])
        self.assertIsNone(result.connector_error)
        self.assertFalse(result.timed_out)
        self.assertEqual(result.last_row_count, 1)
        self.assertEqual(sleeps, [])
        self.assertEqual(elapsed, 0)

    def test_polling_uses_five_seconds(self):
        _, sleeps, elapsed = self.run_case([0, 0, 1], [[], [], []])
        self.assertEqual(sleeps, [5, 5])
        self.assertEqual(elapsed, 10)

    def test_last_sleep_respects_deadline(self):
        result, sleeps, elapsed = self.run_case([0, 0], [[], []], timeout=7)
        self.assertEqual(sleeps, [5, 2])
        self.assertEqual(elapsed, 7)
        self.assertEqual(result.values, [])
        self.assertIsNone(result.connector_error)
        self.assertTrue(result.timed_out)
        self.assertEqual(result.elapsed_seconds, 7)
        self.assertEqual(result.last_row_count, 0)
        self.assertIn("timed_out=True", result.diagnostics)

    def test_missing_table_is_not_reported_as_zero_rows(self):
        result, _, _ = self.run_case([None], [[]], timeout=1)
        self.assertTrue(result.timed_out)
        self.assertIsNone(result.last_row_count)

    def test_failed_task_still_short_circuits(self):
        result, sleeps, _ = self.run_case([], [[{"trace": "validation failed"}]])
        self.assertEqual(result.connector_error, "validation failed")
        self.assertFalse(result.timed_out)
        self.assertEqual(sleeps, [])


if __name__ == "__main__":
    unittest.main()
