"""Offline unit tests for the SPCS release oracle (SNOW-4202412).

Run: python3 -m unittest test/spcs/test_oracle.py   (or python3 -m pytest)
"""
import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from spcs_oracle import Observation, evaluate, parse_logs  # noqa: E402

N = 1000
OK_LOGS = "E2E| all records committed\nE2E_ERR 390422=0\nE2E_ERR 395090=0\nE2E_EXIT=0\n"
C_LOGS = ("E2E| ERROR get_pipe_info failed 390422\nE2E_ERR 390422=7\n"
          "E2E_ERR 395090=0\nE2E_EXIT=1\n")


class ParseLogsTest(unittest.TestCase):
    def test_summary(self):
        p = parse_logs(C_LOGS)
        self.assertEqual(p["exit"], 1)
        self.assertEqual(p["390422"], 7)
        self.assertEqual(p["395090"], 0)

    def test_raw_occurrence_without_summary(self):
        p = parse_logs("ERROR code 395090 from ingest\n")
        self.assertIsNone(p["exit"])
        self.assertEqual(p["395090"], 1)


class EvaluateTest(unittest.TestCase):
    def test_a_b_pass(self):
        for cell in ("A", "B"):
            self.assertTrue(evaluate(Observation(cell, N, "DONE", OK_LOGS), N).passed)

    def test_running_never_passes(self):
        for cell in ("A", "B", "C"):
            logs = OK_LOGS if cell != "C" else C_LOGS
            rows = N if cell != "C" else 0
            v = evaluate(Observation(cell, rows, "RUNNING", logs), N)
            self.assertFalse(v.passed, cell)

    def test_rows_short_fails(self):
        self.assertFalse(evaluate(Observation("A", N - 1, "DONE", OK_LOGS), N).passed)

    def test_error_code_fails_even_with_rows(self):
        logs = OK_LOGS.replace("E2E_ERR 395090=0", "E2E_ERR 395090=2")
        self.assertFalse(evaluate(Observation("B", N, "DONE", logs), N).passed)

    def test_job_failed_fails(self):
        self.assertFalse(evaluate(Observation("A", N, "FAILED", OK_LOGS), N).passed)

    def test_c_pass_requires_zero_rows_and_390422(self):
        self.assertTrue(evaluate(Observation("C", 0, "FAILED", C_LOGS), N).passed)
        self.assertTrue(evaluate(Observation("C", 0, "DONE", C_LOGS), N).passed)

    def test_c_rows_appear_fails(self):
        self.assertFalse(evaluate(Observation("C", 5, "FAILED", C_LOGS), N).passed)

    def test_c_without_390422_fails(self):
        logs = "E2E_ERR 390422=0\nE2E_ERR 395090=3\nE2E_EXIT=1\n"
        self.assertFalse(evaluate(Observation("C", 0, "FAILED", logs), N).passed)

    def test_unknown_cell(self):
        self.assertFalse(evaluate(Observation("Z", N, "DONE", OK_LOGS), N).passed)


if __name__ == "__main__":
    unittest.main()
