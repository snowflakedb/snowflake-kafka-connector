"""Offline lifecycle contracts: no account connection or policy mutation."""
import contextlib
import io
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_spcs_release as driver
from spcs_oracle import Verdict


class DriverTests(unittest.TestCase):
    def test_invalid_arguments_rejected_before_connection(self):
        for flags in (["--cells", ""], ["--cells", "A,"], ["--cells", "A,A"],
                      ["--nrecords", "0"], ["--timeout-secs", "0"],
                      ["--retries", "-1"], ["--retries", "2"]):
            with self.subTest(flags=flags), contextlib.redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    driver.parse_args(["--journal", "unused", *flags])

    def test_restores_policy_after_upload_failure_and_retains_lock(self):
        journal = Mock()
        with patch.object(driver, "preflight"), patch.object(driver, "query") as query, \
             patch.object(driver, "account_policy", return_value="ORIGINAL"), \
             patch.object(driver, "upload_harness", side_effect=RuntimeError("upload")), \
             patch.object(driver, "set_policy") as policy:
            with self.assertRaisesRegex(RuntimeError, "upload"):
                driver.execute(Mock(), Mock(), "ACCOUNT", "RUN", journal)
            policy.assert_called_once()
            self.assertEqual(policy.call_args.args[1], "ORIGINAL")
            self.assertFalse(any("DROP TABLE" in call.args[1] for call in query.call_args_list))
            self.assertEqual(journal.save.call_args.kwargs["phase"], "recovery_required")

    def test_success_restores_policy_before_releasing_lock(self):
        actions = []
        args = driver.parse_args(["--journal", "unused", "--cells", "A"])
        with patch.object(driver, "preflight"), \
             patch.object(driver, "query", side_effect=lambda conn, sql: actions.append(sql)), \
             patch.object(driver, "account_policy", return_value="ORIGINAL"), \
             patch.object(driver, "upload_harness"), \
             patch.object(driver, "run_cell", return_value=Verdict("A", True)), \
             patch.object(driver, "set_policy", side_effect=lambda conn, policy: actions.append(policy)):
            self.assertEqual(driver.execute(Mock(), args, "ACCOUNT", "RUN", Mock()), 0)
        self.assertLess(actions.index("ORIGINAL"), actions.index("DROP TABLE " + driver.LOCK))

    def test_restore_failure_cannot_report_success_or_release_lock(self):
        args = driver.parse_args(["--journal", "unused", "--cells", "A"])
        with patch.object(driver, "preflight"), patch.object(driver, "query") as query, \
             patch.object(driver, "account_policy", return_value=None), \
             patch.object(driver, "upload_harness"), \
             patch.object(driver, "run_cell", return_value=Verdict("A", True)), \
             patch.object(driver, "set_policy", side_effect=RuntimeError("restore")):
            with self.assertRaisesRegex(RuntimeError, "restore"):
                driver.execute(Mock(), args, "ACCOUNT", "RUN", Mock())
            self.assertFalse(any("DROP TABLE" in call.args[1] for call in query.call_args_list))

    def test_journal_failure_cannot_prevent_policy_restoration(self):
        args = driver.parse_args(["--journal", "unused", "--cells", "A"])
        journal = Mock()
        journal.save.side_effect = [None, OSError("disk full")]
        with patch.object(driver, "preflight"), patch.object(driver, "query") as query, \
             patch.object(driver, "account_policy", return_value="ORIGINAL"), \
             patch.object(driver, "upload_harness"), \
             patch.object(driver, "run_cell", return_value=Verdict("A", True)), \
             patch.object(driver, "set_policy") as policy:
            with self.assertRaisesRegex(OSError, "disk full"):
                driver.execute(Mock(), args, "ACCOUNT", "RUN", journal)
            self.assertEqual(policy.call_args.args[1], "ORIGINAL")
            self.assertFalse(any("DROP TABLE" in call.args[1] for call in query.call_args_list))

    def test_lock_collision_never_changes_policy(self):
        with patch.object(driver, "preflight"), \
             patch.object(driver, "query", side_effect=RuntimeError("already exists")), \
             patch.object(driver, "set_policy") as policy:
            with self.assertRaises(RuntimeError):
                driver.execute(Mock(), Mock(), "ACCOUNT", "RUN", Mock())
            policy.assert_not_called()

    def test_wrong_account_rejected(self):
        with patch.object(driver, "query", return_value=[("OTHER",)]):
            with self.assertRaisesRegex(RuntimeError, "unexpected account"):
                driver.preflight(Mock(), "EXPECTED")

    def test_identifier_quoting(self):
        self.assertEqual(driver.quoted_identifier('a"b'), '"a""b"')
        with self.assertRaises(ValueError):
            driver.quoted_identifier("")

    def test_journal_is_private_and_not_overwritten(self):
        scratch = driver.HERE / ".scratch"
        scratch.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(dir=scratch) as folder:
            path = Path(folder) / "journal.json"
            journal = driver.Journal(path, "ACCOUNT", "RUN")
            journal.save(phase="complete")
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)
            with self.assertRaises(FileExistsError):
                driver.Journal(path, "OTHER", "RUN2")


if __name__ == "__main__":
    unittest.main()
