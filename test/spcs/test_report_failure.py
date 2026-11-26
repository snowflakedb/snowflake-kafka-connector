"""TEST ONLY: offline tests for report_failure.py using a fake Jira (no network)."""

from contextlib import redirect_stdout
import base64
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import urllib.error

sys.path.insert(0, str(Path(__file__).resolve().parent))
import report_failure  # noqa: E402

SECRETS = {
    "JIRA_BASE_URL": "https://jira.example/",
    "JIRA_USER_EMAIL": "ci@example.com",
    "JIRA_API_TOKEN": "synthetic-token",
}
ENVIRON = {
    "RUN_URL": "https://github.com/o/r/actions/runs/1",
    "GITHUB_SHA": "abc123",
    "FAILED_STAGE": "preflight",
}


class FakeJira:
    def __init__(self, open_keys=(), denied=(), assignable=True):
        self.open_keys = list(open_keys)
        self.denied = set(denied)
        self.assignable = assignable
        self.calls = []

    def __call__(self, method, path, body=None):
        self.calls.append((method, path, body))
        if path == "/rest/api/3/myself":
            return {"displayName": "CI", "accountId": "1"}
        if path.startswith("/rest/api/3/mypermissions?"):
            return {
                "permissions": {
                    name: {"havePermission": name not in self.denied}
                    for name in report_failure.PERMISSIONS
                }
            }
        if path.startswith("/rest/api/3/user/assignable/search?"):
            return [{"accountId": "dl"}] if self.assignable else []
        if path.startswith("/rest/api/3/search/jql?"):
            return {"issues": [{"key": key} for key in self.open_keys]}
        if path == "/rest/api/2/issue":
            return {"key": "SNOW-1"}
        return {}


class ReportTest(unittest.TestCase):
    def test_creates_routed_ticket_when_none_is_open(self):
        jira = FakeJira()
        self.assertEqual(report_failure.report(ENVIRON, jira), "created SNOW-1")
        fields = jira.calls[-1][2]["fields"]
        self.assertEqual(fields["components"], [{"id": "19142"}])
        self.assertEqual(fields["customfield_11401"], {"id": "26480"})
        self.assertEqual(
            fields["assignee"], {"id": "712020:0f73754d-6ca5-4bb6-988f-4afa88a8e631"}
        )
        self.assertEqual(fields["labels"], [report_failure.LABEL])
        self.assertIn(ENVIRON["RUN_URL"], fields["description"])
        self.assertIn("preflight", fields["description"])

    def test_comments_on_open_ticket_instead_of_creating_another(self):
        jira = FakeJira(["SNOW-7"])
        self.assertEqual(report_failure.report(ENVIRON, jira), "commented on SNOW-7")
        self.assertEqual([c[0] for c in jira.calls], ["GET", "POST"])
        self.assertEqual(jira.calls[1][1], "/rest/api/2/issue/SNOW-7/comment")

    def test_search_finds_only_open_tickets_with_the_label(self):
        jira = FakeJira()
        report_failure.report(ENVIRON, jira)
        query = jira.calls[0][1]
        self.assertIn(report_failure.LABEL, query)
        self.assertIn("statusCategory+%21%3D+Done", query)

    def check(self, jira):
        with redirect_stdout(io.StringIO()):
            return report_failure.check(jira)

    def test_check_passes_and_files_nothing(self):
        jira = FakeJira()
        self.assertEqual(self.check(jira), [])
        self.assertEqual({c[0] for c in jira.calls}, {"GET"})

    def test_check_names_each_missing_permission(self):
        problems = self.check(FakeJira(denied={"CREATE_ISSUES", "ADD_COMMENTS"}))
        self.assertEqual(
            problems,
            [
                "missing SNOW permission CREATE_ISSUES",
                "missing SNOW permission ADD_COMMENTS",
            ],
        )

    def test_check_asks_about_the_triage_dl(self):
        jira = FakeJira(assignable=False)
        self.assertEqual(len(self.check(jira)), 1)
        self.assertIn(
            "accountId=712020%3A0f73754d-6ca5-4bb6-988f-4afa88a8e631", jira.calls[-1][1]
        )

    def test_check_flag_runs_check_and_never_reports(self):
        secrets = {"JIRA_BASE_URL": "u", "JIRA_USER_EMAIL": "e", "JIRA_API_TOKEN": "t"}
        with (
            patch.dict(report_failure.os.environ, secrets),
            patch.object(report_failure, "check", return_value=[]) as check,
            patch.object(report_failure, "report") as report,
            redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(report_failure.main(["--check"]), 0)
            check.assert_called_once()
            report.assert_not_called()

    def test_check_requires_permission_to_set_the_assignee(self):
        problems = self.check(FakeJira(denied={"ASSIGN_ISSUES"}))
        self.assertEqual(problems, ["missing SNOW permission ASSIGN_ISSUES"])

    def test_without_flag_main_reports_and_never_only_checks(self):
        with (
            patch.dict(report_failure.os.environ, {**SECRETS, **ENVIRON}),
            patch.object(report_failure, "check") as check,
            patch.object(
                report_failure, "report", return_value="created SNOW-1"
            ) as report,
            redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(report_failure.main([]), 0)
            report.assert_called_once()
            check.assert_not_called()

    def test_request_uses_base_url_basic_auth_and_parses_json(self):
        class Response(io.BytesIO):
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False

        with (
            patch.dict(report_failure.os.environ, SECRETS),
            patch.object(
                report_failure.urllib.request,
                "urlopen",
                return_value=Response(b'{"key": "SNOW-9"}'),
            ) as urlopen,
        ):
            result = report_failure.request("POST", "/rest/api/2/issue", {"a": 1})
        self.assertEqual(result, {"key": "SNOW-9"})
        call = urlopen.call_args.args[0]
        self.assertEqual(call.full_url, "https://jira.example/rest/api/2/issue")
        self.assertEqual(call.get_method(), "POST")
        self.assertEqual(json.loads(call.data), {"a": 1})
        expected = base64.b64encode(b"ci@example.com:synthetic-token").decode()
        self.assertEqual(call.get_header("Authorization"), f"Basic {expected}")

    def test_http_error_prints_only_the_status(self):
        error = urllib.error.HTTPError(
            "https://jira.example", 401, "synthetic-token in body", {}, None
        )
        output = io.StringIO()
        with (
            patch.dict(report_failure.os.environ, {**SECRETS, **ENVIRON}),
            patch.object(report_failure, "report", side_effect=error),
            redirect_stdout(output),
        ):
            self.assertEqual(report_failure.main([]), 1)
        self.assertIn("Jira request failed: 401", output.getvalue())
        self.assertNotIn("synthetic-token", output.getvalue())

    def test_unreadable_response_fails_cleanly(self):
        for error in (ValueError("not json"), KeyError("issues")):
            with (
                self.subTest(error=type(error).__name__),
                patch.dict(report_failure.os.environ, {**SECRETS, **ENVIRON}),
                patch.object(report_failure, "report", side_effect=error),
                redirect_stdout(io.StringIO()),
            ):
                self.assertEqual(report_failure.main([]), 1)

    def test_missing_token_fails_without_calling_jira(self):
        with (
            patch.dict(report_failure.os.environ, {"JIRA_API_TOKEN": ""}, clear=False),
            patch.object(report_failure, "request") as request,
            redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(report_failure.main([]), 1)
            request.assert_not_called()


if __name__ == "__main__":
    unittest.main()
