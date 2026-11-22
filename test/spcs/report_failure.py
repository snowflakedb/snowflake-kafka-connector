"""Report a failed nightly KC SPCS smoke run to the Snowpipe Streaming triage DL.

Comments on the open nightly-failure ticket if there is one, otherwise creates
it, so repeated failures stay on one ticket until triage closes it. Uses the
JIRA_BASE_URL, JIRA_USER_EMAIL and JIRA_API_TOKEN repository secrets.

`--check` files nothing: it confirms the token works, the account may create
and comment on SNOW issues, and the triage DL can be assigned. Run it after
setting or handing over the secrets (see test/spcs/README.md).
"""

import base64
import json
import os
import sys
import urllib.parse
import urllib.request

LABEL = "kc-spcs-nightly-failure"
FIELDS = {
    "project": {"key": "SNOW"},
    "issuetype": {"name": "Bug"},
    # Connectors - Kafka Push (Kafka Connector), Area "Data Ingestion": the
    # values the Kafka connector team uses for its own engineering tickets.
    "components": [{"id": "19142"}],
    "customfield_11401": {"id": "26480"},
    # triage-data_pipelines-snowpipe-streaming-v2-dl
    "assignee": {"id": "712020:0f73754d-6ca5-4bb6-988f-4afa88a8e631"},
    "labels": [LABEL],
}
SUMMARY = "Kafka connector nightly SPCS smoke test is failing"
# ASSIGN_ISSUES: creating with an assignee needs it, or Jira rejects the issue.
PERMISSIONS = ("BROWSE_PROJECTS", "CREATE_ISSUES", "ASSIGN_ISSUES", "ADD_COMMENTS")


def request(method, path, body=None):
    environ = os.environ
    credentials = f"{environ['JIRA_USER_EMAIL']}:{environ['JIRA_API_TOKEN']}"
    call = urllib.request.Request(
        environ["JIRA_BASE_URL"].rstrip("/") + path,
        method=method,
        data=None if body is None else json.dumps(body).encode(),
        headers={
            "Authorization": "Basic " + base64.b64encode(credentials.encode()).decode(),
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
    )
    with urllib.request.urlopen(call, timeout=30) as response:
        return json.load(response)


def message(environ):
    return (
        f"Nightly KC SPCS smoke test failed: {environ['RUN_URL']}\n"
        f"Commit: {environ['GITHUB_SHA']}\n"
        f"Failed stage: {environ['FAILED_STAGE']}\n\n"
        "preflight (failure): its contracts job or setup apply failed; apply "
        "repairs SFCTEST0 objects, grants and network policies, so this "
        "usually means it could not (for example, the CI user lost "
        "ACCOUNTADMIN). The log names the step.\n"
        "(cancelled): another SPCS run took the shared concurrency slot; "
        "rerun the nightly.\n"
        "smoke (failure): download the spcs-smoke-result artifact and follow 'Result and "
        "recovery' in test/spcs/README.md."
    )


def report(environ, call=request):
    jql = f'project = SNOW AND labels = "{LABEL}" AND statusCategory != Done'
    found = call(
        "GET",
        "/rest/api/3/search/jql?"
        + urllib.parse.urlencode({"jql": jql, "fields": "key", "maxResults": 1}),
    )["issues"]
    text = message(environ)
    if found:
        key = found[0]["key"]
        call("POST", f"/rest/api/2/issue/{key}/comment", {"body": text})
        return f"commented on {key}"
    created = call(
        "POST",
        "/rest/api/2/issue",
        {"fields": {**FIELDS, "summary": SUMMARY, "description": text}},
    )
    return f"created {created['key']}"


def check(call=request):
    """Return the problems that would stop a report; empty means ready."""
    me = call("GET", "/rest/api/3/myself")
    print(f"Jira account: {me.get('displayName')} ({me.get('accountId')})", flush=True)
    problems = []
    granted = call(
        "GET",
        "/rest/api/3/mypermissions?"
        + urllib.parse.urlencode(
            {"projectKey": "SNOW", "permissions": ",".join(PERMISSIONS)}
        ),
    )["permissions"]
    for name in PERMISSIONS:
        if not granted.get(name, {}).get("havePermission"):
            problems.append(f"missing SNOW permission {name}")
    assignee = FIELDS["assignee"]["id"]
    if not call(
        "GET",
        "/rest/api/3/user/assignable/search?"
        + urllib.parse.urlencode({"project": "SNOW", "accountId": assignee}),
    ):
        problems.append("the triage DL cannot be assigned SNOW issues by this account")
    return problems


def main(argv=None):
    checking = (sys.argv[1:] if argv is None else argv) == ["--check"]
    missing = [
        name
        for name in ("JIRA_BASE_URL", "JIRA_USER_EMAIL", "JIRA_API_TOKEN")
        if not os.environ.get(name)
    ]
    if missing:
        print(f"::error::Cannot report to Jira; secrets not set: {', '.join(missing)}")
        return 1
    try:
        if checking:
            problems = check()
            for problem in problems:
                print(f"::error::{problem}", flush=True)
            if problems:
                return 1
            print("Jira reporting is ready.", flush=True)
        else:
            print(report(os.environ), flush=True)
    except (OSError, ValueError, KeyError) as error:
        # HTTPError bodies can echo request data; print only the status, or
        # the error type for unreadable or unexpected responses.
        print(
            f"::error::Jira request failed: {getattr(error, 'code', type(error).__name__)}"
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
