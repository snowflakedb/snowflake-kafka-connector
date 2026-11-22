"""Report a failed nightly KC SPCS smoke run to the Snowpipe Streaming triage DL.

Comments on the open nightly-failure ticket if there is one, otherwise creates
it, so repeated failures stay on one ticket until triage closes it. Uses the
JIRA_BASE_URL, JIRA_USER_EMAIL and JIRA_API_TOKEN repository secrets.
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
        "A failed preflight usually means the SFCTEST0 network policy changed: "
        "run 'Set up permanent KC SPCS infrastructure' with apply. Otherwise "
        "download the spcs-smoke-result artifact and follow 'Result and "
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


def main():
    missing = [
        name
        for name in ("JIRA_BASE_URL", "JIRA_USER_EMAIL", "JIRA_API_TOKEN")
        if not os.environ.get(name)
    ]
    if missing:
        print(f"::error::Cannot report to Jira; secrets not set: {', '.join(missing)}")
        return 1
    try:
        print(report(os.environ), flush=True)
    except OSError as error:
        # HTTPError bodies can echo request data; print only the status.
        print(
            f"::error::Jira request failed: {getattr(error, 'code', type(error).__name__)}"
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
