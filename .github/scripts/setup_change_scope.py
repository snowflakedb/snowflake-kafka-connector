"""Skip live PR steps only for a complete, exact setup-only change set."""

import os
from pathlib import Path
import re
import subprocess
import sys

SETUP_ONLY = frozenset(
    {
        ".github/workflows/spcs-infrastructure.yml",
        "test/spcs/setup_ci.py",
        "test/spcs/test_setup_ci.py",
        "test/spcs/README.md",
        ".github/scripts/test_spcs_workflow.py",
    }
)


def needs_live_tests(paths):
    return not paths or any(path not in SETUP_ONLY for path in paths)


def classify(event, base, head, root):
    if event != "pull_request":
        return True
    if not all(re.fullmatch(r"[0-9a-f]{40}", value) for value in (base, head)):
        return True
    try:
        result = subprocess.run(
            [
                "git",
                "-C",
                str(root),
                "diff",
                "--no-renames",
                "--name-only",
                "-z",
                f"{base}...{head}",
                "--",
            ],
            capture_output=True,
            check=True,
            timeout=30,
        )
        # Ambiguous merge bases and other warnings are not complete change evidence.
        if result.stderr or (result.stdout and not result.stdout.endswith(b"\0")):
            return True
        paths = result.stdout.decode("utf-8").split("\0")[:-1]
        return needs_live_tests(paths)
    except (OSError, ValueError, UnicodeError, subprocess.SubprocessError):
        return True


def main():
    run_live = classify(
        os.environ.get("GITHUB_EVENT_NAME", ""),
        os.environ.get("PR_BASE_SHA", ""),
        os.environ.get("PR_HEAD_SHA", ""),
        Path(__file__).resolve().parents[2],
    )
    value = str(run_live).lower()
    print(f"run_live={value}")
    with open(os.environ["GITHUB_OUTPUT"], "a") as output:
        output.write(f"run_live={value}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
