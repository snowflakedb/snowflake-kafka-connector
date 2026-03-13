#!/usr/bin/env python3
"""
Parse Maven Surefire and Failsafe XML reports, append a Markdown summary to
GITHUB_STEP_SUMMARY when set, and emit ::error workflow commands to stderr
for annotations. Exits silently when GITHUB_STEP_SUMMARY is unset or no failures.
"""

import os
import re
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


def _classname_to_path(classname: str, connector_root: Path):
    """Return repo-relative path to Java file (src/test/... or src/main/...)."""
    rel = classname.replace(".", "/") + ".java"
    for prefix in ("src/test/java/", "src/main/java/"):
        candidate = connector_root / prefix / rel
        if candidate.exists():
            return prefix + rel
    return None


def _line_in_test_class(stack_trace: str, classname: str):
    """First (File.java:line) in stack trace for the test class (not JUnit/framework)."""
    test_class_file = classname.split(".")[-1] + ".java"
    pattern = re.escape(test_class_file) + r":(\d+)\)"
    match = re.search(pattern, stack_trace)
    return int(match.group(1)) if match else None


def parse_suite(path: Path) -> list[tuple[str, str, str, str, str]]:
    """Parse a TEST-*.xml file; return list of (classname, testname, exc_type, message, stack_trace)."""
    failures = []
    try:
        root = ET.parse(path).getroot()
    except (ET.ParseError, OSError):
        return failures
    for testcase in root.findall(".//testcase"):
        for kind in ("failure", "error"):
            node = testcase.find(kind)
            if node is not None:
                classname = testcase.get("classname", "")
                name = testcase.get("name", "")
                exc_type = (node.get("type") or "").strip()
                message = (node.get("message") or "").strip()
                stack_trace = (node.text or "").strip()
                failures.append((classname, name, exc_type, message, stack_trace))
    return failures


def _first_line_for_annotation(exc_type: str, message: str, stack_trace: str) -> str:
    """First line of failure for ::error message (exception type + message or first stack line)."""
    if message:
        first = f"{exc_type}: {message}" if exc_type else message
    elif stack_trace:
        first = stack_trace.split("\n")[0].strip()
    else:
        first = exc_type or "Failure"
    return first[:500]


def _emit_error_annotation(
    classname: str,
    name: str,
    exc_type: str,
    message: str,
    stack_trace: str,
    connector_root: Path,
    stderr: object,
) -> None:
    """Print one ::error workflow command to stderr for GitHub annotations."""
    title = f"{classname}#{name}"
    first_line = _first_line_for_annotation(exc_type, message, stack_trace)
    first_line_escaped = first_line.replace("%", "%25").replace("\r", "%0D").replace("\n", "%0A")
    file_path = _classname_to_path(classname, connector_root)
    file_line = _line_in_test_class(stack_trace, classname) if stack_trace else None
    parts = [f"title={title}"]
    if file_path:
        parts.append(f"file={file_path}")
    if file_line is not None:
        parts.append(f"line={file_line}")
    opts = ",".join(parts)
    print(f"::error {opts}::{first_line_escaped}", file=stderr)


def main() -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        sys.exit(0)
    connector_root = Path(sys.argv[1]) if len(sys.argv) >= 2 else Path(os.environ.get("GITHUB_WORKSPACE", "."))
    surefire_dir = connector_root / "target" / "surefire-reports"
    failsafe_dir = connector_root / "target" / "failsafe-reports"
    all_failures = []
    for report_dir in (surefire_dir, failsafe_dir):
        if not report_dir.is_dir():
            continue
        for path in sorted(report_dir.glob("TEST-*.xml")):
            all_failures.extend(parse_suite(path))
    if not all_failures:
        sys.exit(0)
    for classname, name, exc_type, message, stack_trace in all_failures:
        _emit_error_annotation(
            classname, name, exc_type, message, stack_trace, connector_root, sys.stderr
        )
    # Group by class for headings: class -> [(name, exc_type, message, stack_trace), ...]
    by_class = {}
    for classname, name, exc_type, message, stack_trace in all_failures:
        by_class.setdefault(classname, []).append((name, exc_type, message, stack_trace))
    lines = ["", "## Java test failures", ""]
    for classname in sorted(by_class.keys()):
        short_name = classname.split(".")[-1] if classname else classname
        lines.append(f"## {short_name}")
        lines.append("")
        lines.append(f"**Class:** `{classname}`")
        lines.append("")
        for name, exc_type, message, stack_trace in by_class[classname]:
            lines.append(f"### {name}")
            lines.append("")
            if exc_type:
                lines.append(f"**Exception type:** `{exc_type}`")
                lines.append("")
            if message:
                lines.append("**Message:**")
                lines.append("")
                lines.append(message)
                lines.append("")
            if stack_trace:
                lines.append("**Stack trace:**")
                lines.append("")
                lines.append("```")
                lines.append(stack_trace)
                lines.append("```")
                lines.append("")
        lines.append("")
    with open(summary_path, "a", encoding="utf-8") as summary_file:
        summary_file.write("\n".join(lines))


if __name__ == "__main__":
    main()
