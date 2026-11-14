"""Explicit, fail-closed functional lanes. Not a replacement for the full suite."""

import json
import os
from pathlib import Path
import re

import pytest
import requests

from lib.config_migration import V4_CONNECTOR_CLASS

TEST_ROOT = Path(__file__).resolve().parents[1]
# Add files only after auditing their fixtures and direct connector creation.
GROUPS = {
    "core": ("test_string_json.py",),
    "compatibility": ("compatibility/test_type_compatibility.py",),
    "schema_and_correctness": (
        "schema_evolution/test_se_nonnullable_json.py",
        "test_snowpipe_streaming_string_json_dlq.py",
    ),
}
BLOCKED = {"pressure", "soak", "fault_injection", "spcs", "iceberg"}


def exclusion(item, group):
    relative = item.path.relative_to(TEST_ROOT / "tests").as_posix()
    if relative not in GROUPS[group]:
        return "outside audited lane"
    if any(item.get_closest_marker(marker) for marker in BLOCKED):
        return "excluded workload"
    params = getattr(getattr(item, "callspec", None), "params", {})
    versions = {
        key: params[key]
        for key in ("connector_version", "ingestion_mode")
        if key in params
    }
    if not versions:
        return "unclassified version"
    if any(value not in {"v4", "v4-compat", "v4-ht"} for value in versions.values()):
        return "legacy or unknown version"
    return None


def validate_plugins(plugins):
    if not isinstance(plugins, list):
        raise RuntimeError("Invalid worker plugin inventory")
    snowflake = [
        plugin
        for plugin in plugins
        if "snowflake" in str(plugin.get("class", "")).lower()
    ]
    if len(snowflake) != 1 or snowflake[0].get("class") != V4_CONNECTOR_CLASS:
        raise RuntimeError("Worker must expose exactly one Snowflake connector: KC v4")
    version = snowflake[0].get("version", "")
    if not re.fullmatch(r"4\.\d+\.\d+(?:[-+][A-Za-z0-9.-]+)?", version):
        raise RuntimeError("Worker did not report a KC v4 artifact version")
    return version


def require_v4_config(config):
    if config.get("connector.class") != V4_CONNECTOR_CLASS:
        raise RuntimeError(
            "v4-only qualification refuses legacy or unknown connector classes"
        )


def pytest_addoption(parser):
    parser.addoption("--v4-only-group", choices=tuple(GROUPS), default=None)
    parser.addoption("--v4-inventory", type=Path, default=None)
    parser.addoption("--v4-artifact", type=Path, default=None)


def pytest_configure(config):
    group = config.getoption("--v4-only-group")
    if not group:
        return
    if config.getoption("--v4-trial") or config.getoption("--spcs"):
        raise pytest.UsageError(
            "v4-only qualification cannot be combined with trial or SPCS"
        )
    if config.getoption("numprocesses", default=0):
        raise pytest.UsageError("v4-only qualification requires sequential execution")
    config._v4_previous_guard = os.environ.get("KC_V4_ONLY")
    os.environ["KC_V4_ONLY"] = "1"
    config._v4_worker_checked = False
    config._v4_passed = 0
    config.pluginmanager.register(_OutcomeCounter(config), "v4-qualification-outcomes")


class _OutcomeCounter:
    def __init__(self, config):
        self.config = config

    def pytest_runtest_logreport(self, report):
        if report.when == "call" and report.passed and not hasattr(report, "wasxfail"):
            self.config._v4_passed += 1


def pytest_sessionfinish(session, exitstatus):
    config = session.config
    if (
        config.getoption("--v4-only-group")
        and not config.getoption("collectonly")
        and exitstatus == 0
        and not config._v4_passed
    ):
        session.exitstatus = pytest.ExitCode.TESTS_FAILED


def pytest_unconfigure(config):
    if hasattr(config, "_v4_previous_guard"):
        previous = config._v4_previous_guard
        if previous is None:
            os.environ.pop("KC_V4_ONLY", None)
        else:
            os.environ["KC_V4_ONLY"] = previous


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config, items):
    group = config.getoption("--v4-only-group")
    if not group:
        return
    selected, rejected, inventory = [], [], []
    for item in items:
        reason = exclusion(item, group)
        (rejected if reason else selected).append(item)
        inventory.append(
            {"nodeid": item.nodeid, "selected": reason is None, "reason": reason}
        )
    config.hook.pytest_deselected(items=rejected)
    items[:] = selected
    path = config.getoption("--v4-inventory")
    if path:
        path.write_text(
            json.dumps({"group": group, "cases": inventory}, indent=2) + "\n"
        )
    if not selected:
        raise pytest.UsageError("No audited v4 cases selected")


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    config = item.config
    group = config.getoption("--v4-only-group")
    if not group:
        return
    if exclusion(item, group):
        raise pytest.UsageError("Unaudited case reached v4-only setup")
    if not config._v4_worker_checked:
        response = requests.get(
            f"http://{config.getoption('--kafka-connect-address')}/connector-plugins",
            timeout=15,
        )
        response.raise_for_status()
        version = validate_plugins(response.json())
        artifact = config.getoption("--v4-artifact")
        if (
            artifact is None
            or json.loads(artifact.read_text()).get("version") != version
        ):
            raise pytest.UsageError(
                "Worker version does not match the inspected build artifact"
            )
        print(f"KC v4 qualification worker artifact: {version}")
        config._v4_worker_checked = True
