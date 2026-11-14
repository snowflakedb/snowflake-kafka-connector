"""Offline selection and connector-creation guards. No credentials required."""

import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
import zipfile
from unittest.mock import Mock, patch

TEST = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TEST))
from lib import v4_qualification as qualification  # noqa: E402
from lib.driver import KafkaDriver  # noqa: E402
from lib.config_migration import V4_CONNECTOR_CLASS  # noqa: E402

sys.path.insert(0, str(TEST / "scripts"))
import qualification_artifact  # noqa: E402


class QualificationTests(unittest.TestCase):
    def item(self, relative="test_string_json.py", **params):
        return SimpleNamespace(
            path=TEST / "tests" / relative,
            callspec=SimpleNamespace(params=params),
            get_closest_marker=lambda marker: None,
        )

    def test_audited_v4_selected(self):
        self.assertIsNone(
            qualification.exclusion(self.item(connector_version="v4"), "core")
        )

    def test_unclassified_and_legacy_rejected(self):
        for params in ({}, {"connector_version": "v3"}, {"connector_version": "v5"}):
            self.assertIsNotNone(qualification.exclusion(self.item(**params), "core"))

    def test_hidden_v3_migration_rejected_even_when_labeled_v4(self):
        self.assertIsNotNone(
            qualification.exclusion(
                self.item("compatibility/test_migration.py", connector_version="v4"),
                "compatibility",
            )
        )

    def test_both_v4_compatibility_modes_selected(self):
        for mode in ("v4-compat", "v4-ht"):
            self.assertIsNone(
                qualification.exclusion(
                    self.item(
                        "compatibility/test_type_compatibility.py", ingestion_mode=mode
                    ),
                    "compatibility",
                )
            )

    def test_worker_rejects_stale_legacy_unknown_and_duplicate_plugins(self):
        good = {"class": V4_CONNECTOR_CLASS, "version": "4.2.1"}
        self.assertEqual(qualification.validate_plugins([good]), "4.2.1")
        for plugins in (
            [],
            [good, good],
            [
                good,
                {
                    "class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
                    "version": "3.5.0",
                },
            ],
            [{**good, "version": "unknown"}],
            [{**good, "version": "3.5.0"}],
        ):
            with self.subTest(plugins=plugins), self.assertRaises(RuntimeError):
                qualification.validate_plugins(plugins)

    def test_custom_and_transformed_legacy_configs_fail_before_http(self):
        driver = object.__new__(KafkaDriver)
        driver.credentials = SimpleNamespace(
            make_url=lambda: "unused",
            database="DB",
            schema="S",
            user="U",
            role="R",
            private_key="not-a-key",
        )
        driver.schemaRegistryAddress = "unused"
        with (
            patch.dict(os.environ, {"KC_V4_ONLY": "1"}),
            patch("lib.driver.requests.delete") as delete,
            patch("lib.driver.requests.post") as post,
        ):
            with self.assertRaisesRegex(RuntimeError, "refuses"):
                driver.createConnector(
                    "_salt",
                    unsalted_name="test",
                    config_template={
                        "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector"
                    },
                )
            delete.assert_not_called()
            post.assert_not_called()

    def test_configuration_guard(self):
        qualification.require_v4_config({"connector.class": V4_CONNECTOR_CLASS})
        for config in ({}, {"connector.class": "SnowflakeSinkConnector"}):
            with self.assertRaises(RuntimeError):
                qualification.require_v4_config(config)

    def test_runner_rejects_overrides_before_credentials(self):
        for extra in (
            ["--", "-k", "v3"],
            ["--v4-trial"],
            ["--keep"],
            ["--interactive"],
            ["--with-mitmproxy"],
        ):
            result = subprocess.run(
                ["bash", str(TEST / "run_tests.sh"), "--v4-only-group=core", *extra],
                capture_output=True,
                text=True,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("rejects", result.stderr)

    def test_real_collection_all_groups(self):
        with tempfile.TemporaryDirectory() as directory:
            for group in qualification.GROUPS:
                inventory = Path(directory) / f"{group}.json"
                result = subprocess.run(
                    [
                        sys.executable,
                        "-m",
                        "pytest",
                        "tests",
                        "--collect-only",
                        "-q",
                        "-p",
                        "no:cacheprovider",
                        "--v4-only-group",
                        group,
                        "--v4-inventory",
                        str(inventory),
                        "--kafka-address",
                        "localhost:9092",
                        "--kafka-connect-address",
                        "localhost:8083",
                        "--platform",
                        "apache",
                        "--platform-version",
                        "4.1.1",
                    ],
                    cwd=TEST,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                cases = json.loads(inventory.read_text())["cases"]
                selected = [case["nodeid"] for case in cases if case["selected"]]
                self.assertTrue(selected)
                self.assertTrue(
                    all(
                        "v3" not in node and "test_migration" not in node
                        for node in selected
                    )
                )
                print(f"{group}: {len(selected)} selected / {len(cases)} collected")

    def test_artifact_rejects_legacy_and_duplicate_bundles(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "kc.jar"

            def make_jar(target, version="4.2.1", legacy=False):
                with zipfile.ZipFile(target, "w") as archive:
                    archive.writestr(qualification_artifact.V4, b"class")
                    archive.writestr(
                        qualification_artifact.METADATA, f"version={version}\n"
                    )
                    if legacy:
                        archive.writestr(qualification_artifact.V3, b"legacy")

            make_jar(path)
            self.assertEqual(
                qualification_artifact.inspect(directory)["version"], "4.2.1"
            )
            for version, legacy in (
                ("3.5.0", False),
                ("unknown", False),
                ("4.2.1", True),
            ):
                make_jar(path, version, legacy)
                with self.assertRaises(ValueError):
                    qualification_artifact.inspect(directory)
            make_jar(path)
            make_jar(Path(directory) / "stale.jar")
            with self.assertRaises(ValueError):
                qualification_artifact.inspect(directory)

    def test_all_skipped_is_not_qualified(self):
        config = Mock()
        config.getoption.side_effect = lambda name: (
            "core" if name == "--v4-only-group" else False
        )
        for passed, expected in ((0, 1), (1, 0)):
            config._v4_passed = passed
            session = SimpleNamespace(config=config, exitstatus=0)
            qualification.pytest_sessionfinish(session, 0)
            self.assertEqual(session.exitstatus, expected)

    def test_worker_version_must_match_artifact(self):
        item = self.item(connector_version="v4")
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory) / "artifact.json"
            artifact.write_text(json.dumps({"version": "4.2.1"}))
            config = Mock(_v4_worker_checked=False)
            values = {
                "--v4-only-group": "core",
                "--v4-artifact": artifact,
                "--kafka-connect-address": "localhost:8083",
            }
            config.getoption.side_effect = values.get
            item.config = config
            response = Mock()
            response.json.return_value = [
                {"class": V4_CONNECTOR_CLASS, "version": "4.2.0"}
            ]
            import pytest

            with patch.object(qualification.requests, "get", return_value=response):
                with self.assertRaises(pytest.UsageError):
                    qualification.pytest_runtest_setup(item)
            self.assertFalse(config._v4_worker_checked)

    def test_empty_selection_fails(self):
        config = Mock()
        config.getoption.side_effect = lambda name: (
            "core" if name == "--v4-only-group" else None
        )
        import pytest

        with self.assertRaises(pytest.UsageError):
            qualification.pytest_collection_modifyitems(config, [])


if __name__ == "__main__":
    unittest.main()
