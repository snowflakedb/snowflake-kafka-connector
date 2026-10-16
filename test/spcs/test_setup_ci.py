"""Permanent infrastructure contracts; no credentials or Snowflake connection."""

from contextlib import redirect_stdout
import copy
import io
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import yaml

sys.path.insert(0, str(Path(__file__).resolve().parent))
import setup_ci as setup  # noqa: E402


class Catalog:
    def __init__(self):
        self.statements = []
        self.objects = {}
        self.fail_kind = None
        self.identity = {
            "account": setup.ACCOUNT,
            "role": setup.ROLE,
            "region": "AWS_US_WEST_2",
        }
        self.families = [
            {"name": "CPU_X64_XS", "memory_gib": 7, "vcpu": 3, "gpu_count": 0}
        ]
        self.names = {
            "pool": setup.POOL,
            "schema": setup.SCHEMA,
            "stage": "HARNESS_STAGE",
            "repository": "IMAGES",
        }

    def row(self, kind):
        row = {"name": self.names[kind], "owner": setup.ROLE, "comment": setup.MARKER}
        if kind == "pool":
            row.update(
                min_nodes=1,
                max_nodes=1,
                instance_family="CPU_X64_XS",
                auto_resume=True,
                auto_suspend_secs=300,
                state="SUSPENDED",
                num_jobs=0,
                num_services=0,
                backup_instance_families=None,
            )
        if kind == "stage":
            row["type"] = "INTERNAL NO CSE"
        return row

    def execute(self, connection, statement):
        self.statements.append(statement)
        if statement.startswith("SELECT CURRENT_ACCOUNT"):
            return [self.identity]
        if statement == "SHOW COMPUTE POOL INSTANCE FAMILIES":
            return self.families
        for token, kind in (
            ("COMPUTE POOL", "pool"),
            ("SCHEMA", "schema"),
            ("STAGE", "stage"),
            ("IMAGE REPOSITOR", "repository"),
        ):
            if statement.startswith(f"SHOW {token}"):
                return copy.deepcopy(self.objects.get(kind, []))
            if statement.startswith(f"CREATE {token}"):
                if kind == self.fail_kind:
                    raise setup.snowflake.connector.ProgrammingError(
                        msg="PRIVATE_ERROR_TEXT", errno=3001
                    )
                if kind in self.objects:
                    raise AssertionError("Attempted to recreate an existing object")
                self.objects[kind] = [self.row(kind)]
                return []
        if statement == "USE SECONDARY ROLES NONE":
            return []
        raise AssertionError(f"Unexpected SQL: {statement}")

    def creates(self):
        return [
            statement for statement in self.statements if statement.startswith("CREATE")
        ]


class SetupTests(unittest.TestCase):
    def setUp(self):
        self.catalog = Catalog()
        self.report = {"setup_complete": False, "resources": {}}
        self.query = patch.object(setup, "query", side_effect=self.catalog.execute)
        self.query.start()
        self.addCleanup(self.query.stop)
        output = redirect_stdout(io.StringIO())
        output.__enter__()
        self.addCleanup(output.__exit__, None, None, None)

    def run_setup(self):
        setup.setup(None, self.report)

    def test_creates_once_and_rechecks_on_repeat(self):
        self.run_setup()
        self.assertTrue(self.report["setup_complete"])
        self.assertEqual(len(self.catalog.creates()), 4)
        self.assertIn("INITIALLY_SUSPENDED=TRUE", self.catalog.creates()[0])
        self.assertIn("MAX_NODES=1", self.catalog.creates()[0])
        self.assertIn("ENCRYPTION=(TYPE='SNOWFLAKE_SSE')", self.catalog.creates()[2])
        self.catalog.statements.clear()
        self.run_setup()
        self.assertEqual(self.catalog.creates(), [])
        self.assertEqual(set(self.report["resources"].values()), {"verified"})
        self.assertTrue(self.report["image_publication_required"])

    def test_wrong_identity_stops_before_inventory_or_creation(self):
        for key in self.catalog.identity:
            with self.subTest(key=key):
                original = self.catalog.identity[key]
                self.catalog.identity[key] = "WRONG"
                self.catalog.statements.clear()
                with self.assertRaises(ValueError):
                    self.run_setup()
                self.assertEqual(len(self.catalog.statements), 1)
                self.catalog.identity[key] = original

    def test_collision_or_missing_metadata_never_adopted(self):
        for kind in self.catalog.names:
            for field in ("owner", "comment"):
                with self.subTest(kind=kind, field=field):
                    self.catalog.objects = {
                        name: [self.catalog.row(name)] for name in self.catalog.names
                    }
                    self.catalog.objects[kind][0][field] = "UNRELATED"
                    self.catalog.statements.clear()
                    with self.assertRaises(ValueError):
                        self.run_setup()
                    self.assertFalse(self.catalog.creates())

    def test_pool_drift_or_activity_is_not_repaired(self):
        changes = {
            "min_nodes": 2,
            "max_nodes": 2,
            "instance_family": "GPU_NV_S",
            "auto_resume": False,
            "auto_suspend_secs": 3600,
            "state": "ACTIVE",
            "num_jobs": 1,
            "num_services": 1,
            "backup_instance_families": "[GEN_X64_G2_8]",
        }
        for field, value in changes.items():
            with self.subTest(field=field):
                self.catalog.objects = {"pool": [self.catalog.row("pool")]}
                self.catalog.objects["pool"][0][field] = value
                self.catalog.statements.clear()
                with self.assertRaises(ValueError):
                    self.run_setup()
                self.assertFalse(self.catalog.creates())
                self.assertTrue(
                    all(
                        s.startswith(("SELECT", "SHOW"))
                        for s in self.catalog.statements
                    )
                )

    def test_insufficient_memory_or_unapproved_family_stops_before_ddl(self):
        for row in (
            {"name": "CPU_X64_XS", "memory_gib": 6, "vcpu": 3, "gpu_count": 0},
            {"name": "GPU_NV_S", "memory_gib": 64, "vcpu": 8, "gpu_count": 1},
        ):
            with self.subTest(row=row):
                self.catalog.families = [row]
                with self.assertRaises(ValueError):
                    self.run_setup()
                self.assertFalse(self.catalog.creates())

    def test_absent_pool_privilege_does_not_create_schema_objects(self):
        self.catalog.fail_kind = "pool"
        with self.assertRaises(setup.snowflake.connector.Error):
            self.run_setup()
        self.assertEqual(self.catalog.objects, {})
        self.assertEqual(self.report["resources"], {"pool": "create_submitted"})

    def test_partial_creation_can_be_resumed_without_replacement(self):
        self.catalog.fail_kind = "repository"
        with self.assertRaises(setup.snowflake.connector.Error):
            self.run_setup()
        self.assertEqual(set(self.catalog.objects), {"pool", "schema", "stage"})
        self.assertFalse(self.report["setup_complete"])
        self.assertEqual(self.report["resources"]["repository"], "create_submitted")
        self.catalog.fail_kind = None
        self.catalog.statements.clear()
        self.run_setup()
        self.assertEqual(len(self.catalog.creates()), 1)
        self.assertTrue(self.report["setup_complete"])

    def test_lost_create_response_rechecks_on_next_run(self):
        def lost_response(connection, statement):
            result = self.catalog.execute(connection, statement)
            if statement.startswith("CREATE STAGE"):
                raise TimeoutError("private transport error")
            return result

        with patch.object(setup, "query", side_effect=lost_response):
            with self.assertRaises(TimeoutError):
                self.run_setup()
        self.assertIn("stage", self.catalog.objects)
        self.assertEqual(self.report["resources"]["stage"], "create_submitted")
        self.catalog.statements.clear()
        self.run_setup()
        self.assertEqual(len(self.catalog.creates()), 1)

    def test_stage_must_be_permanent_and_server_side_encrypted(self):
        for stage_type in ("INTERNAL", "EXTERNAL", "INTERNAL TEMPORARY", None):
            with self.subTest(stage_type=stage_type):
                self.catalog.objects = {
                    name: [self.catalog.row(name)] for name in self.catalog.names
                }
                self.catalog.objects["stage"][0]["type"] = stage_type
                self.catalog.statements.clear()
                with self.assertRaises(ValueError):
                    self.run_setup()
                self.assertFalse(self.catalog.creates())

    def test_like_matches_are_filtered_by_exact_name(self):
        row = self.catalog.row("pool")
        row["name"] = "KCXSPCSXCI_POOL"
        self.catalog.objects["pool"] = [row, self.catalog.row("pool")]
        self.run_setup()
        self.assertFalse(
            any(s.startswith("CREATE COMPUTE POOL") for s in self.catalog.creates())
        )

    def test_no_replace_drop_grant_resume_or_job_statements(self):
        self.run_setup()
        for statement in self.catalog.statements:
            self.assertTrue(statement.startswith(("SELECT", "SHOW", "CREATE")))
            self.assertNotIn("OR REPLACE", statement)
            self.assertNotIn("IF NOT EXISTS", statement)
            self.assertNotIn("EXECUTE JOB", statement)


class DispatchTests(unittest.TestCase):
    ENV = {
        "GITHUB_ACTIONS": "true",
        "GITHUB_EVENT_NAME": "workflow_dispatch",
        "GITHUB_REF": "refs/heads/master",
        "GITHUB_REPOSITORY": "snowflakedb/snowflake-kafka-connector",
        "GITHUB_WORKFLOW_REF": setup.WORKFLOW,
        "KC_SPCS_CONFIRM_SETUP": "true",
        "SNOWFLAKE_CREDENTIAL_FILE": "/unused/profile.json",
    }

    def test_each_dispatch_guard_fails_before_profile_or_connection(self):
        for key in self.ENV.keys() - {"SNOWFLAKE_CREDENTIAL_FILE"}:
            with (
                self.subTest(key=key),
                patch.dict(os.environ, {**self.ENV, key: "WRONG"}, clear=True),
                patch.object(setup.Profile, "load") as load,
                patch.object(setup.snowflake.connector, "connect") as connect,
                redirect_stdout(io.StringIO()) as output,
            ):
                self.assertEqual(setup.main(), 1)
                load.assert_not_called()
                connect.assert_not_called()
                self.assertEqual(
                    json.loads(output.getvalue())["phase"], "dispatch_guard"
                )

    def test_wrong_profile_rejected_before_connection(self):
        with (
            patch.dict(os.environ, self.ENV, clear=True),
            patch.object(
                setup.Profile,
                "load",
                return_value=SimpleNamespace(role="OTHER", database=setup.DATABASE),
            ),
            patch.object(setup.snowflake.connector, "connect") as connect,
            redirect_stdout(io.StringIO()),
        ):
            self.assertEqual(setup.main(), 1)
            connect.assert_not_called()

    def test_main_uses_existing_profile_and_disables_secondary_roles(self):
        catalog = Catalog()
        with (
            patch.dict(os.environ, self.ENV, clear=True),
            patch.object(
                setup.Profile,
                "load",
                return_value=SimpleNamespace(
                    role=setup.ROLE.lower(), database=setup.DATABASE.lower()
                ),
            ),
            patch.object(setup.SnowflakeConnectorConfig, "from_profile") as config,
            patch.object(setup.snowflake.connector, "connect") as connect,
            patch.object(setup, "query", side_effect=catalog.execute),
            redirect_stdout(io.StringIO()) as output,
        ):
            config.return_value.to_dict.return_value = {"private_key": b"PRIVATE"}
            self.assertEqual(setup.main(), 0)
            self.assertEqual(catalog.statements[0], "USE SECONDARY ROLES NONE")
            self.assertEqual(connect.call_args.kwargs["role"], setup.ROLE)
            self.assertNotIn("PRIVATE", output.getvalue())
            self.assertTrue(
                json.loads(output.getvalue().splitlines()[-1])["setup_complete"]
            )

    def test_error_report_retains_progress_without_raw_error_text(self):
        catalog = Catalog()
        catalog.fail_kind = "repository"
        with (
            patch.dict(os.environ, self.ENV, clear=True),
            patch.object(
                setup.Profile,
                "load",
                return_value=SimpleNamespace(role=setup.ROLE, database=setup.DATABASE),
            ),
            patch.object(setup.SnowflakeConnectorConfig, "from_profile") as config,
            patch.object(setup.snowflake.connector, "connect"),
            patch.object(setup, "query", side_effect=catalog.execute),
            redirect_stdout(io.StringIO()) as output,
        ):
            config.return_value.to_dict.return_value = {}
            self.assertEqual(setup.main(), 1)
            self.assertNotIn("PRIVATE_ERROR_TEXT", output.getvalue())
            report = json.loads(output.getvalue().splitlines()[-1])
            self.assertEqual(report["errno"], 3001)
            self.assertEqual(report["phase"], "repository")
            self.assertEqual(report["resources"]["pool"], "verified")
            self.assertFalse(report["setup_complete"])


class WorkflowTests(unittest.TestCase):
    def setUp(self):
        root = Path(__file__).resolve().parents[2]
        self.workflow = yaml.safe_load(
            (root / ".github/workflows/spcs-infrastructure.yml").read_text()
        )

    def test_only_offline_contracts_run_on_pull_requests(self):
        events = self.workflow.get("on", self.workflow.get(True))
        self.assertEqual(set(events), {"pull_request", "workflow_dispatch"})
        self.assertFalse(
            events["workflow_dispatch"]["inputs"]["confirm_setup"]["default"]
        )
        self.assertEqual(set(self.workflow["jobs"]), {"contracts", "setup"})
        self.assertNotIn("secrets.", str(self.workflow["jobs"]["contracts"]))
        self.assertEqual(self.workflow["jobs"]["setup"]["needs"], "contracts")
        self.assertEqual(
            self.workflow["jobs"]["setup"]["if"],
            "github.event_name == 'workflow_dispatch' && github.ref == 'refs/heads/master' && github.repository == 'snowflakedb/snowflake-kafka-connector' && inputs.confirm_setup",
        )

    def test_setup_reuses_profile_with_cleanup_and_no_artifact_upload(self):
        job = self.workflow["jobs"]["setup"]
        self.assertEqual(
            job["concurrency"],
            {"group": "spcs-release-account", "cancel-in-progress": False},
        )
        self.assertEqual(self.workflow["permissions"], {"contents": "read"})
        step = job["steps"][-1]
        self.assertEqual(
            step["env"]["KC_SPCS_CONFIRM_SETUP"], "${{ inputs.confirm_setup }}"
        )
        self.assertEqual(
            step["env"]["SNOWFLAKE_TEST_PROFILE_SECRET"],
            "${{ secrets.SNOWFLAKE_TEST_PROFILE_SECRET }}",
        )
        self.assertIn("trap 'rm -f profile.json' EXIT", step["run"])
        self.assertIn("decrypt_secret.sh AWS", step["run"])
        self.assertIn("unset SNOWFLAKE_TEST_PROFILE_SECRET", step["run"])
        self.assertNotIn("actions/upload-artifact", str(self.workflow))
        self.assertNotIn("run_spcs_release.py", str(self.workflow))


if __name__ == "__main__":
    unittest.main()
