"""Credential-free contracts for one-time setup and profile cleanup."""

from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[2]


def workflow(name):
    # BaseLoader keeps GitHub's `on` key a string, unlike YAML 1.1 safe_load.
    return yaml.load(
        (ROOT / ".github/workflows" / name).read_text(), Loader=yaml.BaseLoader
    )


class WorkflowTests(unittest.TestCase):
    def test_setup_contracts_and_approval_attachment(self):
        document = workflow("spcs-infrastructure.yml")
        self.assertEqual(set(document["on"]), {"pull_request", "workflow_dispatch"})
        self.assertEqual(document["permissions"], {"contents": "read"})
        action = document["on"]["workflow_dispatch"]["inputs"]["action"]
        self.assertEqual(action["default"], "check")
        self.assertEqual(action["options"], ["check", "apply"])
        contracts, setup = (document["jobs"][key] for key in ("contracts", "setup"))
        self.assertEqual(set(document["jobs"]), {"contracts", "setup"})
        self.assertNotIn("secrets.", str(contracts))
        self.assertNotIn("environment", contracts)
        self.assertEqual(setup["environment"], "kc-spcs-setup")
        self.assertEqual(setup["needs"], "contracts")
        self.assertLessEqual(int(setup["timeout-minutes"]), 10)
        self.assertEqual(
            setup["concurrency"],
            {"group": "spcs-release-account", "cancel-in-progress": "false"},
        )
        for restriction in (
            "workflow_dispatch",
            "refs/heads/master",
            "snowflakedb/snowflake-kafka-connector",
        ):
            self.assertIn(restriction, setup["if"])
        self.assertNotIn("upload-artifact", str(setup))
        self.assertNotIn("run_spcs_release", str(document))
        self.assertNotIn("build-connector", str(document))
        gate = setup["steps"][0]
        self.assertEqual(
            gate["env"], {"SETUP_ENABLED": "${{ vars.KC_SPCS_SETUP_ENABLED }}"}
        )
        for enabled in ("", "false", "true"):
            result = subprocess.run(
                ["/bin/bash", "-c", gate["run"]],
                env={"SETUP_ENABLED": enabled},
                check=False,
            )
            self.assertEqual(result.returncode == 0, enabled == "true")

    def test_setup_shell_cleanup_and_exit_propagation(self):
        setup = workflow("spcs-infrastructure.yml")["jobs"]["setup"]
        script = next(
            step["run"]
            for step in setup["steps"]
            if step.get("name") == "Check or create permanent resources"
        )
        for action, gpg_exit, mask_exit, python_exit, expected in (
            ("check", 0, 0, 0, 0),
            ("check", 0, 0, 3, 3),
            ("apply", 0, 0, 7, 7),
            ("check", 9, 0, 0, 9),
            ("check", 0, 8, 0, 8),
            ("invalid", 0, 0, 0, 2),
        ):
            with (
                self.subTest(action=action, gpg_exit=gpg_exit, python_exit=python_exit),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                helpers = root / ".github/scripts"
                helpers.mkdir(parents=True)
                shutil.copyfile(
                    ROOT / ".github/scripts/decrypt_secret.sh",
                    helpers / "decrypt_secret.sh",
                )
                binary = root / "bin"
                binary.mkdir()
                (binary / "gpg").write_text(
                    '#!/bin/sh\nprintf "{}" > profile.json\nprintf "PRIVATE_GPG_OUTPUT\\n" >&2\nexit "$GPG_EXIT"\n'
                )
                (binary / "python3").write_text(
                    '#!/bin/sh\nif [ "$1" = "-c" ]; then printf "PRIVATE_MASK_OUTPUT\\n"; exit "$MASK_EXIT"; fi\n'
                    'test -z "${SNOWFLAKE_TEST_PROFILE_SECRET+x}" || exit 91\n'
                    'test "$1" = test/spcs/setup_ci.py || exit 92\n'
                    'printf "%s" "$2" > invoked\nprintf "PRIVATE_CONNECTOR_WARNING\\n" >&2\nexit "$PYTHON_EXIT"\n'
                )
                for command in binary.iterdir():
                    command.chmod(0o700)
                result = subprocess.run(
                    ["/bin/bash", "-c", script],
                    cwd=root,
                    capture_output=True,
                    text=True,
                    timeout=10,
                    env={
                        "PATH": f"{binary}:/usr/bin:/bin",
                        "SETUP_ACTION": action,
                        "MASK_EXIT": str(mask_exit),
                        "GPG_EXIT": str(gpg_exit),
                        "PYTHON_EXIT": str(python_exit),
                        "SNOWFLAKE_TEST_PROFILE_SECRET": "synthetic-not-a-secret",
                    },
                )
                self.assertEqual(result.returncode, expected, result.stderr)
                self.assertFalse((root / "profile.json").exists())
                self.assertFalse((root / "setup.stderr").exists())
                self.assertNotIn("PRIVATE_", result.stdout + result.stderr)
                if action != "invalid" and not gpg_exit and not mask_exit:
                    self.assertEqual((root / "invoked").read_text(), action)
                else:
                    self.assertFalse((root / "invoked").exists())


if __name__ == "__main__":
    unittest.main()
