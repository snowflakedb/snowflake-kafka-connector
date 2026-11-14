"""TEST ONLY: offline tests for publish_image.sh with fake docker and snow."""

from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]


class PublishImageTests(unittest.TestCase):
    def test_publish_image_prints_pinned_path_and_always_logs_out(self):
        registry = "org-acct.registry.snowflakecomputing.com"
        repository = "testdb_kafka/kc_spcs_ci/images/kc-spcs-smoke"
        digest = "sha256:" + "a" * 64
        # Read as text: importing these needs the Snowflake connector.
        setup = (ROOT / "test/spcs/setup_ci.py").read_text()
        for source in (
            'DATABASE = "TESTDB_KAFKA"',
            'SCHEMA = f"{DATABASE}.KC_SPCS_CI"',
            'f"{SCHEMA}.IMAGES"',
        ):
            self.assertIn(source, setup)
        image_pattern = r'r"/[A-Za-z0-9_./-]+@sha256:[a-f0-9]{64}"'
        runner = (ROOT / "test/spcs/run_spcs_release.py").read_text()
        self.assertIn(f"re.fullmatch({image_pattern}, args.image)", runner)
        self.assertRegex(f"/{repository}@{digest}", f"^{image_pattern[2:-1]}$")
        for case, push_exit, inspected, expected_exit, expected_out in (
            (
                "ok",
                0,
                f"{registry}/{repository}@{digest}",
                0,
                f"/{repository}@{digest}\n",
            ),
            ("other repository", 0, f"{registry}/other/repo@{digest}", 1, ""),
            ("push fails", 4, "", 4, ""),
        ):
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "test/spcs").mkdir(parents=True)
                shutil.copy(ROOT / "test/spcs/publish_image.sh", root / "test/spcs")
                binary = root / "bin"
                binary.mkdir()
                (binary / "snow").write_text(
                    '#!/bin/sh\necho "snow $*" >> "$LOG"\n'
                    f'[ "$3" = url ] && echo {registry} && exit 0\n'
                    "echo Login Succeeded\n"
                )
                # Like the real tools, build, push and login print progress to
                # stdout, which must not reach the captured image path.
                (binary / "docker").write_text(
                    '#!/bin/sh\necho "docker $*" >> "$LOG"\n'
                    '[ "$1" = inspect ] && echo "$INSPECTED" && exit 0\n'
                    "echo progress\n"
                    '[ "$1" = push ] && exit "$PUSH_EXIT"\nexit 0\n'
                )
                for command in binary.iterdir():
                    command.chmod(0o700)
                log = root / "log"
                result = subprocess.run(
                    [str(root / "test/spcs/publish_image.sh"), "abc123"],
                    capture_output=True,
                    text=True,
                    timeout=10,
                    env={
                        "PATH": f"{binary}:/usr/bin:/bin",
                        "LOG": str(log),
                        "PUSH_EXIT": str(push_exit),
                        "INSPECTED": inspected,
                    },
                )
                self.assertEqual(result.returncode, expected_exit, result.stderr)
                self.assertEqual(result.stdout, expected_out)
                calls = log.read_text().splitlines()
                self.assertEqual(calls[-1], f"docker logout {registry}")
                self.assertIn(f"docker push {registry}/{repository}:abc123", calls)
                self.assertIn("snow spcs image-registry login -c spcs", calls)


if __name__ == "__main__":
    unittest.main()
