"""TEST ONLY: offline tests for setup_ci.py using a fake cursor (no Snowflake access)."""

from contextlib import redirect_stdout
import io
from pathlib import Path
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import setup_ci  # noqa: E402

MARKER = "Kafka Connector SPCS validation: permanent CI infrastructure"
SHOW_TO_NAME = {
    "SHOW COMPUTE POOLS LIKE 'KC_SPCS_CI_POOL'": "KC_SPCS_CI_POOL",
    "SHOW SCHEMAS LIKE 'KC_SPCS_CI' IN DATABASE TESTDB_KAFKA": "KC_SPCS_CI",
    "SHOW STAGES LIKE 'HARNESS_STAGE' IN SCHEMA TESTDB_KAFKA.KC_SPCS_CI": "HARNESS_STAGE",
    "SHOW IMAGE REPOSITORIES LIKE 'IMAGES' IN SCHEMA TESTDB_KAFKA.KC_SPCS_CI": "IMAGES",
}


class FakeCursor:
    def __init__(self, existing=(), account="SFCTEST0", owner="TESTROLE_KAFKA"):
        self.objects = {name: owner for name in existing}
        self.account = account
        self.statements = []

    def execute(self, sql):
        self.statements.append(sql)
        if sql.startswith("SELECT"):
            self.result = [("A",), ("R",)], [(self.account, "TESTROLE_KAFKA")]
        elif sql in SHOW_TO_NAME:
            name = SHOW_TO_NAME[sql]
            # Simulate LIKE wildcard noise: a near-match row must be ignored.
            data = [(name + "X", "OTHER", "")]
            if name in self.objects:
                data.append((name, self.objects[name], MARKER))
            self.result = [("name",), ("owner",), ("comment",)], data
        elif sql.startswith("CREATE"):
            name = sql.split(" IF NOT EXISTS ")[1].split()[0].rsplit(".", 1)[-1]
            self.objects.setdefault(name, "TESTROLE_KAFKA")
            self.result = [("status",)], []
        else:
            raise AssertionError(f"unexpected SQL: {sql}")

    @property
    def description(self):
        return self.result[0]

    def fetchall(self):
        return self.result[1]


def run(cursor, apply):
    with redirect_stdout(io.StringIO()):
        return setup_ci.setup(cursor, apply)


class SetupTest(unittest.TestCase):
    def test_check_is_read_only(self):
        cursor = FakeCursor()
        self.assertEqual(run(cursor, apply=False), 4)
        self.assertFalse(any(s.startswith("CREATE") for s in cursor.statements))

    def test_apply_creates_pool_first_with_one_node_limit(self):
        cursor = FakeCursor()
        self.assertEqual(run(cursor, apply=True), 0)
        creates = [s for s in cursor.statements if s.startswith("CREATE")]
        self.assertEqual(len(creates), 4)
        self.assertTrue(creates[0].startswith("CREATE COMPUTE POOL IF NOT EXISTS"))
        self.assertIn("MIN_NODES=1 MAX_NODES=1", creates[0])
        self.assertIn("INITIALLY_SUSPENDED=TRUE", creates[0])
        self.assertTrue(all(f"COMMENT='{MARKER}'" in s for s in creates))

    def test_rerun_creates_nothing(self):
        cursor = FakeCursor(SHOW_TO_NAME.values())
        self.assertEqual(run(cursor, apply=True), 0)
        self.assertFalse(any(s.startswith("CREATE") for s in cursor.statements))

    def test_never_alters_drops_or_grants(self):
        cursor = FakeCursor()
        run(cursor, apply=True)
        for verb in ("ALTER", "DROP", "GRANT", "REPLACE"):
            self.assertFalse(any(verb in s for s in cursor.statements), verb)

    def test_rejects_wrong_account(self):
        with self.assertRaises(RuntimeError):
            run(FakeCursor(account="OTHER"), apply=True)

    def test_rejects_foreign_object(self):
        with self.assertRaises(RuntimeError):
            run(FakeCursor(["KC_SPCS_CI_POOL"], owner="SOMEONE_ELSE"), apply=True)


if __name__ == "__main__":
    unittest.main()
