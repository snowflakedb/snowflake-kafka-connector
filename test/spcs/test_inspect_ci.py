"""Read-only capability probe contracts; no credential or network access."""

from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parent))
import inspect_ci  # noqa: E402


class InspectTests(unittest.TestCase):
    def test_role_hierarchy_and_public_are_inspected_without_mutations(self):
        statements = []

        def execute(connection, statement):
            statements.append(statement)
            if statement.startswith("SELECT"):
                return [{"account": "PRIVATE", "role": "CI_ROLE", "database": "TESTDB"}]
            if statement == 'SHOW GRANTS TO ROLE "CI_ROLE"':
                return [{"granted_on": "ROLE", "privilege": "USAGE", "name": "CHILD"}]
            if statement == 'SHOW GRANTS TO ROLE "CHILD"':
                return [
                    {"granted_on": "ROLE", "privilege": "USAGE", "name": "CI_ROLE"},
                    {
                        "granted_on": "DATABASE",
                        "privilege": "OWNERSHIP",
                        "name": "TESTDB",
                    },
                    {
                        "granted_on": "ACCOUNT",
                        "privilege": "CREATE COMPUTE POOL",
                        "name": "PRIVATE",
                    },
                ]
            return []

        with patch.object(inspect_ci, "query", side_effect=execute):
            result = inspect_ci.inspect(
                None, SimpleNamespace(role="CI_ROLE", database="TESTDB")
            )
        self.assertEqual(result["roles_inspected"], 3)
        self.assertTrue(result["database_owned_by_role_hierarchy"])
        self.assertTrue(result["account_create_compute_pool_granted"])
        self.assertTrue(
            all(statement.startswith(("SELECT", "SHOW")) for statement in statements)
        )
        self.assertNotIn("PRIVATE", str(result))
        self.assertNotIn("CHILD", str(result))

    def test_wrong_role_stops_before_grant_inspection(self):
        with patch.object(
            inspect_ci,
            "query",
            return_value=[{"account": "A", "role": "OTHER", "database": "DB"}],
        ) as query:
            with self.assertRaises(ValueError):
                inspect_ci.inspect(None, SimpleNamespace(role="CI_ROLE", database="DB"))
            self.assertEqual(query.call_count, 1)

    def test_identifier_is_quoted_not_executable_text(self):
        self.assertEqual(
            inspect_ci.quoted('role"; DROP ROLE x; --'), '"role""; DROP ROLE x; --"'
        )
        with self.assertRaises(ValueError):
            inspect_ci.quoted(None)


if __name__ == "__main__":
    unittest.main()
