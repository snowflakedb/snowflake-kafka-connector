"""Regression checks for the optional observer connection (run inside test image)."""

import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import MagicMock, Mock, patch

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from lib.driver import KafkaDriver


class ObserverTests(unittest.TestCase):
    def make_driver(self, **kwargs):
        return KafkaDriver(
            "localhost:9092", "", "localhost:8083", Mock(), "4.1.1", False, **kwargs
        )

    @patch("lib.driver.Consumer")
    @patch("lib.driver.Producer")
    @patch("lib.driver.AdminClient")
    def test_default_still_builds_private_key_connection(self, *_):
        with (
            patch("lib.driver.SnowflakeConnectorConfig.from_profile") as profile,
            patch("lib.driver.snowflake.connector.connect") as connect,
        ):
            profile.return_value.to_dict.return_value = {"private_key": b"fake"}
            instance = self.make_driver()
            connect.assert_called_once_with(private_key=b"fake")
            self.assertIs(instance.snowflake_conn, connect.return_value)

    @patch("lib.driver.Consumer")
    @patch("lib.driver.Producer")
    @patch("lib.driver.AdminClient")
    def test_supplied_observer_never_loads_private_key(self, *_):
        observer = Mock()
        with (
            patch("lib.driver.SnowflakeConnectorConfig.from_profile") as profile,
            patch("lib.driver.snowflake.connector.connect") as connect,
        ):
            instance = self.make_driver(snowflake_connection=observer)
            self.assertIs(instance.snowflake_conn, observer)
            profile.assert_not_called()
            connect.assert_not_called()

    def test_smoke_uses_ambient_role_without_supplied_credentials(self):
        path = Path(__file__).resolve().parents[1] / "tests/spcs/test_spcs_ingestion.py"
        spec = importlib.util.spec_from_file_location("spcs_smoke", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        driver = MagicMock()
        driver.snowflake_conn.cursor.return_value.__enter__.return_value.fetchone.return_value = None
        cursor = driver.snowflake_conn.cursor.return_value
        cursor.execute.return_value.fetchone.return_value = ("SERVICE_OWNER",)
        cursor.execute.return_value.fetchall.return_value = [
            (str(value),) for value in range(1, 101)
        ]
        create_connector = Mock()
        with patch.object(module, "RecordProducer"):
            module.test_spcs_ingestion("v4", driver, "_test", create_connector, Mock())
        config = create_connector.call_args.kwargs["v4_config"]
        self.assertEqual(config["snowflake.authenticator"], "spcs")
        self.assertEqual(config["snowflake.role.name"], "SERVICE_OWNER")
        self.assertFalse(
            {
                "snowflake.user.name",
                "snowflake.private.key",
                "snowflake.oauth.refresh.token",
            }
            & config.keys()
        )

    def test_missing_table_fails_before_connector_creation(self):
        self.check_invalid_fixture(error=RuntimeError("credentials=do-not-print"))

    def test_nonempty_table_fails_before_connector_creation(self):
        self.check_invalid_fixture(row=({}, "1"))

    def check_invalid_fixture(self, *, error=None, row=None):
        path = Path(__file__).resolve().parents[1] / "tests/spcs/test_spcs_ingestion.py"
        spec = importlib.util.spec_from_file_location("spcs_smoke", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        driver = MagicMock()
        cursor = driver.snowflake_conn.cursor.return_value.__enter__.return_value
        cursor.execute.side_effect = error
        cursor.fetchone.return_value = row
        create_connector = Mock()
        with patch.object(module, "RecordProducer") as producer:
            with self.assertRaises(pytest.fail.Exception) as failure:
                module.test_spcs_ingestion(
                    "v4", driver, "_test", create_connector, Mock()
                )
            self.assertIn("SMOKE_ROWS", str(failure.exception))
            self.assertNotIn("do-not-print", str(failure.exception))
            create_connector.assert_not_called()
            producer.assert_not_called()


if __name__ == "__main__":
    unittest.main()
