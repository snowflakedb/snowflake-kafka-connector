from test_suit.test_utils import RetryableError, NonRetryableError
import json
from test_suit.base_iceberg_test import BaseIcebergTest


class TestIcebergJsonAws(BaseIcebergTest):
    def __init__(self, driver, name_salt: str):
        BaseIcebergTest.__init__(self, driver, name_salt, "iceberg_json_aws")


    def setup(self):
        self.driver.create_iceberg_table_with_sample_content(
            table_name=self.topic,
            external_volume="kafka_push_e2e_volume_aws",  # volume created manually
        )


    def send(self):
        self._send_json_values(self.test_message_from_docs, 100)


    def verify(self, round):
        self._assert_number_of_records_in_table(100)

        first_record = (
            self.driver.snowflake_conn.cursor()
            .execute("Select * from {} limit 1".format(self.topic))
            .fetchone()
        )

        self._verify_iceberg_content_from_docs(json.loads(first_record[0]))
        self._verify_iceberg_metadata(json.loads(first_record[1]))
