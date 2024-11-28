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
        msg = json.dumps(self.test_message)

        key = []
        value = []
        for e in range(100):
            key.append(json.dumps({"number": str(e)}).encode("utf-8"))
            value.append(msg.encode("utf-8"))

        self.driver.sendBytesData(
            topic=self.topic,
            value=value,
            key=key,
            partition=0,
            headers=self.test_headers,
        )


    def verify(self, round):
        number_of_records = self.driver.select_number_of_records(self.topic)
        if number_of_records == 0:
            raise RetryableError()
        elif number_of_records != 100:
            raise NonRetryableError(
                "Number of record in table is different from number of record sent"
            )

        first_record = (
            self.driver.snowflake_conn.cursor()
            .execute("Select * from {} limit 1".format(self.topic))
            .fetchone()
        )

        self.verify_iceberg_content(json.loads(first_record[0]))
        self.verify_iceberg_metadata(json.loads(first_record[1]))
