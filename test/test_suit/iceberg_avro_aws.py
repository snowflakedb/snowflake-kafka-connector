from test_suit.test_utils import RetryableError, NonRetryableError
import json
from confluent_kafka import avro
from test_suit.base_iceberg_test import BaseIcebergTest


class TestIcebergAvroAws(BaseIcebergTest):
    def __init__(self, driver, nameSalt: str):
        BaseIcebergTest.__init__(self, driver, nameSalt)
        self.fileName = "iceberg_avro_aws"
        self.topic = self.fileName + nameSalt

        valueSchemaStr = """
        {
            "type":"record",
            "name":"value_schema",
            "fields": [
                {
                  "name": "id",
                  "type": "int"
                },
                {
                  "name": "body_temperature",
                  "type": "float"
                },
                {
                  "name": "name",
                  "type": "string"
                },
                {
                  "name": "approved_coffee_types",
                  "type": {
                    "type": "array",
                    "items": "string"
                  }
                },
                {
                  "name": "animals_possessed",
                  "type": {
                    "type": "map",
                    "values": "boolean"
                  }
                }
            ]
        }
        """
        self.valueSchema = avro.loads(valueSchemaStr)

    def getConfigFileName(self):
        return self.fileName + ".json"

    def setup(self):
        self.driver.create_iceberg_table_with_content(
            table_name=self.topic,
            external_volume="kafka_push_e2e_volume_aws",  # volume created manually
        )

    def send(self):
        value = []

        for e in range(100):
            value.append(self.test_message)

        self.driver.sendAvroSRData(
            topic=self.topic,
            value=value,
            value_schema=self.valueSchema,
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

    def clean(self):
        self.driver.drop_iceberg_table(self.topic)
