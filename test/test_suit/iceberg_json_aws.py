import datetime

from test_suit.test_utils import RetryableError, NonRetryableError
import json
from time import sleep
from test_suit.base_e2e import BaseE2eTest


class TestIcebergJsonAws(BaseE2eTest):
    def __init__(self, driver, nameSalt: str):
        self.driver = driver
        self.fileName = "iceberg_json_aws"
        self.topic = self.fileName + nameSalt

    def getConfigFileName(self):
        return self.fileName + ".json"

    def setup(self):
        self.driver.create_iceberg_table_with_content(
            table_name=self.topic,
            external_volume="kafka_push_e2e_volume_aws",  # volume created manually
        )

    def send(self):
        msg = json.dumps(
            {
                "id": 1,
                "body_temperature": 36.6,
                "name": "Steve",
                "approved_coffee_types": ["Espresso", "Doppio", "Ristretto", "Lungo"],
                "animals_possessed": {"dogs": True, "cats": False},
            }
        )

    def verify(self, round):
        res = self.driver.select_number_of_records(self.topic)
        print("Count records in table {}={}".format(self.topic, str(res)))

    def clean(self):
        self.driver.drop_iceberg_table(self.topic)
        return
