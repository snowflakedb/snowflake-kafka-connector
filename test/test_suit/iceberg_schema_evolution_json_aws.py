from test_suit.base_iceberg_test import BaseIcebergTest


class TestIcebergSchemaEvolutionJsonAws(BaseIcebergTest):
    def __init__(self, driver, name_salt: str):
        BaseIcebergTest.__init__(self, driver, name_salt, "iceberg_schema_evolution_json_aws")


    def setup(self):
        self.driver.create_empty_iceberg_table(
            table_name=self.topic,
            external_volume="kafka_push_e2e_volume_aws",  # volume created manually
        )


    def send(self):
        self._send_json_values(self.test_message_from_docs, 100)
        self._send_json_values(self.test_message_for_schema_evolution_1, 100)
        self._send_json_values(self.test_message_for_schema_evolution_2, 100)


    def verify(self, round):
        self._assert_number_of_records_in_table(300)

