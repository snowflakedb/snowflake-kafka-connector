from test_suit.base_iceberg_test import BaseIcebergTest


class TestIcebergSchemaEvolutionAvroAws(BaseIcebergTest):
    def __init__(self, driver, name_salt: str):
        BaseIcebergTest.__init__(self, driver, name_salt, "iceberg_schema_evolution_avro_aws")


    def setup(self):
        self.driver.create_empty_iceberg_table(
            table_name=self.topic,
            external_volume="kafka_push_e2e_volume_aws",  # volume created manually
        )
        self.driver.enable_schema_evolution_for_iceberg(self.topic)


    def send(self):
        self._send_avro_messages(self.test_message_from_docs, self.test_message_from_docs_schema)
        self._send_avro_messages(self.test_message_for_schema_evolution_1, self.test_message_for_schema_evolution_schema)
        self._send_avro_messages(self.test_message_for_schema_evolution_2, self.test_message_for_schema_evolution_schema)


    def verify(self, round):
        self._assert_number_of_records_in_table(300)

        actual_record_from_docs_dict = self._select_schematized_record_with_offset(1)
        self._verify_iceberg_content_from_docs(actual_record_from_docs_dict)

        actual_record_for_schema_evolution_1 = self._select_schematized_record_with_offset(100)
        self._verify_iceberg_content_for_schema_evolution_1(actual_record_for_schema_evolution_1)

        actual_record_for_schema_evolution_2 = self._select_schematized_record_with_offset(200)
        self._verify_iceberg_content_for_schema_evolution_2(actual_record_for_schema_evolution_2)