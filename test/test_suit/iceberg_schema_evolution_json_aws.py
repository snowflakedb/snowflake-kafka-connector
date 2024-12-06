from test_suit.base_iceberg_test import BaseIcebergTest


class TestIcebergSchemaEvolutionJsonAws(BaseIcebergTest):
    def __init__(self, driver, name_salt: str):
        BaseIcebergTest.__init__(self, driver, name_salt, "iceberg_schema_evolution_json_aws")


    def setup(self):
        self.driver.create_empty_iceberg_table(
            table_name=self.topic,
            external_volume="kafka_push_e2e_volume_aws",  # volume created manually
        )
        self.driver.enable_schema_evolution_for_iceberg(self.topic)


    def send(self):
        self._send_json_values(self.test_message_from_docs, 100)
        # first 10 messages should be discarded due to lack of schema for null fields, but after schema evolution
        # coming from the next messages, offset should be reset and the messages should once again consumed and inserted
        self._send_json_values(self.test_message_for_schema_evolution_1, 10)
        # this message should be never inserted due to lack of schema for one extra null field
        self._send_json_values(self.test_message_for_schema_evolution_3, 10)
        self._send_json_values(self.test_message_for_schema_evolution_2, 100)
        # now with the schema coming from test_message_for_schema_evolution_2 we should be able to insert null values
        self._send_json_values(self.test_message_for_schema_evolution_1, 10)
        # this message should be never inserted due to lack of schema for one extra null field
        self._send_json_values(self.test_message_for_schema_evolution_3, 10)


    def verify(self, round):
        self._assert_number_of_records_in_table(220)

        actual_record_from_docs_dict = self._select_schematized_record_with_offset(1)
        self._verify_iceberg_content_from_docs(actual_record_from_docs_dict)

        actual_record_for_schema_evolution_2 = self._select_schematized_record_with_offset(120)
        self._verify_iceberg_content_for_schema_evolution_2(actual_record_for_schema_evolution_2)

        actual_record_for_schema_evolution_1 = self._select_schematized_record_with_offset(210)
        self._verify_iceberg_content_for_schema_evolution_1(actual_record_for_schema_evolution_1)


