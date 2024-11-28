from test_suit.base_iceberg_test import BaseIcebergTest


class TestIcebergSchemaEvolutionAvroAws(BaseIcebergTest):
    def __init__(self, driver, name_salt: str):
        BaseIcebergTest.__init__(self, driver, name_salt, "iceberg_schema_evolution_avro_aws")


    def setup(self):
        self.driver.create_empty_iceberg_table(
            table_name=self.topic,
            external_volume="kafka_push_e2e_volume_aws",  # volume created manually
        )


    def send(self):
        pass


    def verify(self, round):
        pass
