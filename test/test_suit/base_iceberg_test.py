from test_suit.base_e2e import BaseE2eTest
from test_suit.assertions import *

class BaseIcebergTest(BaseE2eTest):

    def __init__(self, driver, name_salt: str, config_file_name: str):
        self.driver = driver
        self.fileName = "iceberg_json_aws"
        self.topic = config_file_name + name_salt

        self.test_message_from_docs = {
                "id": 1,
                "body_temperature": 36.6,
                "name": "Steve",
                "approved_coffee_types": ["Espresso", "Doppio", "Ristretto", "Lungo"],
                "animals_possessed": {"dogs": True, "cats": False},
            }

        self.test_message_for_schema_evolution_1 = {
            "null_long": None,
            "null_array": None,
            "null_object": None,
            "empty_array": [],
            "some_object": {
                "null_key": None,
                "string_key": "string_key"
            }
        }

        self.test_message_for_schema_evolution_2 = {
            "null_long": 2137,
            "null_array": [1, 2, 3],
            "null_object": {"key": "value"},
            "empty_array": [1, 2, 3],
            "some_object": {
                "null_key": None,
                "string_key": "string_key",
                "another_string_key": "another_string_key",
                "inner_object": {
                    "inner_object_key": 456
                }
            }
        }

        self.test_message_from_docs_schema = """
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

        self.test_headers = [("header1", "value1")]


    def getConfigFileName(self):
        return self.file_name + ".json"


    def clean(self):
        self.driver.drop_iceberg_table(self.topic)


    def verify_iceberg_content(self, content: dict):
        assert_equals(1, content['id'])
        assert_equals_with_precision(36.6, content['body_temperature'])
        assert_equals('Steve', content['name'])

        assert_equals('Espresso', content['approved_coffee_types'][0])
        assert_equals('Doppio', content['approved_coffee_types'][1])
        assert_equals('Ristretto', content['approved_coffee_types'][2])
        assert_equals('Lungo', content['approved_coffee_types'][3])

        assert_equals(True, content['animals_possessed']['dogs'])
        assert_equals(False, content['animals_possessed']['cats'])


    def verify_iceberg_metadata(self, metadata: dict):
        assert_equals(0, metadata['offset'])
        assert_equals(0, metadata['partition'])
        assert_starts_with('iceberg_', metadata['topic'])
        assert_not_null(metadata['SnowflakeConnectorPushTime'])

        assert_dict_contains('header1', 'value1', metadata['headers'])

