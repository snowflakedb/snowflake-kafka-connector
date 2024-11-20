from test_suit.base_e2e import BaseE2eTest
from test_suit.assertions import *

class BaseIcebergTest(BaseE2eTest):

    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.test_message = {
                "id": 1,
                "body_temperature": 36.6,
                "name": "Steve",
                "approved_coffee_types": ["Espresso", "Doppio", "Ristretto", "Lungo"],
                "animals_possessed": {"dogs": True, "cats": False},
            }
        self.test_headers = [("header1", "value1")]

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

