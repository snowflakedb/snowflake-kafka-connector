from test_suit.base_e2e import BaseE2eTest
from test_suit.assertions import *
from test_suit.test_utils import RetryableError, NonRetryableError
import json
from confluent_kafka import avro

class BaseIcebergTest(BaseE2eTest):

    def __init__(self, driver, name_salt: str, config_file_name: str):
        self.driver = driver
        self.file_name = config_file_name
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
                "null_key": "solnik",
                "string_key": "string_key",
                "another_string_key": "another_string_key",
                "inner_object": {
                    "inner_object_key": 456
                }
            }
        }

        self.test_message_for_schema_evolution_3 = {
            "extra_null_long": None,
            "null_long": None,
            "null_array": None,
            "null_object": None,
            "empty_array": [],
            "some_object": {
                "null_key": None,
                "string_key": "string_key"
            }
        }

        self.test_message_from_docs_schema = """
        {
            "type":"record",
            "name":"value_schema",
            "fields": [
            {
              "name": "id",
              "type": [
                "null",
                "int"
              ]
            },
            {
              "name": "body_temperature",
              "type": [
                "null",
                "float"
              ]
            },
            {
              "name": "name",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "approved_coffee_types",
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ]
            },
            {
              "name": "animals_possessed",
              "type": [
                "null",
                {
                  "type": "map",
                  "values": "boolean"
                }
              ]
            }
          ]
        }
        """

        self.test_message_for_schema_evolution_schema = """
        {
          "type": "record",
          "name": "value_schema",
          "fields": [
            {
              "name": "id",
              "type": [
                "null",
                "int"
              ]
            },
            {
              "name": "body_temperature",
              "type": [
                "null",
                "float"
              ]
            },
            {
              "name": "name",
              "type": [
                "null",
                "string"
              ]
            },
            {
              "name": "approved_coffee_types",
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "string"
                }
              ]
            },
            {
              "name": "animals_possessed",
              "type": [
                "null",
                {
                  "type": "map",
                  "values": "boolean"
                }
              ]
            },
            {
              "name": "null_long",
              "default": null,
              "type": [
                "null",
                "long"
              ]
            },
            {
              "name": "null_array",
              "default": null,
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "int"
                }
              ]
            },
            {
              "name": "null_object",
              "default": null,
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "null_object_record",
                  "fields": [
                    {
                      "name": "key",
                      "type": "string"
                    }
                  ]
                }
              ]
            },
            {
              "name": "empty_array",
              "default": null,
              "type": [
                "null",
                {
                  "type": "array",
                  "items": "int"
                }
              ]
            },
            {
              "name": "some_object",
              "default": null,
              "type": [
                "null",
                {
                  "type": "record",
                  "name": "some_object_record",
                  "fields": [
                    {
                      "name": "null_key",
                      "type": [
                        "null",
                        "string"
                      ]
                    },
                    {
                      "name": "string_key",
                      "type": "string"
                    },
                    {
                      "name": "another_string_key",
                      "type": [
                        "null",
                        "string"
                      ]
                    },
                    {
                      "name": "inner_object",
                      "type": [
                        "null",
                        {
                          "type": "map",
                          "values": "int"
                        }
                      ]
                    }
                  ]
                }
              ]
            }
          ]
        }
        """

        self.test_headers = [("header1", "value1")]


    def getConfigFileName(self):
        return self.file_name + ".json"


    def clean(self):
        self.driver.drop_iceberg_table(self.topic)


    def _send_json_values(self, msg: dict, number_of_messages: int):
        json_msg = json.dumps(msg)

        key = [json.dumps({"number": str(e)}).encode("utf-8") for e in range(number_of_messages)]
        value = [json_msg.encode("utf-8") for _ in range(number_of_messages)]

        self.driver.sendBytesData(
            topic=self.topic,
            value=value,
            key=key,
            partition=0,
            headers=self.test_headers,
        )

    def _assert_number_of_records_in_table(self, expected_number_of_records: int):
        number_of_records = self.driver.select_number_of_records(self.topic)
        if number_of_records == 0:
            raise RetryableError()
        elif number_of_records != expected_number_of_records:
            raise NonRetryableError(
                f'Number of record in table is different from number of record sent. Expected {expected_number_of_records}, got {number_of_records}.'
            )


    def _verify_iceberg_content_from_docs(self, content: dict):
        assert_equals(1, content['id'])
        assert_equals_with_precision(36.6, content['body_temperature'])
        assert_equals('Steve', content['name'])

        assert_equals('Espresso', content['approved_coffee_types'][0])
        assert_equals('Doppio', content['approved_coffee_types'][1])
        assert_equals('Ristretto', content['approved_coffee_types'][2])
        assert_equals('Lungo', content['approved_coffee_types'][3])

        assert_equals(True, content['animals_possessed']['dogs'])
        assert_equals(False, content['animals_possessed']['cats'])


    def _verify_iceberg_content_for_schema_evolution_1(self, content: dict):
        assert_equals(None, content['id'])
        assert_equals(None, content['body_temperature'])
        assert_equals(None, content['name'])
        assert_equals(None, content['approved_coffee_types'])
        assert_equals(None, content['animals_possessed'])

        assert_equals(None, content['null_long'])
        assert_equals(None, content['null_array'])
        assert_equals(None, content['null_object'])
        assert_equals([], content['empty_array'])
        assert_equals(None, content['some_object']['null_key'])
        assert_equals('string_key', content['some_object']['string_key'])


    def _verify_iceberg_content_for_schema_evolution_2(self, content: dict):
        assert_equals(None, content['id'])
        assert_equals(None, content['body_temperature'])
        assert_equals(None, content['name'])
        assert_equals(None, content['approved_coffee_types'])
        assert_equals(None, content['animals_possessed'])

        assert_equals(2137, content['null_long'])
        assert_equals([1, 2, 3], content['null_array'])
        assert_equals('value', content['null_object']['key'])
        assert_equals([1, 2, 3], content['empty_array'])
        assert_equals("solnik", content['some_object']['null_key'])
        assert_equals('string_key', content['some_object']['string_key'])
        assert_equals('another_string_key', content['some_object']['another_string_key'])
        assert_equals(456, content['some_object']['inner_object']['inner_object_key'])



    def _verify_iceberg_metadata(self, metadata: dict):
        assert_equals(0, metadata['offset'])
        assert_equals(0, metadata['partition'])
        assert_starts_with('iceberg_', metadata['topic'])
        assert_not_null(metadata['SnowflakeConnectorPushTime'])

        assert_dict_contains('header1', 'value1', metadata['headers'])


    def _select_schematized_record_with_offset(self, offset: int) -> dict:
        record = (
            self.driver.snowflake_conn.cursor()
                .execute("select id, body_temperature, name, approved_coffee_types, animals_possessed, null_long, null_array, null_object, empty_array, some_object from {} limit 1 offset {}".format(self.topic, offset))
                .fetchone()
        )

        return {
            "id": record[0],
            "body_temperature": record[1],
            "name": record[2],
            "approved_coffee_types": self.__none_or_json_loads(record[3]),
            "animals_possessed": self.__none_or_json_loads(record[4]),
            "null_long": record[5],
            "null_array": self.__none_or_json_loads(record[6]),
            "null_object": self.__none_or_json_loads(record[7]),
            "empty_array": self.__none_or_json_loads(record[8]),
            "some_object": self.__none_or_json_loads(record[9])
        }


    def __none_or_json_loads(self, value: str) -> dict:
        return None if value is None else json.loads(value)


    def _send_avro_messages(self, message: str, schema: str):
        self.driver.sendAvroSRData(
            topic=self.topic,
            value=[message for _ in range(100)],
            value_schema=avro.loads(schema),
            headers=self.test_headers,
        )

