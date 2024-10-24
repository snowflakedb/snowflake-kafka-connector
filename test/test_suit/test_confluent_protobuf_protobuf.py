from test_suit.test_utils import RetryableError, NonRetryableError
import test_data.sensor_pb2 as sensor_pb2
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka import SerializingProducer
from test_suit.base_e2e import BaseE2eTest

import time

class TestConfluentProtobufProtobuf(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.fileName = "travis_correct_confluent_protobuf_protobuf"
        self.topic = self.fileName + nameSalt

        self.sensor = sensor_pb2.SensorReading()
        self.sensor.dateTime = 1234
        self.sensor.reading = 321.321
        self.sensor.device.deviceID = "555-4321"
        self.sensor.device.enabled = True

        self.sensor.float_val = 4321.4321
        self.sensor.int32_val = (1 << 31) - 1
        self.sensor.sint32_val = (1 << 31) - 1
        self.sensor.sint64_val = (1 << 63) - 1
        self.sensor.uint32_val = (1 << 32) - 1

        self.sensor.bytes_val = b'\xDE\xAD'
        self.sensor.double_array_val.extend([1/3, 32.21, 434324321])
        self.sensor.uint64_val = (1 << 64) - 1

        self.schema_registry_client = SchemaRegistryClient({'url': driver.schemaRegistryAddress})
        self.keyProtobufSerializer = ProtobufSerializer(sensor_pb2.SensorReading, self.schema_registry_client, {'use.deprecated.format': True})
        self.valueProtobufSerializer = ProtobufSerializer(sensor_pb2.SensorReading, self.schema_registry_client, {'use.deprecated.format': True})

        producer_conf = {
            'bootstrap.servers': driver.kafkaAddress,
            'key.serializer': self.keyProtobufSerializer,
            'value.serializer': self.valueProtobufSerializer}

        self.protobufProducer = SerializingProducer(producer_conf)


    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        for e in range(100):
            self.protobufProducer.produce(self.topic, self.sensor, self.sensor)
            self.protobufProducer.poll(0)
        self.protobufProducer.flush()

    def verify(self, round):
        res = self.driver.select_number_of_records(self.topic)
        if res == 0:
            raise RetryableError()
        elif res != 100:
            raise NonRetryableError("Number of record in table is different from number of record sent")

        # validate content of line 1
        res = self.driver.snowflake_conn.cursor().execute(
            "Select * from {} limit 1".format(self.topic)).fetchone()

        # "schema_id" is lost since they are using native avro converter
        goldMeta = r'{"CreateTime":\d*,"key":{"bytes_val":"3q0=","dateTime":1234,"device":' \
                   r'{"deviceID":"555-4321","enabled":true},"double_array_val":' \
                   r'[0.3333333333333333,32.21,4.343243210000000e+08],"float_val":4321.432,' \
                   r'"int32_val":2147483647,"reading":321.321,"sint32_val":2147483647,"sint64_val":9223372036854775807,' \
                   r'"uint32_val":4294967295,"uint64_val":-1},"offset":\d*,"partition":\d*,"topic":"travis_correct_confluent_protobuf_protobuf_\w*"}'
        goldContent = r'{"bytes_val":"3q0=","dateTime":1234,"device":{"deviceID":"555-4321","enabled":true},"double_array_val":' \
                      r'[0.3333333333333333,32.21,4.343243210000000e+08],"float_val":4321.432,"int32_val":2147483647,' \
                      r'"reading":321.321,"sint32_val":2147483647,"sint64_val":9223372036854775807,"uint32_val":4294967295,"uint64_val":-1}'
        self.driver.regexMatchOneLine(res, goldMeta, goldContent)

        self.driver.verifyStageIsCleaned(self.topic)

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)