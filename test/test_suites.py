from collections import OrderedDict
from test_suit.test_string_json import TestStringJson
from test_suit.test_json_json import TestJsonJson
from test_suit.test_string_avro import TestStringAvro
from test_suit.test_avro_avro import TestAvroAvro
from test_suit.test_string_avrosr import TestStringAvrosr
from test_suit.test_avrosr_avrosr import TestAvrosrAvrosr

from test_suit.test_native_string_avrosr import TestNativeStringAvrosr
from test_suit.test_native_string_json_without_schema import TestNativeStringJsonWithoutSchema
from test_suit.test_native_complex_smt import TestNativeComplexSmt

from test_suit.test_native_string_protobuf import TestNativeStringProtobuf
from test_suit.test_confluent_protobuf_protobuf import TestConfluentProtobufProtobuf

from test_suit.test_snowpipe_streaming_string_json import TestSnowpipeStreamingStringJson
from test_suit.test_snowpipe_streaming_string_json_dlq import TestSnowpipeStreamingStringJsonDLQ
from test_suit.test_snowpipe_streaming_string_avro_sr import TestSnowpipeStreamingStringAvroSR

from test_suit.test_multiple_topic_to_one_table_snowpipe_streaming import \
    TestMultipleTopicToOneTableSnowpipeStreaming
from test_suit.test_multiple_topic_to_one_table_snowpipe import TestMultipleTopicToOneTableSnowpipe

from test_suit.test_schema_mapping import TestSchemaMapping

from test_suit.test_auto_table_creation import TestAutoTableCreation
from test_suit.test_auto_table_creation_topic2table import TestAutoTableCreationTopic2Table

from test_suit.test_schema_evolution_json import TestSchemaEvolutionJson
from test_suit.test_schema_evolution_avro_sr import TestSchemaEvolutionAvroSR

from test_suit.test_schema_evolution_w_auto_table_creation_json import \
    TestSchemaEvolutionWithAutoTableCreationJson
from test_suit.test_schema_evolution_w_auto_table_creation_avro_sr import \
    TestSchemaEvolutionWithAutoTableCreationAvroSR

from test_suit.test_schema_evolution_nonnullable_json import TestSchemaEvolutionNonNullableJson

from test_suit.test_schema_not_supported_converter import TestSchemaNotSupportedConverter

from test_suit.test_schema_evolution_drop_table import TestSchemaEvolutionDropTable

from test_suit.test_snowpipe_streaming_schema_mapping_dlq import TestSnowpipeStreamingSchemaMappingDLQ

# res tests
from test_suit.resilience_tests.test_kc_delete_create import TestKcDeleteCreate
from test_suit.resilience_tests.test_kc_delete_create_chaos import TestKcDeleteCreateChaos
from test_suit.resilience_tests.test_kc_delete_resume import TestKcDeleteResume
from test_suit.resilience_tests.test_kc_delete_resume_chaos import TestKcDeleteResumeChaos
from test_suit.resilience_tests.test_kc_pause_create import TestKcPauseCreate
from test_suit.resilience_tests.test_kc_pause_create_chaos import TestKcPauseCreateChaos
from test_suit.resilience_tests.test_kc_pause_resume import TestKcPauseResume
from test_suit.resilience_tests.test_kc_pause_resume_chaos import TestKcPauseResumeChaos
from test_suit.resilience_tests.test_kc_recreate import TestKcRecreate
from test_suit.resilience_tests.test_kc_recreate_chaos import TestKcRecreateChaos
from test_suit.resilience_tests.test_kc_restart import TestKcRestart

class EndToEndTestSuite:
    '''
    Placeholder class for defining what a single end to end test looks like.
    Just modify the caller constructor of this class to disable, enable in confluent or Apache Kafka.
    In future can add whether it runs in snowpipe or snowpipe streaming mode.
    '''
    def __init__(self, test_instance, clean, run_in_confluent, run_in_apache):
        self._test_instance = test_instance
        self._clean = clean
        self._run_in_confluent = run_in_confluent
        self._run_in_apache = run_in_apache

    @property
    def test_instance(self):
        return self._test_instance

    @property
    def clean(self):
        return self._clean

    @property
    def run_in_confluent(self):
        return self._run_in_confluent

    @property
    def run_in_apache(self):
        return self._run_in_apache

def create_end_to_end_test_suites(driver, nameSalt, schemaRegistryAddress, testSet):
    '''
    Creates all End to End tests which needs to run against Confluent Kafka or Apache Kafka.
    :param driver: Driver holds all helper function for tests - Create topic, create connector, send data are few functions amongst many present in Class KafkaTest.
    :param nameSalt: random string appended for uniqueness of Connector Name
    :param schemaRegistryAddress: Schema registry For confluent runs
    :param testSet: confluent Kafka or apache Kafka (OSS)
    :return:
    '''
    test_suites = OrderedDict([
        ("TestStringJson", EndToEndTestSuite(
            test_instance=TestStringJson(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestJsonJson", EndToEndTestSuite(
            test_instance=TestJsonJson(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestStringAvro", EndToEndTestSuite(
            test_instance=TestStringAvro(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestAvroAvro", EndToEndTestSuite(
            test_instance=TestAvroAvro(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestStringAvrosr", EndToEndTestSuite(
            test_instance=TestStringAvrosr(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=False
        )),
        ("TestAvrosrAvrosr", EndToEndTestSuite(
            test_instance=TestAvrosrAvrosr(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=False
        )),
        ("TestNativeStringAvrosr", EndToEndTestSuite(
            test_instance=TestNativeStringAvrosr(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=False
        )),
        ("TestNativeStringJsonWithoutSchema", EndToEndTestSuite(
            test_instance=TestNativeStringJsonWithoutSchema(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestNativeComplexSmt", EndToEndTestSuite(
            test_instance=TestNativeComplexSmt(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestNativeStringProtobuf", EndToEndTestSuite(
            test_instance=TestNativeStringProtobuf(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestConfluentProtobufProtobuf", EndToEndTestSuite(
            test_instance=TestConfluentProtobufProtobuf(driver, nameSalt), clean=True, run_in_confluent=False, run_in_apache=False
        )),
        ("TestSnowpipeStreamingStringJson", EndToEndTestSuite(
            test_instance=TestSnowpipeStreamingStringJson(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestSnowpipeStreamingStringJsonDLQ", EndToEndTestSuite(
            test_instance=TestSnowpipeStreamingStringJsonDLQ(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestSnowpipeStreamingStringAvro", EndToEndTestSuite(
            test_instance=TestSnowpipeStreamingStringAvroSR(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=False
        )),
        ("TestMultipleTopicToOneTableSnowpipeStreaming", EndToEndTestSuite(
            test_instance=TestMultipleTopicToOneTableSnowpipeStreaming(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestMultipleTopicToOneTableSnowpipe", EndToEndTestSuite(
            test_instance=TestMultipleTopicToOneTableSnowpipe(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestSchemaMapping", EndToEndTestSuite(
            test_instance=TestSchemaMapping(driver, nameSalt), clean=True, run_in_confluent=True,run_in_apache=True
        )),
        ("TestSnowpipeStreamingSchemaMappingDLQ", EndToEndTestSuite(
            test_instance=TestSnowpipeStreamingSchemaMappingDLQ(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestAutoTableCreation", EndToEndTestSuite(
            test_instance=TestAutoTableCreation(driver, nameSalt, schemaRegistryAddress, testSet), clean=True, run_in_confluent=True, run_in_apache=False
        )),
        ("TestAutoTableCreationTopic2Table", EndToEndTestSuite(
            test_instance=TestAutoTableCreationTopic2Table(driver, nameSalt, schemaRegistryAddress, testSet), clean=True, run_in_confluent=True, run_in_apache=False
        )),
        ("TestSchemaEvolutionJson", EndToEndTestSuite(
            test_instance=TestSchemaEvolutionJson(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestSchemaEvolutionAvroSR", EndToEndTestSuite(
            test_instance=TestSchemaEvolutionAvroSR(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=False
        )),
        ("TestSchemaEvolutionWithAutoTableCreationJson", EndToEndTestSuite(
            test_instance=TestSchemaEvolutionWithAutoTableCreationJson(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestSchemaEvolutionWithAutoTableCreationAvroSR", EndToEndTestSuite(
            test_instance=TestSchemaEvolutionWithAutoTableCreationAvroSR(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=False
        )),
        ("TestSchemaEvolutionNonNullableJson", EndToEndTestSuite(
            test_instance=TestSchemaEvolutionNonNullableJson(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestSchemaNotSupportedConverter", EndToEndTestSuite(
            test_instance=TestSchemaNotSupportedConverter(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestSchemaEvolutionDropTable", EndToEndTestSuite(
            test_instance=TestSchemaEvolutionDropTable(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestKcDeleteCreate", EndToEndTestSuite(
            test_instance=TestKcDeleteCreate(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestKcDeleteCreateChaos", EndToEndTestSuite(
            test_instance=TestKcDeleteCreateChaos(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestKcDeleteResume", EndToEndTestSuite(
            test_instance=TestKcDeleteResume(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestKcDeleteResumeChaos", EndToEndTestSuite(
            test_instance=TestKcDeleteResumeChaos(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestKcPauseCreate", EndToEndTestSuite(
            test_instance=TestKcPauseCreate(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestKcPauseCreateChaos", EndToEndTestSuite(
            test_instance=TestKcPauseCreateChaos(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestKcPauseResume", EndToEndTestSuite(
            test_instance=TestKcPauseResume(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestKcPauseResumeChaos", EndToEndTestSuite(
            test_instance=TestKcPauseResumeChaos(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestKcRecreate", EndToEndTestSuite(
            test_instance=TestKcRecreate(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestKcRecreateChaos", EndToEndTestSuite(
            test_instance=TestKcRecreateChaos(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
        ("TestKcRestart", EndToEndTestSuite(
            test_instance=TestKcRestart(driver, nameSalt), clean=True, run_in_confluent=True, run_in_apache=True
        )),
    ])
    return test_suites


