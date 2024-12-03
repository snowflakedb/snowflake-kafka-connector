from collections import OrderedDict

from test_suit.resilience_tests.test_kc_delete_create import TestKcDeleteCreate
from test_suit.resilience_tests.test_kc_delete_create_chaos import (
    TestKcDeleteCreateChaos,
)
from test_suit.resilience_tests.test_kc_delete_resume import TestKcDeleteResume
from test_suit.resilience_tests.test_kc_delete_resume_chaos import (
    TestKcDeleteResumeChaos,
)
from test_suit.resilience_tests.test_kc_pause_create import TestKcPauseCreate
from test_suit.resilience_tests.test_kc_pause_create_chaos import TestKcPauseCreateChaos
from test_suit.resilience_tests.test_kc_pause_resume import TestKcPauseResume
from test_suit.resilience_tests.test_kc_pause_resume_chaos import TestKcPauseResumeChaos
from test_suit.resilience_tests.test_kc_recreate import TestKcRecreate
from test_suit.resilience_tests.test_kc_recreate_chaos import TestKcRecreateChaos
from test_suit.resilience_tests.test_kc_restart import TestKcRestart
from test_suit.test_auto_table_creation import TestAutoTableCreation
from test_suit.test_auto_table_creation_topic2table import (
    TestAutoTableCreationTopic2Table,
)
from test_suit.test_avro_avro import TestAvroAvro
from test_suit.test_avrosr_avrosr import TestAvrosrAvrosr
from test_suit.test_confluent_protobuf_protobuf import TestConfluentProtobufProtobuf
from test_suit.test_json_json import TestJsonJson
from test_suit.test_multiple_topic_to_one_table_snowpipe import (
    TestMultipleTopicToOneTableSnowpipe,
)
from test_suit.test_multiple_topic_to_one_table_snowpipe_streaming import (
    TestMultipleTopicToOneTableSnowpipeStreaming,
)
from test_suit.test_native_complex_smt import TestNativeComplexSmt
from test_suit.test_native_string_avrosr import TestNativeStringAvrosr
from test_suit.test_native_string_json_without_schema import (
    TestNativeStringJsonWithoutSchema,
)
from test_suit.test_nullable_values_after_smt import TestNullableValuesAfterSmt
from test_suit.test_schema_evolution_avro_sr import TestSchemaEvolutionAvroSR
from test_suit.test_schema_evolution_avro_sr_logical_types import (
    TestSchemaEvolutionAvroSRLogicalTypes,
)
from test_suit.test_schema_evolution_drop_table import TestSchemaEvolutionDropTable
from test_suit.iceberg_avro_aws import TestIcebergAvroAws
from test_suit.iceberg_json_aws import TestIcebergJsonAws
from test_suit.iceberg_schema_evolution_avro_aws import TestIcebergSchemaEvolutionAvroAws
from test_suit.iceberg_schema_evolution_json_aws import TestIcebergSchemaEvolutionJsonAws
from test_suit.test_schema_evolution_json import TestSchemaEvolutionJson
from test_suit.test_schema_evolution_json_ignore_tombstone import (
    TestSchemaEvolutionJsonIgnoreTombstone,
)
from test_suit.test_schema_evolution_multi_topic_drop_table import (
    TestSchemaEvolutionMultiTopicDropTable,
)
from test_suit.test_schema_evolution_nullable_values_after_smt import (
    TestSchemaEvolutionNullableValuesAfterSmt,
)
from test_suit.test_schema_evolution_nonnullable_json import (
    TestSchemaEvolutionNonNullableJson,
)
from test_suit.test_schema_evolution_w_auto_table_creation_avro_sr import (
    TestSchemaEvolutionWithAutoTableCreationAvroSR,
)
from test_suit.test_schema_evolution_w_auto_table_creation_json import (
    TestSchemaEvolutionWithAutoTableCreationJson,
)
from test_suit.test_schema_evolution_w_random_row_count import (
    TestSchemaEvolutionWithRandomRowCount,
)
from test_suit.test_schema_mapping import TestSchemaMapping
from test_suit.test_schema_not_supported_converter import (
    TestSchemaNotSupportedConverter,
)
from test_suit.test_snowpipe_streaming_channel_migration_disabled import (
    TestSnowpipeStreamingStringJsonChannelMigrationDisabled,
)
from test_suit.test_snowpipe_streaming_schema_mapping_dlq import (
    TestSnowpipeStreamingSchemaMappingDLQ,
)
from test_suit.test_snowpipe_streaming_nullable_values_after_smt import (
    TestSnowpipeStreamingNullableValuesAfterSmt,
)
from test_suit.test_snowpipe_streaming_string_avro_sr import (
    TestSnowpipeStreamingStringAvroSR,
)
from test_suit.test_snowpipe_streaming_string_json import (
    TestSnowpipeStreamingStringJson,
)
from test_suit.test_snowpipe_streaming_string_json_dlq import (
    TestSnowpipeStreamingStringJsonDLQ,
)
from test_suit.test_snowpipe_streaming_string_json_ignore_tombstone import (
    TestSnowpipeStreamingStringJsonIgnoreTombstone,
)
from test_suit.test_native_string_protobuf import TestNativeStringProtobuf
from test_suit.test_string_avro import TestStringAvro
from test_suit.test_string_avrosr import TestStringAvrosr
from test_suit.test_string_json import TestStringJson
from test_suit.test_string_json_ignore_tombstone import TestStringJsonIgnoreTombstone
from test_suit.test_streaming_client_parameter_override import (
    TestStreamingClientParameterOverride,
)
from cloud_platform import CloudPlatform


class EndToEndTestSuite:
    """
    Placeholder class for defining what a single end to end test looks like.
    Just modify the caller constructor of this class to disable, enable in confluent or Apache Kafka.
    In future can add whether it runs in snowpipe or snowpipe streaming mode.
    """

    def __init__(
        self,
        test_instance,
        run_in_confluent,
        run_in_apache,
        cloud_platform: CloudPlatform,
    ):
        self._test_instance = test_instance
        self._run_in_confluent = run_in_confluent
        self._run_in_apache = run_in_apache
        self._cloud_platform = cloud_platform

    @property
    def test_instance(self):
        return self._test_instance

    @property
    def run_in_confluent(self):
        return self._run_in_confluent

    @property
    def run_in_apache(self):
        return self._run_in_apache

    @property
    def cloud_platform(self):
        return self._cloud_platform


def create_end_to_end_test_suites(driver, nameSalt, schemaRegistryAddress, testSet):
    """
    Creates all End to End tests which needs to run against Confluent Kafka or Apache Kafka.
    :param driver: Driver holds all helper function for tests - Create topic, create connector, send data are few functions amongst many present in Class KafkaTest.
    :param nameSalt: random string appended for uniqueness of Connector Name
    :param schemaRegistryAddress: Schema registry For confluent runs
    :param testSet: confluent Kafka or apache Kafka (OSS)
    :return:
    """
    return OrderedDict(
        [
            (
                "TestStringJson",
                EndToEndTestSuite(
                    test_instance=TestStringJson(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestStringJsonIgnoreTombstone",
                EndToEndTestSuite(
                    test_instance=TestStringJsonIgnoreTombstone(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestJsonJson",
                EndToEndTestSuite(
                    test_instance=TestJsonJson(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestStringAvro",
                EndToEndTestSuite(
                    test_instance=TestStringAvro(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestAvroAvro",
                EndToEndTestSuite(
                    test_instance=TestAvroAvro(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestStringAvrosr",
                EndToEndTestSuite(
                    test_instance=TestStringAvrosr(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=False,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestAvrosrAvrosr",
                EndToEndTestSuite(
                    test_instance=TestAvrosrAvrosr(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=False,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestNativeStringAvrosr",
                EndToEndTestSuite(
                    test_instance=TestNativeStringAvrosr(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=False,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestNativeStringJsonWithoutSchema",
                EndToEndTestSuite(
                    test_instance=TestNativeStringJsonWithoutSchema(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestNativeComplexSmt",
                EndToEndTestSuite(
                    test_instance=TestNativeComplexSmt(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestNativeStringProtobuf",
                EndToEndTestSuite(
                    test_instance=TestNativeStringProtobuf(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestNullableValuesAfterSmt",
                EndToEndTestSuite(
                    test_instance=TestNullableValuesAfterSmt(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestConfluentProtobufProtobuf",
                EndToEndTestSuite(
                    test_instance=TestConfluentProtobufProtobuf(driver, nameSalt),
                    run_in_confluent=False,
                    run_in_apache=False,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSnowpipeStreamingNullableValuesAfterSmt",
                EndToEndTestSuite(
                    test_instance=TestSnowpipeStreamingNullableValuesAfterSmt(
                        driver, nameSalt
                    ),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSnowpipeStreamingStringJson",
                EndToEndTestSuite(
                    test_instance=TestSnowpipeStreamingStringJson(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSnowpipeStreamingStringJsonChannelMigrationDisabled",
                EndToEndTestSuite(
                    test_instance=TestSnowpipeStreamingStringJsonChannelMigrationDisabled(
                        driver, nameSalt
                    ),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSnowpipeStreamingStringJsonIgnoreTombstone",
                EndToEndTestSuite(
                    test_instance=TestSnowpipeStreamingStringJsonIgnoreTombstone(
                        driver, nameSalt
                    ),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSnowpipeStreamingStringJsonDLQ",
                EndToEndTestSuite(
                    test_instance=TestSnowpipeStreamingStringJsonDLQ(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSnowpipeStreamingStringAvroSR",
                EndToEndTestSuite(
                    test_instance=TestSnowpipeStreamingStringAvroSR(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=False,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestMultipleTopicToOneTableSnowpipeStreaming",
                EndToEndTestSuite(
                    test_instance=TestMultipleTopicToOneTableSnowpipeStreaming(
                        driver, nameSalt
                    ),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestMultipleTopicToOneTableSnowpipe",
                EndToEndTestSuite(
                    test_instance=TestMultipleTopicToOneTableSnowpipe(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSchemaMapping",
                EndToEndTestSuite(
                    test_instance=TestSchemaMapping(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSnowpipeStreamingSchemaMappingDLQ",
                EndToEndTestSuite(
                    test_instance=TestSnowpipeStreamingSchemaMappingDLQ(
                        driver, nameSalt
                    ),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestAutoTableCreation",
                EndToEndTestSuite(
                    test_instance=TestAutoTableCreation(
                        driver, nameSalt, schemaRegistryAddress, testSet
                    ),
                    run_in_confluent=True,
                    run_in_apache=False,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestAutoTableCreationTopic2Table",
                EndToEndTestSuite(
                    test_instance=TestAutoTableCreationTopic2Table(
                        driver, nameSalt, schemaRegistryAddress, testSet
                    ),
                    run_in_confluent=True,
                    run_in_apache=False,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSchemaEvolutionJson",
                EndToEndTestSuite(
                    test_instance=TestSchemaEvolutionJson(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSchemaEvolutionJsonIgnoreTombstone",
                EndToEndTestSuite(
                    test_instance=TestSchemaEvolutionJsonIgnoreTombstone(
                        driver, nameSalt
                    ),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSchemaEvolutionAvroSR",
                EndToEndTestSuite(
                    test_instance=TestSchemaEvolutionAvroSR(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=False,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSchemaEvolutionAvroSRLogicalTypes",
                EndToEndTestSuite(
                    test_instance=TestSchemaEvolutionAvroSRLogicalTypes(
                        driver, nameSalt
                    ),
                    run_in_confluent=False,
                    run_in_apache=False,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSchemaEvolutionWithAutoTableCreationJson",
                EndToEndTestSuite(
                    test_instance=TestSchemaEvolutionWithAutoTableCreationJson(
                        driver, nameSalt
                    ),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSchemaEvolutionWithAutoTableCreationAvroSR",
                EndToEndTestSuite(
                    test_instance=TestSchemaEvolutionWithAutoTableCreationAvroSR(
                        driver, nameSalt
                    ),
                    run_in_confluent=True,
                    run_in_apache=False,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSchemaEvolutionWithRandomRowCount",
                EndToEndTestSuite(
                    test_instance=TestSchemaEvolutionWithRandomRowCount(
                        driver, nameSalt
                    ),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSchemaEvolutionNonNullableJson",
                EndToEndTestSuite(
                    test_instance=TestSchemaEvolutionNonNullableJson(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSchemaEvolutionNullableValuesAfterSmt",
                EndToEndTestSuite(
                    test_instance=TestSchemaEvolutionNullableValuesAfterSmt(
                        driver, nameSalt
                    ),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSchemaNotSupportedConverter",
                EndToEndTestSuite(
                    test_instance=TestSchemaNotSupportedConverter(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestKcDeleteCreate",
                EndToEndTestSuite(
                    test_instance=TestKcDeleteCreate(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestKcDeleteCreateChaos",
                EndToEndTestSuite(
                    test_instance=TestKcDeleteCreateChaos(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestKcDeleteResume",
                EndToEndTestSuite(
                    test_instance=TestKcDeleteResume(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestKcDeleteResumeChaos",
                EndToEndTestSuite(
                    test_instance=TestKcDeleteResumeChaos(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestKcPauseCreate",
                EndToEndTestSuite(
                    test_instance=TestKcPauseCreate(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestKcPauseCreateChaos",
                EndToEndTestSuite(
                    test_instance=TestKcPauseCreateChaos(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestKcPauseResume",
                EndToEndTestSuite(
                    test_instance=TestKcPauseResume(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestKcPauseResumeChaos",
                EndToEndTestSuite(
                    test_instance=TestKcPauseResumeChaos(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestKcRecreate",
                EndToEndTestSuite(
                    test_instance=TestKcRecreate(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestKcRecreateChaos",
                EndToEndTestSuite(
                    test_instance=TestKcRecreateChaos(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestKcRestart",
                EndToEndTestSuite(
                    test_instance=TestKcRestart(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSchemaEvolutionDropTable",
                EndToEndTestSuite(
                    test_instance=TestSchemaEvolutionDropTable(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestSchemaEvolutionMultiTopicDropTable",
                EndToEndTestSuite(
                    test_instance=TestSchemaEvolutionMultiTopicDropTable(
                        driver, nameSalt
                    ),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestStreamingClientParameterOverride",
                EndToEndTestSuite(
                    test_instance=TestStreamingClientParameterOverride(
                        driver, nameSalt
                    ),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.ALL,
                ),
            ),
            (
                "TestIcebergJsonAws",
                EndToEndTestSuite(
                    test_instance=TestIcebergJsonAws(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.AWS,
                ),
            ),
            (
                "TestIcebergAvroAws",
                EndToEndTestSuite(
                    test_instance=TestIcebergAvroAws(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=False,
                    cloud_platform=CloudPlatform.AWS,
                ),
            ),
            (
                "TestIcebergSchemaEvolutionJsonAws",
                EndToEndTestSuite(
                    test_instance=TestIcebergSchemaEvolutionJsonAws(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=True,
                    cloud_platform=CloudPlatform.AWS,
                ),
            ),
            (
                "TestIcebergSchemaEvolutionAvroAws",
                EndToEndTestSuite(
                    test_instance=TestIcebergSchemaEvolutionAvroAws(driver, nameSalt),
                    run_in_confluent=True,
                    run_in_apache=False,
                    cloud_platform=CloudPlatform.AWS,
                ),
            ),
        ]
    )
