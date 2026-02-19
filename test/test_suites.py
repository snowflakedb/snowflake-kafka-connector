from collections import OrderedDict

from test_suit.resilience_tests.test_kc_delete_create import TestKcDeleteCreate
from test_suit.resilience_tests.test_kc_delete_create_chaos import (
    TestKcDeleteCreateChaos,
)
# from test_suit.resilience_tests.test_kc_delete_resume import TestKcDeleteResume
# from test_suit.resilience_tests.test_kc_delete_resume_chaos import (
#     TestKcDeleteResumeChaos,
# )
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

from test_suit.test_native_complex_smt import TestNativeComplexSmt
from test_suit.test_native_string_json_without_schema import (
    TestNativeStringJsonWithoutSchema,
)
# from test_suit.iceberg_avro_aws import TestIcebergAvroAws
# from test_suit.iceberg_json_aws import TestIcebergJsonAws
# from test_suit.iceberg_schema_evolution_avro_aws import TestIcebergSchemaEvolutionAvroAws
# from test_suit.iceberg_schema_evolution_json_aws import TestIcebergSchemaEvolutionJsonAws
from test_suit.test_schema_mapping import TestSchemaMapping
from test_suit.test_snowpipe_streaming_schema_mapping_dlq import (
    TestSnowpipeStreamingSchemaMappingDLQ,
)
from test_suit.test_snowpipe_streaming_string_json_ignore_tombstone import (
    TestSnowpipeStreamingStringJsonIgnoreTombstone,
)

from test_suit.test_string_avro import TestStringAvro
from test_suit.test_string_json_ignore_tombstone import TestStringJsonIgnoreTombstone
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
    :param driver: Driver holds all helper function for tests - Create topic, create connector, send data are few functions amongst many present in Class KafkaDriver.
    :param nameSalt: random string appended for uniqueness of Connector Name
    :param schemaRegistryAddress: Schema registry For confluent runs
    :param testSet: confluent Kafka or apache Kafka (OSS)
    :return:
    """
    return OrderedDict(
        [
            # (
            #     "TestStringJsonIgnoreTombstone",
            #     EndToEndTestSuite(
            #         test_instance=TestStringJsonIgnoreTombstone(driver, nameSalt),
            #         run_in_confluent=True,
            #         run_in_apache=True,
            #         cloud_platform=CloudPlatform.ALL,
            #     ),
            # ),
            # (
            #     "TestStringAvro",
            #     EndToEndTestSuite(
            #         test_instance=TestStringAvro(driver, nameSalt),
            #         run_in_confluent=True,
            #         run_in_apache=True,
            #         cloud_platform=CloudPlatform.ALL,
            #     ),
            # ),
            # (
            #     "TestAvroAvro",
            #     EndToEndTestSuite(
            #         test_instance=TestAvroAvro(driver, nameSalt),
            #         run_in_confluent=True,
            #         run_in_apache=True,
            #         cloud_platform=CloudPlatform.ALL,
            #     ),
            # ),
            # (
            #     "TestNativeStringJsonWithoutSchema",
            #     EndToEndTestSuite(
            #         test_instance=TestNativeStringJsonWithoutSchema(driver, nameSalt),
            #         run_in_confluent=True,
            #         run_in_apache=True,
            #         cloud_platform=CloudPlatform.ALL,
            #     ),
            # ),
            # (
            #     "TestNativeComplexSmt",
            #     EndToEndTestSuite(
            #         test_instance=TestNativeComplexSmt(driver, nameSalt),
            #         run_in_confluent=True,
            #         run_in_apache=True,
            #         cloud_platform=CloudPlatform.ALL,
            #     ),
            # ),
            # (
            #     "TestSnowpipeStreamingStringJsonIgnoreTombstone",
            #     EndToEndTestSuite(
            #         test_instance=TestSnowpipeStreamingStringJsonIgnoreTombstone(
            #             driver, nameSalt
            #         ),
            #         run_in_confluent=True,
            #         run_in_apache=True,
            #         cloud_platform=CloudPlatform.AWS,
            #     ),
            # ),
            # (
            #     "TestSchemaMapping",
            #     EndToEndTestSuite(
            #         test_instance=TestSchemaMapping(driver, nameSalt),
            #         run_in_confluent=True,
            #         run_in_apache=True,
            #         cloud_platform=CloudPlatform.AWS,
            #     ),
            # ),
            # (
            #     "TestSnowpipeStreamingSchemaMappingDLQ",
            #     EndToEndTestSuite(
            #         test_instance=TestSnowpipeStreamingSchemaMappingDLQ(
            #             driver, nameSalt
            #         ),
            #         run_in_confluent=True,
            #         run_in_apache=True,
            #         cloud_platform=CloudPlatform.AWS,
            #     ),
            # ),
            # (
            #     "TestAutoTableCreation",
            #     EndToEndTestSuite(
            #         test_instance=TestAutoTableCreation(
            #             driver, nameSalt, schemaRegistryAddress, testSet
            #         ),
            #         run_in_confluent=True,
            #         run_in_apache=False,
            #         cloud_platform=CloudPlatform.AWS,
            #     ),
            # ),
            # (
            #     "TestAutoTableCreationTopic2Table",
            #     EndToEndTestSuite(
            #         test_instance=TestAutoTableCreationTopic2Table(
            #             driver, nameSalt, schemaRegistryAddress, testSet
            #         ),
            #         run_in_confluent=True,
            #         run_in_apache=False,
            #         cloud_platform=CloudPlatform.AWS,
            #     ),
            # ),
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
            # This test is flaky and needs to be fixed, it takes too long to execute 10 attempts
            # (
            #     "TestKcDeleteResume",
            #     EndToEndTestSuite(
            #         test_instance=TestKcDeleteResume(driver, nameSalt),
            #         run_in_confluent=True,
            #         run_in_apache=True,
            #         cloud_platform=CloudPlatform.AWS,
            #     ),
            # ),
            # This test is flaky and needs to be fixed, it takes too long to execute 10 attempts
            # (
            #     "TestKcDeleteResumeChaos",
            #     EndToEndTestSuite(
            #         test_instance=TestKcDeleteResumeChaos(driver, nameSalt),
            #         run_in_confluent=True,
            #         run_in_apache=True,
            #         cloud_platform=CloudPlatform.AWS,
            #     ),
            # ),
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
            # ),
            # (
            #     "TestIcebergJsonAws",
            #     EndToEndTestSuite(
            #         test_instance=TestIcebergJsonAws(driver, nameSalt),
            #         run_in_confluent=True,
            #         run_in_apache=True,
            #         cloud_platform=CloudPlatform.AWS,
            #     ),
            # ),
            # (
            #     "TestIcebergAvroAws",
            #     EndToEndTestSuite(
            #         test_instance=TestIcebergAvroAws(driver, nameSalt),
            #         run_in_confluent=True,
            #         run_in_apache=False,
            #         cloud_platform=CloudPlatform.AWS,
            #     ),
            # ),
            # (
            #     "TestIcebergSchemaEvolutionJsonAws",
            #     EndToEndTestSuite(
            #         test_instance=TestIcebergSchemaEvolutionJsonAws(driver, nameSalt),
            #         run_in_confluent=True,
            #         run_in_apache=True,
            #         cloud_platform=CloudPlatform.AWS,
            #     ),
            # ),
            # (
            #     "TestIcebergSchemaEvolutionAvroAws",
            #     EndToEndTestSuite(
            #         test_instance=TestIcebergSchemaEvolutionAvroAws(driver, nameSalt),
            #         run_in_confluent=True,
            #         run_in_apache=False,
            #         cloud_platform=CloudPlatform.AWS,
            #     ),
            # ),
        ]
    )
