
from datetime import datetime
import logging
import os
from pathlib import Path
import sys

from lib.config import Profile
from lib.driver import KafkaDriver
from test_executor import TestExecutor
from test_selector import TestSelector

from cloud_platform import CloudPlatform


def errorExit(message):
    print(datetime.now().strftime("%H:%M:%S "), message)
    exit(1)


def runTestSet(driver, testSet, nameSalt, enable_stress_test, skipProxy, cloud_platform, allowedTestsCsv):
    ############################ round 1 ############################
    print(datetime.now().strftime("\n%H:%M:%S "), "=== Round 1 ===")

    testSelector = TestSelector()
    end_to_end_tests_suite = testSelector.select_tests_to_be_run(driver, nameSalt, schemaRegistryAddress, testSet, cloud_platform, allowedTestsCsv)

    execution(testSet, end_to_end_tests_suite, driver, nameSalt)

    ############################ Always run Proxy tests in the end ############################

    ############################ Proxy End To End Test ############################
    # Don't run proxy tests locally
    # TODO: proxy test has been disabled after removing ssv1 support. Return to this when able
    # if skipProxy:
    #     return
    #
    # print("Running Proxy tests")
    #
    # from test_suit.test_string_json_proxy import TestStringJsonProxy
    # from test_suites import EndToEndTestSuite
    #
    # print(datetime.now().strftime("\n%H:%M:%S "), "=== Last Round: Proxy E2E Test ===")
    # print("Proxy Test should be the last test, since it modifies the JVM values")
    #
    # proxy_tests_suite = [EndToEndTestSuite(
    #     test_instance=TestStringJsonProxy(driver, nameSalt), run_in_confluent=True, run_in_apache=True, cloud_platform = CloudPlatform.ALL
    # )]
    #
    # end_to_end_proxy_tests_suite = [single_end_to_end_test.test_instance for single_end_to_end_test in proxy_tests_suite]
    #
    # execution(testSet, end_to_end_proxy_tests_suite, driver, nameSalt)
    ############################ Proxy End To End Test End ############################


def execution(testSet, testSuitList, driver, nameSalt, round=1):
    if testSet == "clean":
        for test in testSuitList:
            test.clean()
        print(datetime.now().strftime("\n%H:%M:%S "), "=== All clean done ===")
    else:
        testExecutor = TestExecutor()
        testExecutor.execute(testSuitList, driver, nameSalt, round)


def run_test_set_with_parameters(kafka_test: KafkaDriver, testSet, nameSalt, pressure, skipProxy, cloud_platform, allowedTestsCsv):
    runTestSet(kafka_test, testSet, nameSalt, pressure, skipProxy, cloud_platform, allowedTestsCsv)


def __parseCloudPlatform() -> CloudPlatform:
    if cloud_platform := os.environ.get("SF_CLOUD_PLATFORM", ""):
        return CloudPlatform[cloud_platform]
    else:
        print("No SF_CLOUD_PLATFORM defined. Fallback to ALL.")
        return CloudPlatform.ALL


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
        datefmt="%H:%M:%S",
    )

    if len(sys.argv) < 10:
        errorExit(
            """\n=== Usage: ./ingest.py <kafka address> <schema registry address> <kafka connect address>
             <test set> <test version> <name salt> <pressure> <enableSSL> <skipProxy> [allowedTestsCsv]===""")

    kafkaAddress = sys.argv[1]
    global schemaRegistryAddress
    schemaRegistryAddress = sys.argv[2]
    kafkaConnectAddress = sys.argv[3]
    testSet = sys.argv[4]
    testVersion = sys.argv[5]
    nameSalt = sys.argv[6]
    pressure = (sys.argv[7] == 'true')
    enableSSL = (sys.argv[8] == 'true')
    skipProxy = (sys.argv[9] == 'true')
    allowedTestsCsv = sys.argv[10] if len(sys.argv) == 11 else None

    if "SNOWFLAKE_CREDENTIAL_FILE" not in os.environ:
        errorExit(
            "\n=== Require environment variable SNOWFLAKE_CREDENTIAL_FILE but it's not set.  Aborting. ===")

    credentialPath = Path(os.environ['SNOWFLAKE_CREDENTIAL_FILE'])
    assert credentialPath.is_file(), f"Provided SNOWFLAKE_CREDENTIAL_FILE {credentialPath} does not exist."
    credentials = Profile.load(credentialPath)

    snowflakeCloudPlatform: CloudPlatform = __parseCloudPlatform()
    print("Running tests for platform {} and distribution {}".format(snowflakeCloudPlatform, testSet))

    driver = KafkaDriver(kafkaAddress, schemaRegistryAddress, kafkaConnectAddress, credentials, testVersion, enableSSL)
    runTestSet(driver, testSet, nameSalt, pressure, skipProxy, snowflakeCloudPlatform, allowedTestsCsv)
