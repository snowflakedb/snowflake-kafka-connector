from test_suites import create_end_to_end_test_suites
from collections import OrderedDict
from test_suites import EndToEndTestSuite
from cloud_platform import CloudPlatform
import test_suit


# TestSelector is responsible for selecting a subset of tests to be run
# It is meant to filter tests by platform, cloud vendor or any other predicate needed
class TestSelector:

    def select_tests_to_be_run(
        self,
        driver,
        name_salt: str,
        schema_registry_address: str,
        test_platform: str,
        cloud_platform: CloudPlatform,
        allowed_tests_csv: str,
    ):
        test_suites = create_end_to_end_test_suites(
            driver, name_salt, schema_registry_address, test_platform
        )
        test_suites = self.__filter_by_allow_list(allowed_tests_csv, test_suites)
        test_suites = self.__filter_by_test_platform(test_platform, test_suites)
        test_suites = self.__filter_by_cloud_platform(cloud_platform, test_suites)

        print("Running", len(test_suites), "tests")

        return [
            single_end_to_end_test.test_instance
            for single_end_to_end_test in test_suites.values()
        ]

    def __filter_by_allow_list(
        self,
        allowed_tests_csv: str,
        tests_to_be_filtered: OrderedDict[str, EndToEndTestSuite],
    ) -> OrderedDict[str, EndToEndTestSuite]:
        if allowed_tests_csv is None or allowed_tests_csv == "":
            return tests_to_be_filtered
        else:
            testsToRun = dict(
                (k, v)
                for k, v in tests_to_be_filtered.items()
                if k in allowed_tests_csv.split(",")
            )
            return testsToRun

    def __filter_by_test_platform(
        self,
        test_platform: str,
        tests_to_be_filtered: OrderedDict[str, EndToEndTestSuite],
    ) -> OrderedDict[str, EndToEndTestSuite]:
        if test_platform == "apache":
            return dict(
                (k, v)
                for k, v in tests_to_be_filtered.items()
                if v.run_in_apache == True
            )
        elif test_platform == "confluent":
            return dict(
                (k, v)
                for k, v in tests_to_be_filtered.items()
                if v.run_in_confluent == True
            )
        elif test_platform == "clean":
            return tests_to_be_filtered
        else:
            raise test_suit.test_utils.NonRetryableError(
                "unknown test_platform={}".format(test_platform)
            )

    def __filter_by_cloud_platform(
        self,
        cloud_platform: CloudPlatform,
        tests_to_be_filtered: OrderedDict[str, EndToEndTestSuite],
    ) -> OrderedDict[str, EndToEndTestSuite]:
        if cloud_platform == CloudPlatform.ALL:
            return tests_to_be_filtered
        else:
            return dict(
                (k, v)
                for k, v in tests_to_be_filtered.items()
                if v.cloud_platform in (cloud_platform, CloudPlatform.ALL)
            )
