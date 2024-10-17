from test_suites import create_end_to_end_test_suites
import test_suit

# TestSelector is responsible for selecting a subset of tests to be run
# It is meant to filter tests by platform, cloud vendor or any other predicate needed
class TestSelector:

    def selectTestsToBeRun(self, driver, nameSalt, schemaRegistryAddress, testPlatform, allowedTestsCsv):
        test_suites = create_end_to_end_test_suites(driver, nameSalt, schemaRegistryAddress, testPlatform, allowedTestsCsv)

        if testPlatform == "apache":
            return [single_end_to_end_test.test_instance for single_end_to_end_test in test_suites.values() if single_end_to_end_test.run_in_apache == True]
        elif testPlatform == "confluent":
            return [single_end_to_end_test.test_instance for single_end_to_end_test in test_suites.values() if single_end_to_end_test.run_in_confluent == True]
        elif testPlatform == "clean":
            return [single_end_to_end_test.test_instance for single_end_to_end_test in test_suites.values()]
        else:
            raise test_suit.test_utils.NonRetryableError("unknown testPlatform={}".format(testPlatform))
