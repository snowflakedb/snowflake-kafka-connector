from test_suit.test_utils import NonRetryableError


def assert_equals(expected, actual):
    if expected != actual:
        raise NonRetryableError(
            "Actual {} does not equal expected {}".format(actual, expected)
        )


def assert_equals_with_precision(expected, actual, precision=0.1):
    if not expected - precision < actual < expected + precision:
        raise NonRetryableError(
            "Actual {} does not equal expected {} with precision {}".format(
                actual, expected, precision
            )
        )


def assert_starts_with(expected_prefix, actual):
    if not actual.startswith(expected_prefix):
        raise NonRetryableError(
            "Actual {} does not start with {}".format(expected_prefix, actual)
        )


def assert_not_null(actual):
    if actual is None:
        raise NonRetryableError("Actual {} is null".format(actual))


def assert_dict_contains(expected_key, expected_value, actual_dict):
    if actual_dict[expected_key] != expected_value:
        raise NonRetryableError(
            "Actual value from dict {} does not equal expected {}".format(
                actual_dict[expected_key], expected_value
            )
        )
