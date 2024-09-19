package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS;

import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class SnowflakeSinkTaskAuthorizationExceptionTrackerTest {

  @Test
  public void shouldThrowExceptionOnAuthorizationError() {
    // given
    SnowflakeSinkTaskAuthorizationExceptionTracker tracker =
        new SnowflakeSinkTaskAuthorizationExceptionTracker();
    Map<String, String> config = TestUtils.getConfig();
    config.put(ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS, "true");
    tracker.updateStateOnTaskStart(config);

    // when
    tracker.reportPrecommitException(new Exception("Authorization failed after retry"));

    // then
    Assertions.assertThrows(
        SnowflakeKafkaConnectorException.class, tracker::throwExceptionIfAuthorizationFailed);
  }

  @Test
  public void shouldNotThrowExceptionWhenNoExceptionReported() {
    // given
    SnowflakeSinkTaskAuthorizationExceptionTracker tracker =
        new SnowflakeSinkTaskAuthorizationExceptionTracker();
    Map<String, String> config = TestUtils.getConfig();
    config.put(ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS, "true");
    tracker.updateStateOnTaskStart(config);

    // expect
    Assertions.assertDoesNotThrow(tracker::throwExceptionIfAuthorizationFailed);
  }

  @ParameterizedTest
  @MethodSource("noExceptionConditions")
  public void shouldNotThrowException(boolean enabled, String exceptionMessage) {
    // given
    SnowflakeSinkTaskAuthorizationExceptionTracker tracker =
        new SnowflakeSinkTaskAuthorizationExceptionTracker();
    Map<String, String> config = TestUtils.getConfig();
    config.put(ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS, Boolean.toString(enabled));
    tracker.updateStateOnTaskStart(config);

    // when
    tracker.reportPrecommitException(new Exception(exceptionMessage));

    // then
    Assertions.assertDoesNotThrow(tracker::throwExceptionIfAuthorizationFailed);
  }

  public static Stream<Arguments> noExceptionConditions() {
    return Stream.of(
        Arguments.of(false, "Authorization failed after retry"),
        Arguments.of(true, "NullPointerException"));
  }
}
