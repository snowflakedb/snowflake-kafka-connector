package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SinkTaskConfigTestBuilder;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
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
    SinkTaskConfig config =
        SinkTaskConfigTestBuilder.builder()
            .connectorName("test")
            .taskId("0")
            .authorizationTaskFailureEnabled(true)
            .build();
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
    SinkTaskConfig config =
        SinkTaskConfigTestBuilder.builder()
            .connectorName("test")
            .taskId("0")
            .authorizationTaskFailureEnabled(true)
            .build();
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
    SinkTaskConfig config =
        SinkTaskConfigTestBuilder.builder()
            .connectorName("test")
            .taskId("0")
            .authorizationTaskFailureEnabled(enabled)
            .build();
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
