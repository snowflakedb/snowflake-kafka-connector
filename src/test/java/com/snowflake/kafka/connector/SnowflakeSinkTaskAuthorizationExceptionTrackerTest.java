package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS;

import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

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

  // --- structural detection -------------------------------------------------

  /**
   * Regression: a null message previously caused a NullPointerException inside
   * reportPrecommitException, thrown from within preCommit's own catch block.
   */
  @Test
  public void shouldTolerateExceptionWithNullMessage() {
    Assertions.assertDoesNotThrow(
        () ->
            SnowflakeSinkTaskAuthorizationExceptionTracker.isAuthorizationFailure(new Exception()));
    Assertions.assertFalse(
        SnowflakeSinkTaskAuthorizationExceptionTracker.isAuthorizationFailure(new Exception()));
  }

  @Test
  public void shouldDetectAuthorizationFailureFromSqlState() {
    Assertions.assertTrue(
        SnowflakeSinkTaskAuthorizationExceptionTracker.isAuthorizationFailure(
            new SQLException("access denied", "28000")));
    // the driver also defines 28P01 in the same SQLState class
    Assertions.assertTrue(
        SnowflakeSinkTaskAuthorizationExceptionTracker.isAuthorizationFailure(
            new SQLException("bad password", "28P01")));
  }

  /**
   * The driver and the streaming SDK both wrap authorization failures in higher-level exceptions.
   */
  @Test
  public void shouldDetectAuthorizationFailureNestedInCauseChain() {
    Exception nested =
        new IllegalStateException(
            "failed to flush channel", new RuntimeException(new SQLException("denied", "28000")));

    Assertions.assertTrue(
        SnowflakeSinkTaskAuthorizationExceptionTracker.isAuthorizationFailure(nested));
  }

  @Test
  public void shouldNotDetectNonAuthorizationSqlState() {
    Assertions.assertFalse(
        SnowflakeSinkTaskAuthorizationExceptionTracker.isAuthorizationFailure(
            new SQLException("data exception", "22000")));
    Assertions.assertFalse(
        SnowflakeSinkTaskAuthorizationExceptionTracker.isAuthorizationFailure(
            new SQLException("no sqlstate at all")));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "Authorization failed after retry",
        "authorization failed after retry",
        "OAuth Access Token Expired",
        "Token is expired"
      })
  public void shouldDetectKnownAuthorizationMessagesCaseInsensitively(String message) {
    Assertions.assertTrue(
        SnowflakeSinkTaskAuthorizationExceptionTracker.isAuthorizationFailure(
            new Exception(message)));
  }

  @Test
  public void shouldDetectAuthorizationMessageNestedInCauseChain() {
    Exception nested =
        new IllegalStateException("preCommit failed", new Exception("Token is expired"));

    Assertions.assertTrue(
        SnowflakeSinkTaskAuthorizationExceptionTracker.isAuthorizationFailure(nested));
  }

  /** A self-referential cause chain must terminate rather than loop forever. */
  @Test
  public void shouldTerminateOnCyclicCauseChain() {
    Exception first = new Exception("outer");
    Exception second = new Exception("inner", first);
    first.initCause(second);

    Assertions.assertDoesNotThrow(
        () -> SnowflakeSinkTaskAuthorizationExceptionTracker.isAuthorizationFailure(first));
    Assertions.assertFalse(
        SnowflakeSinkTaskAuthorizationExceptionTracker.isAuthorizationFailure(first));
  }

  /** Structural detection must still respect the opt-in flag. */
  @Test
  public void shouldNotFailTaskOnSqlStateWhenFeatureDisabled() {
    SnowflakeSinkTaskAuthorizationExceptionTracker tracker =
        new SnowflakeSinkTaskAuthorizationExceptionTracker();
    Map<String, String> config = new HashMap<>();
    config.put(ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS, "false");
    tracker.updateStateOnTaskStart(config);

    tracker.reportPrecommitException(new SQLException("denied", "28000"));

    Assertions.assertDoesNotThrow(tracker::throwExceptionIfAuthorizationFailed);
  }

  @Test
  public void shouldFailTaskOnSqlStateWhenFeatureEnabled() {
    SnowflakeSinkTaskAuthorizationExceptionTracker tracker =
        new SnowflakeSinkTaskAuthorizationExceptionTracker();
    Map<String, String> config = new HashMap<>();
    config.put(ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS, "true");
    tracker.updateStateOnTaskStart(config);

    tracker.reportPrecommitException(
        new IllegalStateException("flush failed", new SQLException("denied", "28000")));

    Assertions.assertThrows(
        SnowflakeKafkaConnectorException.class, tracker::throwExceptionIfAuthorizationFailed);
  }
}
