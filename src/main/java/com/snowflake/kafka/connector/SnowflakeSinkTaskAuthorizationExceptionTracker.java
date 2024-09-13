package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS_DEFAULT;
import static com.snowflake.kafka.connector.internal.SnowflakeErrors.ERROR_1005;

import java.util.Map;

/**
 * When the user rotates Snowflake key that is stored in an external file the Connector hangs and
 * does not mark its tasks as failed. To fix this corner case we need to track the authorization
 * exception thrown during preCommit() and stop tasks during put().
 *
 * <p>Note that exceptions thrown during preCommit() are swallowed by Kafka Connect and will not
 * cause task failure.
 */
public class SnowflakeSinkTaskAuthorizationExceptionTracker {

  private static final String AUTHORIZATION_EXCEPTION_MESSAGE = "Authorization failed after retry";

  private boolean authorizationTaskFailureEnabled;
  private boolean authorizationErrorReported;

  public SnowflakeSinkTaskAuthorizationExceptionTracker() {
    this.authorizationTaskFailureEnabled = true;
    this.authorizationErrorReported = false;
  }

  public void updateStateOnTaskStart(Map<String, String> taskConfig) {
    authorizationTaskFailureEnabled =
        Boolean.parseBoolean(
            taskConfig.getOrDefault(
                ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS,
                Boolean.toString(ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS_DEFAULT)));
  }

  /**
   * Check if the thrown exception is related to authorization
   *
   * @param ex - any exception that occurred during preCommit
   */
  public void reportPrecommitException(Exception ex) {
    if (ex.getMessage().contains(AUTHORIZATION_EXCEPTION_MESSAGE)) {
      authorizationErrorReported = true;
    }
  }

  /** Throw exception if authorization has failed before */
  public void throwExceptionIfAuthorizationFailed() {
    if (authorizationTaskFailureEnabled && authorizationErrorReported) {
      throw ERROR_1005.getException();
    }
  }
}
