package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS_DEFAULT;
import static com.snowflake.kafka.connector.internal.SnowflakeErrors.ERROR_1005;

import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Detects authorization failures reported during {@code preCommit()} so the task can be failed
 * during {@code put()}.
 *
 * <p>Exceptions thrown from {@code preCommit()} are swallowed by Kafka Connect and never fail the
 * task. Without this tracker a connector whose credential has stopped working keeps running and
 * silently makes no progress, which is far harder to diagnose than a failed task.
 *
 * <p>Detection is structural first and textual second:
 *
 * <ol>
 *   <li>Any {@link SQLException} in the cause chain whose {@code SQLState} is in class {@code 28}
 *       ("invalid authorization specification"). This is the SQL standard class for authorization
 *       failure and is what the Snowflake JDBC driver reports, so it survives changes to error
 *       wording.
 *   <li>Failing that, a small set of known message fragments, matched case-insensitively.
 * </ol>
 *
 * <p>The whole cause chain is inspected, because the driver wraps authorization failures inside
 * higher-level exceptions and the outermost message frequently says nothing about authorization.
 *
 * <p><b>SSv2 scope note:</b> for SSv2 streaming tasks, {@code BatchOffsetFetcher} catches
 * auth-related {@code SFException} before they reach this tracker. Auth failures on the SSv2
 * streaming path are not currently detected here.
 */
public class SnowflakeSinkTaskAuthorizationExceptionTracker {

  /**
   * SQL standard SQLState class for "invalid authorization specification". The Snowflake JDBC
   * driver defines both {@code 28000} and {@code 28P01} in this class, so the two-character prefix
   * is matched rather than a specific value.
   */
  @VisibleForTesting static final String SQL_STATE_INVALID_AUTHORIZATION_CLASS = "28";

  /**
   * Snowflake server error codes that mean the credential itself was rejected.
   *
   * <p>These are needed because Snowflake does <b>not</b> report OAuth token rejection under
   * SQLState class {@code 28}. It reports SQLState {@code 08001}, the generic connection class, and
   * distinguishes the cause only by error code. Measured against a live deployment:
   *
   * <pre>
   *   garbage token                    08001 / 390303  "Invalid OAuth access token."
   *   empty token                      08001 / 390303  "Invalid OAuth access token."
   *   well-formed token, bad signature 08001 / 390303  "Invalid OAuth access token."
   *   valid token, mismatched user     08001 / 390309  "The user you were trying to authenticate
   *                                                    as differs from the user tied to the token"
   * </pre>
   *
   * <p>Without these codes a task retries indefinitely against a credential the server is actively
   * rejecting, which is the situation this tracker exists to stop.
   *
   * <p>390318 ({@code OAUTH_ACCESS_TOKEN_EXPIRED}) is deliberately absent: an expired token can be
   * refreshed automatically when the connector is configured with a refresh token or client
   * credentials flow, so expiry is potentially self-healing. 390303 and 390309 identify credentials
   * that are permanently invalid regardless of any refresh configuration.
   */
  @VisibleForTesting
  static final Set<Integer> AUTHORIZATION_FAILURE_ERROR_CODES =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(390303, 390309)));

  /**
   * Message fragments that indicate an authorization failure, lower-cased for case-insensitive
   * matching.
   *
   * <p>Kept deliberately short. Every entry is verified against a shipped dependency; nothing is
   * speculative. Message matching covers JDBC-path exceptions that carry no {@code SQLState} (for
   * example, OAuth flow errors logged by the JDBC driver before they are wrapped in a {@code
   * SQLException}). It does <b>not</b> cover SSv2 streaming SDK failures: the streaming SDK throws
   * {@code SFException} which {@code BatchOffsetFetcher} catches before it can reach this tracker.
   */
  @VisibleForTesting
  static final List<String> AUTHORIZATION_FAILURE_MARKERS =
      Collections.unmodifiableList(
          Arrays.asList(
              // Historical message from the classic Snowpipe ingest SDK. Retained so behavior is
              // unchanged for anyone relying on it.
              "authorization failed after retry",
              // Matches snowflake-jdbc SessionUtil "OAuth Access Token Expired: {}".
              "oauth access token expired",
              // Matches snowflake-jdbc (Google OAuth) TokenVerifier "Token is expired".
              "token is expired"));

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
   * Records whether the given exception represents an authorization failure.
   *
   * @param ex any exception that occurred during preCommit; may have a null message
   */
  public void reportPrecommitException(Exception ex) {
    if (isAuthorizationFailure(ex)) {
      authorizationErrorReported = true;
    }
  }

  /** Throw exception if authorization has failed before */
  public void throwExceptionIfAuthorizationFailed() {
    if (authorizationTaskFailureEnabled && authorizationErrorReported) {
      throw ERROR_1005.getException();
    }
  }

  /**
   * Walks the cause chain looking for an authorization failure.
   *
   * <p>Tolerates a null message at any level, and terminates on a cyclic chain rather than looping
   * forever.
   */
  @VisibleForTesting
  static boolean isAuthorizationFailure(Throwable ex) {
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable current = ex;
        current != null && visited.add(current);
        current = current.getCause()) {
      if (current instanceof SQLException) {
        String sqlState = ((SQLException) current).getSQLState();
        if (sqlState != null && sqlState.startsWith(SQL_STATE_INVALID_AUTHORIZATION_CLASS)) {
          return true;
        }
        // OAuth rejections arrive as SQLState 08001, so the error code is the only structural
        // signal available. See AUTHORIZATION_FAILURE_ERROR_CODES.
        if (AUTHORIZATION_FAILURE_ERROR_CODES.contains(((SQLException) current).getErrorCode())) {
          return true;
        }
      }

      String message = current.getMessage();
      if (message != null) {
        String normalized = message.toLowerCase(Locale.ROOT);
        for (String marker : AUTHORIZATION_FAILURE_MARKERS) {
          if (normalized.contains(marker)) {
            return true;
          }
        }
      }
    }
    return false;
  }
}
