package net.snowflake.client.internal.jdbc.telemetry;

import java.sql.Connection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.logging.Logger;

/**
 * TEMPORARY SHIM — see {@code package-info}. No-op telemetry client that stands in for the classic
 * snowflake-jdbc {@code TelemetryClient} until snowflake-jdbc-native (private preview) exposes a
 * real in-band JDBC telemetry API. Every method is a no-op; connector telemetry is effectively
 * disabled when running against snowflake-jdbc-native.
 */
public class TelemetryClient implements Telemetry {

  private static final Logger LOGGER = Logger.getLogger(TelemetryClient.class.getName());

  /**
   * Drop-in replacement for the classic factory method used by the connector. Returns a no-op
   * client because snowflake-jdbc-native does not yet ship a JDBC telemetry implementation.
   */
  public static Telemetry createTelemetry(Connection conn) {
    LOGGER.warning(
        "snowflake-jdbc-native does not expose a JDBC telemetry API yet; "
            + "connector telemetry is disabled (no-op shim). See "
            + "net.snowflake.client.internal.jdbc.telemetry package-info.");
    return new TelemetryClient();
  }

  @Override
  public void addLogToBatch(TelemetryData log) {
    // no-op
  }

  @Override
  public void close() {
    // no-op
  }

  @Override
  public Future<Boolean> sendBatchAsync() {
    return CompletableFuture.completedFuture(Boolean.FALSE);
  }

  @Override
  public void postProcess(String queryId, String sqlState, int vendorCode, Throwable ex) {
    // no-op
  }
}
