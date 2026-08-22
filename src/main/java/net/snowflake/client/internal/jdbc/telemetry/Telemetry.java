package net.snowflake.client.internal.jdbc.telemetry;

import java.util.concurrent.Future;

/**
 * TEMPORARY SHIM — see {@code package-info}. Mirrors the subset of the classic snowflake-jdbc
 * {@code Telemetry} interface the connector relies on, so the connector compiles against
 * snowflake-jdbc-native (private preview), which does not yet expose this API.
 */
public interface Telemetry {

  void addLogToBatch(TelemetryData log);

  void close();

  Future<Boolean> sendBatchAsync();

  void postProcess(String queryId, String sqlState, int vendorCode, Throwable ex);
}
