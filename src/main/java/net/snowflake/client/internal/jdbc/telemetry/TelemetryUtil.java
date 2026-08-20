package net.snowflake.client.internal.jdbc.telemetry;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * TEMPORARY SHIM — see {@code package-info}. Provides the single {@code buildJobData} helper the
 * connector uses from the classic snowflake-jdbc {@code TelemetryUtil}, which snowflake-jdbc-native
 * (private preview) does not yet expose.
 */
public final class TelemetryUtil {

  private TelemetryUtil() {}

  public static TelemetryData buildJobData(ObjectNode message) {
    return new TelemetryData(message, System.currentTimeMillis());
  }
}
