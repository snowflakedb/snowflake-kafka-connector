package net.snowflake.client.internal.jdbc.telemetry;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * TEMPORARY SHIM — see {@code package-info}. Minimal stand-in for the classic snowflake-jdbc
 * telemetry payload, which snowflake-jdbc-native (private preview) does not yet expose.
 */
public class TelemetryData {

  private final ObjectNode message;
  private final long timeStamp;

  public TelemetryData(ObjectNode message, long timeStamp) {
    this.message = message;
    this.timeStamp = timeStamp;
  }

  public ObjectNode getMessage() {
    return message;
  }

  public long getTimeStamp() {
    return timeStamp;
  }
}
