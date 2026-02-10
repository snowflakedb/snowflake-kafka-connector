package com.snowflake.kafka.connector.internal.telemetry;

import java.sql.Connection;

/**
 * Factory class which produces the telemetry service which essentially has a telemetry client
 * instance.
 */
public final class SnowflakeTelemetryServiceFactory {

  private SnowflakeTelemetryServiceFactory() {}

  public static SnowflakeTelemetryServiceBuilder builder(Connection conn) {
    return new SnowflakeTelemetryServiceBuilder(conn);
  }

  /** Builder for TelemetryService */
  public static final class SnowflakeTelemetryServiceBuilder {
    private final SnowflakeTelemetryService service;

    /**
     * @param conn snowflake connection is required for telemetry service
     */
    SnowflakeTelemetryServiceBuilder(Connection conn) {
      this.service = new SnowflakeTelemetryService(conn);
    }

    /**
     * @param name connector name
     * @return builder instance
     */
    public SnowflakeTelemetryServiceBuilder setAppName(String name) {
      this.service.setAppName(name);
      return this;
    }

    /**
     * @param taskID taskId
     * @return builder instance
     */
    public SnowflakeTelemetryServiceBuilder setTaskID(String taskID) {
      this.service.setTaskID(taskID);
      return this;
    }

    public SnowflakeTelemetryService build() {
      return this.service;
    }
  }
}
