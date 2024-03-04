package com.snowflake.kafka.connector.internal.telemetry;

import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryServiceV2;
import java.sql.Connection;

/**
 * Factory class which produces the telemetry service which essentially has a telemetry client
 * instance.
 */
public class SnowflakeTelemetryServiceFactory {

  public static SnowflakeTelemetryServiceBuilder builder(
      Connection conn, IngestionMethodConfig ingestionMethodConfig) {
    return new SnowflakeTelemetryServiceBuilder(conn, ingestionMethodConfig);
  }

  /** Builder for TelemetryService */
  public static class SnowflakeTelemetryServiceBuilder {
    private final SnowflakeTelemetryService service;

    /**
     * @param conn snowflake connection is required for telemetry service
     * @param ingestionMethodConfig Snowpipe or Snowpipe Streaming
     */
    public SnowflakeTelemetryServiceBuilder(
        Connection conn, IngestionMethodConfig ingestionMethodConfig) {
      if (ingestionMethodConfig.equals(IngestionMethodConfig.SNOWPIPE)) {
        this.service = new SnowflakeTelemetryServiceV1(conn);
      } else {
        this.service = new SnowflakeTelemetryServiceV2(conn);
      }
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
