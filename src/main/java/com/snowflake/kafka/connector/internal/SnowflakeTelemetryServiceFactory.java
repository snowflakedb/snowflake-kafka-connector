package com.snowflake.kafka.connector.internal;

import java.sql.Connection;

class SnowflakeTelemetryServiceFactory
{

  static SnowflakeTelemetryServiceBuilder builder(Connection conn)
  {
    return new SnowflakeTelemetryServiceBuilder(conn);
  }

  static class SnowflakeTelemetryServiceBuilder extends Logging
  {
    private final SnowflakeTelemetryService service;
    SnowflakeTelemetryServiceBuilder(Connection conn)
    {
      this.service = new SnowflakeTelemetryServiceV1(conn);
    }

    SnowflakeTelemetryServiceBuilder setAppName(String name)
    {
      this.service.setAppName(name);
      return this;
    }

    SnowflakeTelemetryServiceBuilder setTaskID(String taskID)
    {
      this.service.setTaskID(taskID);
      return this;
    }

    SnowflakeTelemetryService build()
    {
      return this.service;
    }
  }
}
