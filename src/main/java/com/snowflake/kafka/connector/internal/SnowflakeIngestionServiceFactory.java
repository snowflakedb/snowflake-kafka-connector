package com.snowflake.kafka.connector.internal;

import java.security.PrivateKey;

/**
 * A factory to create {@link SnowflakeIngestionService}
 */
public class SnowflakeIngestionServiceFactory
{

  public static SnowflakeIngestionServiceBuilder builder(String accountName,
                                                         String userName,
                                                         String host,
                                                         int port,
                                                         String connectionScheme,
                                                         String stageName,
                                                         String pipeName,
                                                         PrivateKey privateKey)
  {
    return new SnowflakeIngestionServiceBuilder(accountName, userName, host, port,
                                                connectionScheme, stageName, pipeName, privateKey);
  }

  /**
   * Builder class to create instance of {@link SnowflakeIngestionService}
   */
  static class SnowflakeIngestionServiceBuilder extends Logging
  {
    private final SnowflakeIngestionService service;

    private SnowflakeIngestionServiceBuilder(String accountName,
                                             String userName, String host,
                                             int port, String connectionScheme,
                                             String stageName,
                                             String pipeName,
                                             PrivateKey privateKey)
    {
      this.service = new SnowflakeIngestionServiceV1(accountName, userName,
        host, port, connectionScheme, stageName, pipeName, privateKey);
    }

    SnowflakeIngestionServiceBuilder setTelemetry(SnowflakeTelemetryService telemetry)
    {
      service.setTelemetry(telemetry);
      return this;
    }

    SnowflakeIngestionService build()
    {
      return service;
    }


  }
}
