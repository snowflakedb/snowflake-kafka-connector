package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.security.PrivateKey;

/** A factory to create {@link SnowflakeIngestionService} */
public class SnowflakeIngestionServiceFactory {

  public static SnowflakeIngestionServiceBuilder builder(
      String accountName,
      String userName,
      String host,
      int port,
      String connectionScheme,
      String stageName,
      String pipeName,
      PrivateKey privateKey,
      String userAgentSuffix) {
    return new SnowflakeIngestionServiceBuilder(
        accountName,
        userName,
        host,
        port,
        connectionScheme,
        stageName,
        pipeName,
        privateKey,
        userAgentSuffix);
  }

  /** Builder class to create instance of {@link SnowflakeIngestionService} */
  static class SnowflakeIngestionServiceBuilder {
    private final SnowflakeIngestionService service;

    private SnowflakeIngestionServiceBuilder(
        String accountName,
        String userName,
        String host,
        int port,
        String connectionScheme,
        String stageName,
        String pipeName,
        PrivateKey privateKey,
        String userAgentSuffix) {
      this.service =
          new SnowflakeIngestionServiceV1(
              accountName,
              userName,
              host,
              port,
              connectionScheme,
              stageName,
              pipeName,
              privateKey,
              userAgentSuffix);
    }

    SnowflakeIngestionServiceBuilder setTelemetry(SnowflakeTelemetryService telemetry) {
      service.setTelemetry(telemetry);
      return this;
    }

    SnowflakeIngestionService build() {
      return service;
    }
  }
}
