package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceV1;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import dev.failsafe.Failsafe;
import dev.failsafe.Fallback;
import dev.failsafe.RetryPolicy;
import java.time.Duration;

/**
 * Encapsulates logic around offset token migration that is required to avoid data duplication when
 * upgrading the connector from version 2.1.0. (see PROD-39429 for details).
 */
public class ChannelOffsetTokenMigrator {

  private static final KCLogger LOGGER = new KCLogger(ChannelOffsetTokenMigrator.class.getName());

  private static final Duration CHANNEL_MIGRATION_RETRY_DELAY = Duration.ofSeconds(1);

  private static final int CHANNEL_MIGRATION_RETRY_MAX_ATTEMPTS = 3;

  private final SnowflakeConnectionService snowflakeConnectionService;

  private final SnowflakeTelemetryService telemetryService;

  public ChannelOffsetTokenMigrator(
      SnowflakeConnectionService snowflakeConnectionService,
      SnowflakeTelemetryService telemetryService) {
    this.snowflakeConnectionService = snowflakeConnectionService;
    this.telemetryService = telemetryService;
  }

  private static RetryPolicy<ChannelMigrateOffsetTokenResponseDTO> channelMigrationRetryPolicy() {
    return RetryPolicy.<ChannelMigrateOffsetTokenResponseDTO>builder()
        .handle(SnowflakeConnectionServiceV1.OffsetTokenMigrationRetryableException.class)
        .withDelay(CHANNEL_MIGRATION_RETRY_DELAY)
        .withMaxAttempts(CHANNEL_MIGRATION_RETRY_MAX_ATTEMPTS)
        .onRetry(
            event ->
                LOGGER.warn(
                    "Channel offset token migration retry no:{}, message:{}",
                    event.getAttemptCount(),
                    event.getLastException().getMessage()))
        .build();
  }

  public void migrateChannelOffsetWithRetry(
      String tableName, String sourceChannelName, String destinationChannelName) {
    Fallback<ChannelMigrateOffsetTokenResponseDTO> fallback =
        Fallback.ofException(
            e -> {
              LOGGER.error("Channel offset token migration - max retry attempts", e);
              throw SnowflakeErrors.ERROR_5023.getException(
                  e.getLastException().getMessage(), this.telemetryService);
            });

    Failsafe.with(fallback)
        .compose(channelMigrationRetryPolicy())
        .get(
            () ->
                snowflakeConnectionService.migrateStreamingChannelOffsetToken(
                    tableName, sourceChannelName, destinationChannelName));
  }
}
