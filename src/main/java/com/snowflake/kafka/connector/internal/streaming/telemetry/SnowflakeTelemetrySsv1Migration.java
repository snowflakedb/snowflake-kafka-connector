package com.snowflake.kafka.connector.internal.streaming.telemetry;

import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.SSV1_CHANNEL_NAME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.SSV1_MIGRATED_OFFSET;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.SSV1_MIGRATION_MODE;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.SSV1_MIGRATION_OUTCOME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.TABLE_NAME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.TOPIC_PARTITION_CHANNEL_NAME;

import com.snowflake.kafka.connector.internal.streaming.v2.migration.Ssv1MigrationMode;
import com.snowflake.kafka.connector.internal.streaming.v2.migration.Ssv1MigrationResponse;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryBasicInfo;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.util.Locale;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

/**
 * One-shot telemetry event sent when SSv1 offset migration is attempted for a channel. Only emitted
 * when the migration mode is not SKIP and the SSv2 channel has no committed offset yet.
 */
public class SnowflakeTelemetrySsv1Migration extends SnowflakeTelemetryBasicInfo {
  private final String channelName;
  private final String ssv1ChannelName;
  private final Ssv1MigrationMode migrationMode;
  private final Ssv1MigrationResponse response;

  public SnowflakeTelemetrySsv1Migration(
      String tableName,
      String channelName,
      String ssv1ChannelName,
      Ssv1MigrationMode migrationMode,
      Ssv1MigrationResponse response) {
    super(tableName, SnowflakeTelemetryService.TelemetryType.KAFKA_SSV1_MIGRATION);
    this.channelName = channelName;
    this.ssv1ChannelName = ssv1ChannelName;
    this.migrationMode = migrationMode;
    this.response = response;
  }

  @Override
  public void dumpTo(ObjectNode msg) {
    msg.put(TABLE_NAME, this.tableName);
    msg.put(TOPIC_PARTITION_CHANNEL_NAME, this.channelName);
    msg.put(SSV1_CHANNEL_NAME, this.ssv1ChannelName);
    msg.put(SSV1_MIGRATION_MODE, this.migrationMode.name().toLowerCase(Locale.ROOT));
    msg.put(SSV1_MIGRATION_OUTCOME, deriveOutcome());
    Long offset = this.response.getMigratedOffset();
    if (offset != null) {
      msg.put(SSV1_MIGRATED_OFFSET, offset);
    }
  }

  private String deriveOutcome() {
    if (response.getMigratedOffset() != null) {
      return "migrated";
    } else if (!response.isSsv1ChannelFound()) {
      return migrationMode == Ssv1MigrationMode.STRICT ? "ssv1_not_found_strict" : "ssv1_not_found";
    } else {
      return "ssv1_no_offset";
    }
  }

  @Override
  public boolean isEmpty() {
    throw new IllegalStateException("isEmpty does not apply to " + this.getClass().getSimpleName());
  }
}
