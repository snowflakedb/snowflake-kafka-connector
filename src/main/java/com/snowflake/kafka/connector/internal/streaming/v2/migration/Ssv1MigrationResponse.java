package com.snowflake.kafka.connector.internal.streaming.v2.migration;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import javax.annotation.Nullable;

/**
 * Deserialized response from SYSTEM$MIGRATE_SSV1_CHANNEL_OFFSET. The three possible outcomes are:
 *
 * <ul>
 *   <li>{@code ssv1ChannelFound == false} — the SSv1 channel does not exist
 *   <li>{@code ssv1ChannelFound == true, migratedOffset == null} — channel exists but has no
 *       committed offset
 *   <li>{@code ssv1ChannelFound == true, migratedOffset != null} — offset was migrated successfully
 * </ul>
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Ssv1MigrationResponse {

  @JsonProperty("ssv1_channel_found")
  boolean ssv1ChannelFound;

  @Nullable
  @JsonProperty("migrated_offset")
  Long migratedOffset;

  /** Creates a response representing a channel that was not found. */
  @VisibleForTesting
  public static Ssv1MigrationResponse channelNotFound() {
    Ssv1MigrationResponse response = new Ssv1MigrationResponse();
    response.ssv1ChannelFound = false;
    return response;
  }

  /** Creates a response representing a channel that exists but has no committed offset. */
  @VisibleForTesting
  public static Ssv1MigrationResponse channelFoundNoOffset() {
    Ssv1MigrationResponse response = new Ssv1MigrationResponse();
    response.ssv1ChannelFound = true;
    return response;
  }

  /** Creates a response representing a successful migration with the given offset. */
  @VisibleForTesting
  public static Ssv1MigrationResponse migrated(long offset) {
    Ssv1MigrationResponse response = new Ssv1MigrationResponse();
    response.ssv1ChannelFound = true;
    response.migratedOffset = offset;
    return response;
  }

  public boolean isSsv1ChannelFound() {
    return ssv1ChannelFound;
  }

  @Nullable
  public Long getMigratedOffset() {
    return migratedOffset;
  }
}
