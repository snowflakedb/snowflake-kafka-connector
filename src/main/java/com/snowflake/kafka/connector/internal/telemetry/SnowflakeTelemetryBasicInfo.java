package com.snowflake.kafka.connector.internal.telemetry;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.snowflake.kafka.connector.internal.KCLogger;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

/** Minimum information needed to sent to Snowflake through Telemetry API */
public abstract class SnowflakeTelemetryBasicInfo {
  final String tableName;

  /**
   * User created or user visible stage name.
   *
   * <p>It is null for Snowpipe Streaming Telemetry.
   */
  final String stageName;

  static final KCLogger LOGGER = new KCLogger(SnowflakeTelemetryBasicInfo.class.getName());

  /**
   * Base Constructor. Accepts a tableName and StageName.
   *
   * @param tableName Checks for Nullability
   * @param stageName Can be null (In case of Snowpipe Streaming since there is no user visible
   *     Snowflake Stage)
   */
  public SnowflakeTelemetryBasicInfo(final String tableName, final String stageName) {
    Preconditions.checkArgument(
        !Strings.isNullOrEmpty(tableName), "tableName cannot be null or empty");
    this.tableName = tableName;
    this.stageName = stageName;
  }

  /**
   * Adds the required fields into the given ObjectNode which will then be used as payload in
   * Telemetry API
   *
   * @param msg ObjectNode in which extra fields needs to be added.
   */
  public abstract void dumpTo(ObjectNode msg);

  /**
   * @return true if it would suggest that their was no update to corresponding implementation's
   *     member variables. Or, in other words, the corresponding partition didnt receive any
   *     records, in which case we would not call telemetry API.
   */
  public abstract boolean isEmpty();
}
