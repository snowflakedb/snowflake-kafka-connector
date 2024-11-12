/*
 * Copyright (c) 2023 Snowflake Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.snowflake.kafka.connector.internal.streaming.telemetry;

import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.IS_REUSE_TABLE;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.TABLE_NAME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.TOPIC_PARTITION_CHANNEL_CREATION_TIME;
import static com.snowflake.kafka.connector.internal.telemetry.TelemetryConstants.TOPIC_PARTITION_CHANNEL_NAME;

import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryBasicInfo;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;

/**
 * This object is sent only once when a channel starts. No concurrent modification is made on this
 * object, thus no lock is required.
 */
public class SnowflakeTelemetryChannelCreation extends SnowflakeTelemetryBasicInfo {
  private final long tpChannelCreationTime; // start time of the channel
  private final String tpChannelName;
  private boolean isReuseTable = false; // is the channel reusing existing table

  public SnowflakeTelemetryChannelCreation(
      final String tableName, final String channelName, final long startTime) {
    super(tableName, SnowflakeTelemetryService.TelemetryType.KAFKA_CHANNEL_START);
    this.tpChannelName = channelName;
    this.tpChannelCreationTime = startTime;
  }

  @Override
  public void dumpTo(ObjectNode msg) {
    msg.put(TABLE_NAME, this.tableName);
    msg.put(TOPIC_PARTITION_CHANNEL_NAME, this.tpChannelName);

    msg.put(IS_REUSE_TABLE, this.isReuseTable);
    msg.put(TOPIC_PARTITION_CHANNEL_CREATION_TIME, tpChannelCreationTime);
  }

  @Override
  public boolean isEmpty() {
    throw new IllegalStateException(
        "Empty function doesnt apply to:" + this.getClass().getSimpleName());
  }

  public void setReuseTable(boolean reuseTable) {
    isReuseTable = reuseTable;
  }
}
