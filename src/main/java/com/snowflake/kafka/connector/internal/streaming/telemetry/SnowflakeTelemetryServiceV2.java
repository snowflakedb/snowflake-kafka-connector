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

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import java.sql.Connection;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryClient;

/**
 * This is the implementation of Telemetry Service for Snowpipe Streaming. Sends data related to
 * Snowpipe Streaming to Snowflake for additional debugging purposes.
 */
public class SnowflakeTelemetryServiceV2 extends SnowflakeTelemetryService {

  /**
   * Constructor for TelemetryService which is used by Snowpipe Streaming.
   *
   * @param conn Connection Object which gives the Telemetry Instance.
   */
  public SnowflakeTelemetryServiceV2(Connection conn) {
    this.telemetry = TelemetryClient.createTelemetry(conn);
  }

  @VisibleForTesting
  public SnowflakeTelemetryServiceV2(Telemetry telemetry) {
    this.telemetry = telemetry;
  }

  @Override
  public ObjectNode getObjectNode() {
    ObjectNode objectNode = getDefaultObjectNode(IngestionMethodConfig.SNOWPIPE_STREAMING);
    return objectNode;
  }
}
