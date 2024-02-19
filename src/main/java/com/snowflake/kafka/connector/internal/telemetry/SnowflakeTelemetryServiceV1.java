package com.snowflake.kafka.connector.internal.telemetry;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import java.sql.Connection;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryClient;

/** Implementation of Snowflake Telemetry Service specific for Snowpipe telemetries. */
public class SnowflakeTelemetryServiceV1 extends SnowflakeTelemetryService {

  SnowflakeTelemetryServiceV1(Connection conn) {
    this.telemetry = TelemetryClient.createTelemetry(conn);
  }

  @VisibleForTesting
  SnowflakeTelemetryServiceV1(Telemetry telemetry) {
    this.telemetry = telemetry;
  }

  @Override
  public ObjectNode getObjectNode() {
    ObjectNode objectNode = getDefaultObjectNode(IngestionMethodConfig.SNOWPIPE);
    return objectNode;
  }
}
