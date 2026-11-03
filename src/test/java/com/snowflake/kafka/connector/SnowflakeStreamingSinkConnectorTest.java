/*
 * Copyright (c) 2019 Snowflake Inc. All rights reserved.
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
package com.snowflake.kafka.connector;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Guards the connection lifecycle of {@link SnowflakeStreamingSinkConnector}.
 *
 * <p>{@code start()} builds a long-lived, otherwise idle JDBC-backed connection — ingestion itself
 * runs over separate Snowpipe Streaming channels — and for a long time {@code stop()} never closed
 * it, leaking one connection per connector stop. These tests pin the close down, and pin down the
 * order it happens in, which is the part that is easy to break by accident.
 *
 * <p>{@code start()} cannot be driven here, because it builds a real Snowflake connection that no
 * unit test can satisfy; the connection is injected instead, wired the same way {@code start()}
 * wires it.
 */
public class SnowflakeStreamingSinkConnectorTest {

  @Test
  public void stopClosesTheConnectionItHasHeldOpenSinceStart() {
    // GIVEN a started connector holding a connection
    SnowflakeConnectionService conn = connectionWithTelemetry();
    SnowflakeStreamingSinkConnector connector = new SnowflakeStreamingSinkConnector();
    connector.injectConnectionForTests(conn);

    // WHEN the connector is stopped
    connector.stop();

    // THEN the connection is closed rather than left dangling for the worker's lifetime
    verify(conn).close();
  }

  /**
   * The stop telemetry travels over the connection being closed, so reporting has to happen first.
   * A telemetry client that buffers rather than sending per event additionally depends on the close
   * to flush — reverse these two calls and the {@code kafka_stop} event is lost, which is precisely
   * the event Support needs when a connector goes down unexpectedly.
   */
  @Test
  public void stopReportsTelemetryBeforeClosingTheConnectionItTravelsOver() {
    SnowflakeConnectionService conn = connectionWithTelemetry();
    SnowflakeTelemetryService telemetry = conn.getTelemetryClient();
    SnowflakeStreamingSinkConnector connector = new SnowflakeStreamingSinkConnector();
    connector.injectConnectionForTests(conn);

    connector.stop();

    InOrder order = inOrder(telemetry, conn);
    order.verify(telemetry).reportKafkaConnectStop(0L);
    order.verify(conn).close();
  }

  @Test
  public void stopSwallowsAFailureToCloseBecauseTheConnectorIsAlreadyShuttingDown() {
    // GIVEN a connection whose close() fails, as close() is permitted to do (ERROR_2005)
    SnowflakeConnectionService conn = connectionWithTelemetry();
    doThrow(SnowflakeErrors.ERROR_2005.getException()).when(conn).close();
    SnowflakeStreamingSinkConnector connector = new SnowflakeStreamingSinkConnector();
    connector.injectConnectionForTests(conn);

    // WHEN / THEN stop() does not propagate it into the Kafka Connect worker's shutdown path
    assertThatCode(connector::stop).doesNotThrowAnyException();
    verify(conn).close();
  }

  @Test
  public void stopIsSafeWhenStartNeverBuiltAConnection() {
    // Kafka Connect calls stop() even when start() failed before assigning the connection.
    assertThatCode(new SnowflakeStreamingSinkConnector()::stop).doesNotThrowAnyException();
  }

  private static SnowflakeConnectionService connectionWithTelemetry() {
    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.getTelemetryClient()).thenReturn(mock(SnowflakeTelemetryService.class));
    return conn;
  }
}
