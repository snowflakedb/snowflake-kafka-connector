package com.snowflake.kafka.connector.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.kafka.connector.records.SnowflakeConverter;
import com.snowflake.kafka.connector.records.SnowflakeJsonConverter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SnowflakeSinkServiceV1InitRetryTest {
  private static final String TABLE = "test_table";
  private static final String TOPIC = "test_topic";

  private SnowflakeConnectionService conn;
  private SnowflakeSinkServiceV1 service;

  @BeforeEach
  void setUp() {
    conn = mock(SnowflakeConnectionService.class, RETURNS_MOCKS);
    service =
        (SnowflakeSinkServiceV1)
            SnowflakeSinkServiceFactory.builder(conn)
                .setRecordNumber(100)
                .addTask(TABLE, new TopicPartition(TOPIC, 0))
                .build();
  }

  @AfterEach
  void tearDown() {
    service.closeAll();
  }

  @Test
  void transientSetupErrorIsRetriableAndDoesNotCreateTable() {
    when(conn.tableExist(TABLE)).thenThrow(SnowflakeErrors.ERROR_2020.getException());

    assertThatThrownBy(() -> service.insert(Collections.singletonList(record(0))))
        .isInstanceOf(RetriableException.class);
    verify(conn, never()).createTable(anyString());

    // Snowflake is reachable again: the redelivered batch sets up the pipe and is accepted.
    doReturn(true).when(conn).tableExist(TABLE);
    doReturn(true).when(conn).isTableCompatible(TABLE);
    assertThatCode(() -> service.insert(Collections.singletonList(record(0))))
        .doesNotThrowAnyException();
    verify(conn, never()).createTable(anyString());
    assertThat(service.initRetryStartMs).isZero();
  }

  @Test
  void transientSetupErrorFailsTaskAfterRetryTimeout() {
    when(conn.tableExist(TABLE)).thenThrow(SnowflakeErrors.ERROR_2020.getException());
    service.initRetryStartMs = System.currentTimeMillis() - 2 * 60 * 60 * 1000L;

    assertThatThrownBy(() -> service.insert(Collections.singletonList(record(0))))
        .isInstanceOfSatisfying(
            SnowflakeKafkaConnectorException.class,
            e -> assertThat(e.checkErrorCode(SnowflakeErrors.ERROR_2020)).isTrue());
  }

  @Test
  void nonTransientSetupErrorIsNotRetried() {
    doThrow(SnowflakeErrors.ERROR_2007.getException()).when(conn).createTable(TABLE);

    assertThatThrownBy(() -> service.insert(Collections.singletonList(record(0))))
        .isInstanceOfSatisfying(
            SnowflakeKafkaConnectorException.class,
            e -> assertThat(e.checkErrorCode(SnowflakeErrors.ERROR_2007)).isTrue());
  }

  private static SinkRecord record(long offset) {
    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue input =
        converter.toConnectData(TOPIC, "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8));
    return new SinkRecord(TOPIC, 0, null, null, input.schema(), input.value(), offset);
  }
}
