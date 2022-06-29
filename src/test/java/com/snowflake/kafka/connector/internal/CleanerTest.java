package com.snowflake.kafka.connector.internal;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService;
import com.snowflake.kafka.connector.records.SnowflakeConverter;
import com.snowflake.kafka.connector.records.SnowflakeJsonConverter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;

public class CleanerTest {
  private final String table = TestUtils.randomTableName();
  private final String topic = "test";

  @Test(expected = ConnectException.class)
  public void testCleanerFails() {

    // throw error in cleaner thread
    SnowflakeTelemetryService telemetryService = mock(SnowflakeTelemetryService.class);
    doThrow(new RuntimeException("Error to stop Cleaner thread"))
        .when(telemetryService)
        .reportKafkaPartitionUsage(any(), anyBoolean());

    SnowflakeConnectionService conn = mock(SnowflakeConnectionService.class);
    when(conn.getTelemetryClient()).thenReturn(telemetryService);

    // set cleaner retries to 0
    final int partition = 0;
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn)
            .setRecordNumber(1)
            .addTask(table, new TopicPartition(topic, partition))
            .setMaxCleanerRetries(0)
            .build();
    service.startTask(table, new TopicPartition(topic, partition));

    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue input =
        converter.toConnectData(topic, "{\"name\":\"test\"}".getBytes(StandardCharsets.UTF_8));
    long offset = 0;
    SinkRecord record1 =
        new SinkRecord(
            topic, partition, Schema.STRING_SCHEMA, "test", input.schema(), input.value(), offset);

    // trigger cleaner failure.
    // The first insert will initialize the cleaner thread which would throw the mocked
    // RuntimeException above
    service.insert(Collections.singletonList(record1));
    SinkRecord record2 =
        new SinkRecord(
            topic, partition, Schema.STRING_SCHEMA, "test2", input.schema(), input.value(), offset);
    // failed cleaner now should kill the task
    service.insert(Collections.singletonList(record2));
  }
}
