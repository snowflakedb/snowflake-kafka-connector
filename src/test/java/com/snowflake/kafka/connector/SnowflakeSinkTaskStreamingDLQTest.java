package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.builder.SinkRecordBuilder;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.records.SnowflakeJsonConverter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class SnowflakeSinkTaskStreamingDLQTest {
  private static final String TOPIC = TestUtils.randomTableName();
  private static final int PARTITION = 0;

  @Test
  public void snowflakeSinkTask_put_whenIngestServiceReturnsError_sendRecordToDLQ() {
    // given
    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();
    SnowflakeSinkTask sinkTask =
        SnowflakeStreamingSinkTaskBuilder.create(TOPIC, PARTITION)
            .withErrorReporter(errorReporter)
            .withStreamingIngestClient(ingestClientReturningError())
            .build();

    // send bad data
    SchemaAndValue schemaAndValue = new SchemaAndValue(Schema.INT32_SCHEMA, 12);
    SinkRecord record =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION)
            .withSchemaAndValue(schemaAndValue)
            .build();

    // when
    sinkTask.put(Collections.singletonList(record));

    // then
    Assertions.assertEquals(1, errorReporter.getReportedRecords().size());
  }

  @Test
  public void snowflakeSinkTask_put_whenJsonRecordCannotBeSchematized_sendRecordToDLQ() {
    // given
    InMemoryKafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();
    SnowflakeSinkTask sinkTask =
        SnowflakeStreamingSinkTaskBuilder.create(TOPIC, PARTITION)
            .withErrorReporter(errorReporter)
            .build();

    SnowflakeJsonConverter jsonConverter = new SnowflakeJsonConverter();
    String notSchematizeableJsonRecord =
        "[{\"name\":\"sf\",\"answer\":42}]"; // cannot schematize array
    byte[] valueContents = (notSchematizeableJsonRecord).getBytes(StandardCharsets.UTF_8);
    SchemaAndValue sv = jsonConverter.toConnectData(TOPIC, valueContents);

    SinkRecord record =
        SinkRecordBuilder.forTopicPartition(TOPIC, PARTITION).withSchemaAndValue(sv).build();

    // when
    sinkTask.put(Collections.singletonList(record));

    // then
    Assertions.assertEquals(1, errorReporter.getReportedRecords().size());
  }

  private SnowflakeStreamingIngestClient ingestClientReturningError() {
    SnowflakeStreamingIngestClient mockStreamingClient =
        Mockito.mock(SnowflakeStreamingIngestClient.class);
    SnowflakeStreamingIngestChannel mockStreamingChannel =
        Mockito.mock(SnowflakeStreamingIngestChannel.class);

    InsertValidationResponse validationResponse1 = new InsertValidationResponse();
    InsertValidationResponse.InsertError insertError =
        new InsertValidationResponse.InsertError("CONTENT", 0);
    insertError.setException(new SFException(ErrorCode.INVALID_VALUE_ROW, "INVALID_CHANNEL"));
    validationResponse1.addError(insertError);

    Mockito.when(
            mockStreamingChannel.insertRows(
                ArgumentMatchers.any(Iterable.class),
                ArgumentMatchers.any(String.class),
                ArgumentMatchers.any(String.class)))
        .thenReturn(validationResponse1);

    Mockito.when(mockStreamingClient.openChannel(ArgumentMatchers.any(OpenChannelRequest.class)))
        .thenReturn(mockStreamingChannel);

    return mockStreamingClient;
  }
}
