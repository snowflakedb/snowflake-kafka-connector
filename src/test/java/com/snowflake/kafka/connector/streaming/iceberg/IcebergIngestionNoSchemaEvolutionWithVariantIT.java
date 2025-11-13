package com.snowflake.kafka.connector.streaming.iceberg;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.streaming.iceberg.sql.ComplexJsonRecord;
import com.snowflake.kafka.connector.streaming.iceberg.sql.MetadataRecord;
import com.snowflake.kafka.connector.streaming.iceberg.sql.RecordWithMetadata;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.snowflake.kafka.connector.streaming.iceberg.sql.ComplexJsonRecord.*;
import static org.assertj.core.api.Assertions.assertThat;

public class IcebergIngestionNoSchemaEvolutionWithVariantIT extends IcebergIngestionIT {

  @Override
  protected Boolean isSchemaEvolutionEnabled() {
    return false;
  }

  @Override
  protected void createIcebergTable() {
    createIcebergTableWithColumnClause(
        tableName,
        Utils.TABLE_COLUMN_METADATA
            + " VARIANT"
            + ", "
            + Utils.TABLE_COLUMN_CONTENT
            + " VARIANT");
  }

  private static Stream<Arguments> prepareData() {
    return Stream.of(
        Arguments.of(
            "Complex JSON with schema", ComplexJsonRecord.complexJsonWithSchemaExample, true),
        Arguments.of(
            "Complex JSON without schema", ComplexJsonRecord.complexJsonPayloadExample, false));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("prepareData")
  void shouldInsertRecords(String description, String message, boolean withSchema)
      throws Exception {
    long overMaxIntOffset = (long) Integer.MAX_VALUE + 1;
    service.insert(
        Arrays.asList(
            createKafkaRecord(message, 0, withSchema), createKafkaRecord(message, 1, withSchema)));
    waitForOffset(2);
    service.insert(Collections.singletonList(createKafkaRecord(message, 2, withSchema)));
    waitForOffset(3);
    service.insert(
        Collections.singletonList(createKafkaRecord(message, overMaxIntOffset, withSchema)));
    waitForOffset(overMaxIntOffset + 1);

    assertRecordsInTable(Arrays.asList(0L, 1L, 2L, overMaxIntOffset));
  }

  @Test
  void shouldSendValueWithWrongTypeToDLQ() throws Exception {
    SinkRecord wrongValueRecord1 =
        createKafkaRecord(complexJsonPayloadWithWrongValueTypeExample, 0, false);
    SinkRecord wrongValueRecord2 =
        createKafkaRecord(complexJsonPayloadWithWrongValueTypeExample, 2, false);
    service.insert(
        Arrays.asList(
            wrongValueRecord1,
            createKafkaRecord(complexJsonPayloadExample, 1, false),
            wrongValueRecord2,
            createKafkaRecord(complexJsonPayloadExample, 3, false),
            createKafkaRecord(complexJsonPayloadExample, 4, false)));
    waitForOffset(5);

    assertRecordsInTable(Arrays.asList(1L, 3L, 4L));
    List<InMemoryKafkaRecordErrorReporter.ReportedRecord> reportedRecords =
        kafkaRecordErrorReporter.getReportedRecords();
    assertThat(reportedRecords).hasSize(2);
    assertThat(reportedRecords.stream().map(it -> it.getRecord()).collect(Collectors.toList()))
        .containsExactlyInAnyOrder(wrongValueRecord1, wrongValueRecord2);
  }

  private void assertRecordsInTable(List<Long> expectedOffsets) {
    List<RecordWithMetadata<ComplexJsonRecord>> recordsWithMetadata =
        selectAllComplexJsonRecordFromRecordContent();
    assertThat(recordsWithMetadata)
        .hasSize(expectedOffsets.size())
        .extracting(RecordWithMetadata::getRecord)
        .containsOnly(complexJsonRecordValueExample);
    List<MetadataRecord> metadataRecords =
        recordsWithMetadata.stream()
            .map(RecordWithMetadata::getMetadata)
            .collect(Collectors.toList());
    assertThat(metadataRecords)
        .extracting(MetadataRecord::getOffset)
        .containsExactlyElementsOf(expectedOffsets);
    assertThat(metadataRecords)
        .hasSize(expectedOffsets.size())
        .allMatch(
            record ->
                record.getTopic().equals(topicPartition.topic())
                    && record.getPartition().equals(topicPartition.partition())
                    && record.getKey().equals("test")
                    && record.getSnowflakeConnectorPushTime() != null);
  }
}
