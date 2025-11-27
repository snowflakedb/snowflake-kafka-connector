package com.snowflake.kafka.connector.streaming.iceberg;

import static com.snowflake.kafka.connector.streaming.iceberg.IcebergVersion.V2;
import static com.snowflake.kafka.connector.streaming.iceberg.sql.ComplexJsonRecord.complexJsonPayload;
import static com.snowflake.kafka.connector.streaming.iceberg.sql.ComplexJsonRecord.complexJsonPayloadWithWrongValueType;
import static com.snowflake.kafka.connector.streaming.iceberg.sql.ComplexJsonRecord.complexJsonRecordValueExample;
import static com.snowflake.kafka.connector.streaming.iceberg.sql.ComplexJsonRecord.complexJsonWithSchema;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.streaming.iceberg.sql.ComplexJsonRecord;
import com.snowflake.kafka.connector.streaming.iceberg.sql.MetadataRecord;
import com.snowflake.kafka.connector.streaming.iceberg.sql.RecordWithMetadata;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Disabled(
    "We don't enable this test suite because xp fails to log any issues that happen during"
        + " conversion of data we're sending to ICeberg objects. Working blind is too time"
        + " consuming. Tracker https://snowflakecomputing.atlassian.net/browse/SNOW-2856015")
public class IcebergIngestionNoSchemaEvolutionIT extends IcebergIngestionIT {

  private static final String PRIMITIVE_JSON_RECORD_CONTENT_OBJECT_SCHEMA =
      "object("
          + "id_int8 NUMBER(10,0),"
          + "id_int16 NUMBER(10,0),"
          + "id_int32 NUMBER(10,0),"
          + "id_int64 NUMBER(19,0),"
          + "description STRING,"
          + "rating_float32 FLOAT,"
          + "rating_float64 FLOAT,"
          + "approval BOOLEAN"
          + ")";

  private static final String COMPLEX_JSON_RECORD_CONTENT_OBJECT_SCHEMA =
      "object("
          + "id_int8 NUMBER(10,0),"
          + "id_int16 NUMBER(10,0),"
          + "id_int32 NUMBER(10,0),"
          + "id_int64 NUMBER(19,0),"
          + "description STRING,"
          + "rating_float32 FLOAT,"
          + "rating_float64 FLOAT,"
          + "approval BOOLEAN,"
          + "array1 ARRAY(LONG),"
          + "array2 ARRAY(STRING),"
          + "array3 ARRAY(BOOLEAN),"
          + "array4 ARRAY(LONG),"
          + "array5 ARRAY(ARRAY(LONG)),"
          + "nestedRecord "
          + PRIMITIVE_JSON_RECORD_CONTENT_OBJECT_SCHEMA
          + ","
          + "nestedRecord2 "
          + PRIMITIVE_JSON_RECORD_CONTENT_OBJECT_SCHEMA
          + ")";

  @BeforeEach
  protected void createIcebergTable() {
    createIcebergTableWithColumnClause(
        tableName,
        Utils.TABLE_COLUMN_METADATA
            + " "
            + IcebergDDLTypes.ICEBERG_METADATA_OBJECT_SCHEMA
            + ", "
            + Utils.TABLE_COLUMN_CONTENT
            + " "
            + COMPLEX_JSON_RECORD_CONTENT_OBJECT_SCHEMA,
        V2);
  }

  private static Stream<Arguments> prepareData() {
    return Stream.of(
        Arguments.of("Complex JSON with schema", complexJsonWithSchema, true),
        Arguments.of("Complex JSON without schema", complexJsonPayload, false));
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
        createKafkaRecord(complexJsonPayloadWithWrongValueType, 0, false);
    SinkRecord wrongValueRecord2 =
        createKafkaRecord(complexJsonPayloadWithWrongValueType, 2, false);
    service.insert(
        Arrays.asList(
            wrongValueRecord1,
            createKafkaRecord(complexJsonPayload, 1, false),
            wrongValueRecord2,
            createKafkaRecord(complexJsonPayload, 3, false),
            createKafkaRecord(complexJsonPayload, 4, false)));
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
