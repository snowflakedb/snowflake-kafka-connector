package com.snowflake.kafka.connector.streaming.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.DescribeTableRow;
import com.snowflake.kafka.connector.streaming.iceberg.sql.MetadataRecord;
import com.snowflake.kafka.connector.streaming.iceberg.sql.MetadataRecord.RecordWithMetadata;
import com.snowflake.kafka.connector.streaming.iceberg.sql.PrimitiveJsonRecord;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class IcebergIngestionSchemaEvolutionIT extends IcebergIngestionIT {

  @Override
  protected Boolean isSchemaEvolutionEnabled() {
    return true;
  }

  @Override
  protected void createIcebergTable() {
    createIcebergTable(tableName);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("prepareData")
  @Disabled
  void shouldEvolveSchemaAndInsertRecords(
      String description, String message, DescribeTableRow[] expectedSchema, boolean withSchema)
      throws Exception {
    // start off with just one column
    List<DescribeTableRow> rows = describeTable(tableName);
    assertThat(rows)
        .hasSize(1)
        .extracting(DescribeTableRow::getColumn)
        .contains(Utils.TABLE_COLUMN_METADATA);

    SinkRecord record = createKafkaRecord(message, 0, withSchema);
    service.insert(Collections.singletonList(record));
    waitForOffset(-1);
    rows = describeTable(tableName);
    assertThat(rows.size()).isEqualTo(9);

    // don't check metadata column schema, we have different tests for that
    rows =
        rows.stream()
            .filter(r -> !r.getColumn().equals(Utils.TABLE_COLUMN_METADATA))
            .collect(Collectors.toList());

    assertThat(rows).containsExactlyInAnyOrder(expectedSchema);

    // resend and store same record without any issues now
    service.insert(Collections.singletonList(record));
    waitForOffset(1);

    // and another record with same schema
    service.insert(Collections.singletonList(createKafkaRecord(message, 1, withSchema)));
    waitForOffset(2);

    // and another record with extra field - schema evolves again
    service.insert(Collections.singletonList(createKafkaRecord(simpleRecordJson, 2, false)));

    rows = describeTable(tableName);
    assertThat(rows).hasSize(10).contains(new DescribeTableRow("SIMPLE", "VARCHAR(16777216)"));

    // reinsert record with extra field
    service.insert(Collections.singletonList(createKafkaRecord(simpleRecordJson, 2, false)));
    waitForOffset(3);

    assertRecordsInTable();
  }

  private void assertRecordsInTable() {
    List<RecordWithMetadata<PrimitiveJsonRecord>> recordsWithMetadata =
        selectAllSchematizedRecords();

    assertThat(recordsWithMetadata)
        .hasSize(3)
        .extracting(RecordWithMetadata::getRecord)
        .containsExactly(
            primitiveJsonRecordValue, primitiveJsonRecordValue, emptyPrimitiveJsonRecordValue);
    List<MetadataRecord> metadataRecords =
        recordsWithMetadata.stream()
            .map(RecordWithMetadata::getMetadata)
            .collect(Collectors.toList());
    assertThat(metadataRecords).extracting(MetadataRecord::getOffset).containsExactly(0L, 1L, 2L);
    assertThat(metadataRecords)
        .hasSize(3)
        .allMatch(
            r ->
                r.getTopic().equals(topicPartition.topic())
                    && r.getPartition().equals(topicPartition.partition())
                    && r.getKey().equals("test")
                    && r.getSnowflakeConnectorPushTime() != null);
  }

  private static Stream<Arguments> prepareData() {
    return Stream.of(
        Arguments.of(
            "Primitive JSON with schema",
            primitiveJsonWithSchema,
            new DescribeTableRow[] {
              new DescribeTableRow("ID_INT8", "NUMBER(10,0)"),
              new DescribeTableRow("ID_INT16", "NUMBER(10,0)"),
              new DescribeTableRow("ID_INT32", "NUMBER(10,0)"),
              new DescribeTableRow("ID_INT64", "NUMBER(19,0)"),
              new DescribeTableRow("DESCRIPTION", "VARCHAR(16777216)"),
              new DescribeTableRow("RATING_FLOAT32", "FLOAT"),
              new DescribeTableRow("RATING_FLOAT64", "FLOAT"),
              new DescribeTableRow("APPROVAL", "BOOLEAN")
            },
            true),
        Arguments.of(
            "Primitive JSON without schema",
            primitiveJson,
            new DescribeTableRow[] {
              new DescribeTableRow("ID_INT8", "NUMBER(19,0)"),
              new DescribeTableRow("ID_INT16", "NUMBER(19,0)"),
              new DescribeTableRow("ID_INT32", "NUMBER(19,0)"),
              new DescribeTableRow("ID_INT64", "NUMBER(19,0)"),
              new DescribeTableRow("DESCRIPTION", "VARCHAR(16777216)"),
              new DescribeTableRow("RATING_FLOAT32", "FLOAT"),
              new DescribeTableRow("RATING_FLOAT64", "FLOAT"),
              new DescribeTableRow("APPROVAL", "BOOLEAN")
            },
            false));
  }
}
