package com.snowflake.kafka.connector.streaming.iceberg;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.streaming.iceberg.sql.MetadataRecord;
import com.snowflake.kafka.connector.streaming.iceberg.sql.MetadataRecord.RecordWithMetadata;
import com.snowflake.kafka.connector.streaming.iceberg.sql.PrimitiveJsonRecord;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class IcebergIngestionNoSchemaEvolutionIT extends IcebergIngestionIT {

  private static final String RECORD_CONTENT_OBJECT_SCHEMA =
      "object("
          + "id_int8 NUMBER(10,0),"
          + "id_int16 NUMBER(10,0),"
          + "id_int32 NUMBER(10,0),"
          + "id_int64 NUMBER(19,0),"
          + "description VARCHAR(16777216),"
          + "rating_float32 FLOAT,"
          + "rating_float64 FLOAT,"
          + "approval BOOLEAN"
          + ")";

  @Override
  protected Boolean isSchemaEvolutionEnabled() {
    return false;
  }

  @Override
  protected void createIcebergTable() {
    createIcebergTableWithColumnClause(
        tableName,
        Utils.TABLE_COLUMN_METADATA
            + " "
            + IcebergDDLTypes.ICEBERG_METADATA_OBJECT_SCHEMA
            + ", "
            + Utils.TABLE_COLUMN_CONTENT
            + " "
            + RECORD_CONTENT_OBJECT_SCHEMA);
  }

  private static Stream<Arguments> prepareData() {
    return Stream.of(
        Arguments.of("Primitive JSON with schema", primitiveJsonWithSchema, true),
        Arguments.of("Primitive JSON without schema", primitiveJson, false));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("prepareData")
  @Disabled
  void shouldInsertRecords(String description, String message, boolean withSchema)
      throws Exception {
    service.insert(
        Arrays.asList(
            createKafkaRecord(message, 0, withSchema), createKafkaRecord(message, 1, withSchema)));
    waitForOffset(2);
    service.insert(Collections.singletonList(createKafkaRecord(message, 2, withSchema)));
    waitForOffset(3);

    assertRecordsInTable();
  }

  private void assertRecordsInTable() {
    List<RecordWithMetadata<PrimitiveJsonRecord>> recordsWithMetadata =
        selectAllFromRecordContent();
    assertThat(recordsWithMetadata)
        .hasSize(3)
        .extracting(RecordWithMetadata::getRecord)
        .containsExactly(
            primitiveJsonRecordValue, primitiveJsonRecordValue, primitiveJsonRecordValue);
    List<MetadataRecord> metadataRecords =
        recordsWithMetadata.stream()
            .map(RecordWithMetadata::getMetadata)
            .collect(Collectors.toList());
    assertThat(metadataRecords).extracting(MetadataRecord::getOffset).containsExactly(0L, 1L, 2L);
    assertThat(metadataRecords)
        .hasSize(3)
        .allMatch(
            record ->
                record.getTopic().equals(topicPartition.topic())
                    && record.getPartition().equals(topicPartition.partition())
                    && record.getKey().equals("test")
                    && record.getSnowflakeConnectorPushTime() != null);
  }
}
