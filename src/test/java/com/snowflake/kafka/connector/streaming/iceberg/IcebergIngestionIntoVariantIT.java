package com.snowflake.kafka.connector.streaming.iceberg;

import static com.snowflake.kafka.connector.streaming.iceberg.IcebergVersion.V3;
import static com.snowflake.kafka.connector.streaming.iceberg.sql.ComplexJsonRecord.complexJsonPayload;
import static com.snowflake.kafka.connector.streaming.iceberg.sql.ComplexJsonRecord.complexJsonRecordValueExample;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.streaming.iceberg.sql.ComplexJsonRecord;
import com.snowflake.kafka.connector.streaming.iceberg.sql.MetadataRecord;
import com.snowflake.kafka.connector.streaming.iceberg.sql.RecordWithMetadata;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.header.Headers;
import org.junit.jupiter.api.Test;

public class IcebergIngestionIntoVariantIT extends IcebergIngestionIT {

  /**
   * Expected {@code RECORD_METADATA:headers} for records built by {@link #createKafkaRecord} with
   * structured headers enabled. The RECORD_METADATA VARIANT column preserves each converted value's
   * type: scalars stay native, the object header lands as a nested object, and the array header as
   * a list. This is the KC v3-compatible behavior (legacy string flattening is covered by unit
   * tests).
   */
  private static final Map<String, Object> EXPECTED_HEADERS =
      Map.of(
          "booleanHeader",
          true,
          "stringHeader",
          "test",
          "intHeader",
          123,
          "longHeader",
          123,
          "shortHeader",
          123,
          "objectHeader",
          Map.of("nestedString", "inner", "nestedInt", 7),
          "arrayHeader",
          List.of(1, 2, 3));

  @Override
  protected void createIcebergTable() {
    createIcebergTableWithColumnClause(
        tableName, "RECORD_METADATA VARIANT, RECORD_CONTENT VARIANT", V3);
  }

  @Override
  protected boolean structuredHeadersEnabled() {
    return true;
  }

  @Override
  protected void addStructuredHeaders(Headers headers) {
    Schema objectSchema =
        SchemaBuilder.struct()
            .field("nestedString", Schema.STRING_SCHEMA)
            .field("nestedInt", Schema.INT32_SCHEMA)
            .build();
    Struct objectValue = new Struct(objectSchema).put("nestedString", "inner").put("nestedInt", 7);
    headers.add("objectHeader", objectValue, objectSchema);
    headers.add("arrayHeader", Arrays.asList(1, 2, 3), SchemaBuilder.array(Schema.INT32_SCHEMA));
  }

  @Test
  void shouldInsertRecordsLegacyBagOfBits() throws Exception {
    final long overMaxIntOffset = (long) Integer.MAX_VALUE + 1;
    final boolean withSchema = false;
    final String message = complexJsonPayload;
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
    assertThat(metadataRecords)
        .extracting(MetadataRecord::getHeaders)
        .containsOnly(EXPECTED_HEADERS);
  }
}
