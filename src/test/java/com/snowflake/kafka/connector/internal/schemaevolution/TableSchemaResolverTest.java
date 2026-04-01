package com.snowflake.kafka.connector.internal.schemaevolution;

import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import com.snowflake.kafka.connector.records.SnowflakeSinkRecord;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class TableSchemaResolverTest {

  private static final SnowflakeMetadataConfig METADATA_CONFIG = new SnowflakeMetadataConfig();

  private final TableSchemaResolver schemaResolver = new TableSchemaResolver();

  private static SnowflakeSinkRecord toSinkRecord(
      SinkRecord kafkaRecord, boolean enableColumnIdentifierNormalization) {
    return SnowflakeSinkRecord.from(
        kafkaRecord, METADATA_CONFIG, true, enableColumnIdentifierNormalization);
  }

  @Test
  public void testGetColumnTypesWithSchema_TimestampField_JacksonCanSerialize() {
    // Reproducer for PR review comment: when schematization IS enabled,
    // content map holds the raw output of convertToMap(). During schema evolution,
    // OBJECT_MAPPER.valueToTree(record.getContent()) runs on this map.
    // If the map contains a raw Instant, plain ObjectMapper (no JavaTimeModule) throws
    // InvalidDefinitionException.
    java.util.Date nearEpochDate =
        new java.util.Date(java.time.Instant.parse("1969-04-08T00:00:00Z").toEpochMilli());

    org.apache.kafka.connect.data.Schema schema =
        org.apache.kafka.connect.data.SchemaBuilder.struct()
            .field("ts", org.apache.kafka.connect.data.Timestamp.SCHEMA)
            .build();
    org.apache.kafka.connect.data.Struct struct =
        new org.apache.kafka.connect.data.Struct(schema).put("ts", nearEpochDate);

    SinkRecord kafkaRecord =
        new SinkRecord(
            "topic",
            0,
            null,
            null,
            schema,
            struct,
            0,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME);

    // enableSchematization=true so content = convertToMap() result (may contain Instant)
    SnowflakeSinkRecord record = toSinkRecord(kafkaRecord, false);

    // This is the call that fails: OBJECT_MAPPER.valueToTree(record.getContent())
    // triggers Jackson serialization of the Instant without JavaTimeModule
    assertThatCode(
            () -> schemaResolver.resolveTableSchemaFromSnowflakeRecord(record, Arrays.asList("ts")))
        .doesNotThrowAnyException();
  }

  @Test
  public void testGetColumnTypesWithoutSchema_NormalizationEnabled()
      throws JsonProcessingException {
    String columnName = "test";
    ObjectMapper mapper = new ObjectMapper();
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, ?> config = Collections.singletonMap("schemas.enable", false);
    jsonConverter.configure(config, false);
    Map<String, String> jsonMap = new HashMap<>();
    jsonMap.put(columnName, "value");
    SchemaAndValue schemaAndValue =
        jsonConverter.toConnectData("topic", mapper.writeValueAsBytes(jsonMap));
    SinkRecord kafkaRecord =
        new SinkRecord(
            "topic",
            0,
            null,
            null,
            schemaAndValue.schema(),
            schemaAndValue.value(),
            0,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME);
    SnowflakeSinkRecord record = toSinkRecord(kafkaRecord, true);

    // With normalization=true, "test" normalizes to "TEST" in ColumnValuePair
    // So columnsToInclude should use raw normalized name "TEST"
    TableSchema tableSchema =
        schemaResolver.resolveTableSchemaFromSnowflakeRecord(
            record, Collections.singletonList("TEST"));

    assertThat(tableSchema.getColumnInfos())
        .containsExactlyInAnyOrderEntriesOf(
            Collections.singletonMap("TEST", new ColumnInfos("VARCHAR", null)));

    // Get non-existing column name should return nothing
    tableSchema =
        schemaResolver.resolveTableSchemaFromSnowflakeRecord(
            record, Collections.singletonList("NONEXISTENT"));
    assertThat(tableSchema.getColumnInfos()).isEmpty();
  }

  @Test
  public void testGetColumnTypesWithoutSchema_NormalizationDisabled()
      throws JsonProcessingException {
    String columnName = "test";
    ObjectMapper mapper = new ObjectMapper();
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, ?> config = Collections.singletonMap("schemas.enable", false);
    jsonConverter.configure(config, false);
    Map<String, String> jsonMap = new HashMap<>();
    jsonMap.put(columnName, "value");
    SchemaAndValue schemaAndValue =
        jsonConverter.toConnectData("topic", mapper.writeValueAsBytes(jsonMap));
    SinkRecord kafkaRecord =
        new SinkRecord(
            "topic",
            0,
            null,
            null,
            schemaAndValue.schema(),
            schemaAndValue.value(),
            0,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME);
    SnowflakeSinkRecord record = toSinkRecord(kafkaRecord, false);

    // With normalization=false, column name stays as-is: "test"
    TableSchema tableSchema =
        schemaResolver.resolveTableSchemaFromSnowflakeRecord(
            record, Collections.singletonList("test"));

    assertThat(tableSchema.getColumnInfos())
        .containsExactlyInAnyOrderEntriesOf(
            Collections.singletonMap("test", new ColumnInfos("VARCHAR", null)));
  }

  @Test
  public void testGetColumnTypesWithSchema_NormalizationEnabled() {
    JsonConverter converter = new JsonConverter();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "true");
    converter.configure(converterConfig, false);
    SchemaAndValue schemaAndValue =
        converter.toConnectData(
            "topic", TestUtils.JSON_WITH_SCHEMA.getBytes(StandardCharsets.UTF_8));

    // With normalization=true: "regionid" → "REGIONID", "gender" → "GENDER"
    String columnName1 = "REGIONID";
    String columnName2 = "GENDER";
    SinkRecord kafkaRecord =
        new SinkRecord(
            "topic",
            0,
            null,
            null,
            schemaAndValue.schema(),
            schemaAndValue.value(),
            0,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME);
    SnowflakeSinkRecord record = toSinkRecord(kafkaRecord, true);

    TableSchema tableSchema =
        schemaResolver.resolveTableSchemaFromSnowflakeRecord(
            record, Arrays.asList(columnName1, columnName2));

    assertThat(tableSchema.getColumnInfos().get(columnName1).getColumnType()).isEqualTo("VARCHAR");
    assertThat(tableSchema.getColumnInfos().get(columnName1).getComments()).isEqualTo("doc");
    assertThat(tableSchema.getColumnInfos().get(columnName2).getColumnType()).isEqualTo("VARCHAR");
    assertThat(tableSchema.getColumnInfos().get(columnName2).getComments()).isNull();
  }

  @Test
  public void testGetColumnTypesWithSchema_NormalizationDisabled() {
    JsonConverter converter = new JsonConverter();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "true");
    converter.configure(converterConfig, false);
    SchemaAndValue schemaAndValue =
        converter.toConnectData(
            "topic", TestUtils.JSON_WITH_SCHEMA.getBytes(StandardCharsets.UTF_8));

    // With normalization=false: column names stay as-is
    String columnName1 = "regionid";
    String columnName2 = "gender";
    SinkRecord kafkaRecord =
        new SinkRecord(
            "topic",
            0,
            null,
            null,
            schemaAndValue.schema(),
            schemaAndValue.value(),
            0,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME);
    SnowflakeSinkRecord record = toSinkRecord(kafkaRecord, false);

    TableSchema tableSchema =
        schemaResolver.resolveTableSchemaFromSnowflakeRecord(
            record, Arrays.asList(columnName1, columnName2));

    assertThat(tableSchema.getColumnInfos().get(columnName1).getColumnType()).isEqualTo("VARCHAR");
    assertThat(tableSchema.getColumnInfos().get(columnName1).getComments()).isEqualTo("doc");
    assertThat(tableSchema.getColumnInfos().get(columnName2).getColumnType()).isEqualTo("VARCHAR");
    assertThat(tableSchema.getColumnInfos().get(columnName2).getComments()).isNull();
  }
}
