package com.snowflake.kafka.connector.internal.schemaevolution;

import static org.assertj.core.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TableSchemaResolverTest {

  private final TableSchemaResolver schemaResolver = new TableSchemaResolver();

  @Test
  public void testGetColumnTypesWithoutSchema() throws JsonProcessingException {
    String columnName = "test";
    String nonExistingColumnName = "random";
    ObjectMapper mapper = new ObjectMapper();
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, ?> config = Collections.singletonMap("schemas.enable", false);
    jsonConverter.configure(config, false);
    Map<String, String> jsonMap = new HashMap<>();
    jsonMap.put(columnName, "value");
    SchemaAndValue schemaAndValue =
        jsonConverter.toConnectData("topic", mapper.writeValueAsBytes(jsonMap));
    SinkRecord recordWithoutSchema =
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

    String processedColumnName = Utils.quoteNameIfNeeded(columnName);
    String processedNonExistingColumnName = Utils.quoteNameIfNeeded(nonExistingColumnName);
    TableSchema tableSchema =
        schemaResolver.resolveTableSchemaFromRecord(
            recordWithoutSchema, Collections.singletonList(processedColumnName));

    assertThat(tableSchema.getColumnInfos())
        .containsExactlyInAnyOrderEntriesOf(
            Collections.singletonMap(processedColumnName, new ColumnInfos("VARCHAR", null)));
    // Get non-existing column name should return nothing
    tableSchema =
        schemaResolver.resolveTableSchemaFromRecord(
            recordWithoutSchema, Collections.singletonList(processedNonExistingColumnName));
    assertThat(tableSchema.getColumnInfos()).isEmpty();
  }

  @Test
  public void testGetColumnTypesWithSchema() {
    JsonConverter converter = new JsonConverter();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "true");
    converter.configure(converterConfig, false);
    SchemaAndValue schemaAndValue =
        converter.toConnectData(
            "topic", TestUtils.JSON_WITH_SCHEMA.getBytes(StandardCharsets.UTF_8));
    String columnName1 = Utils.quoteNameIfNeeded("regionid");
    String columnName2 = Utils.quoteNameIfNeeded("gender");
    SinkRecord recordWithoutSchema =
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

    TableSchema tableSchema =
        schemaResolver.resolveTableSchemaFromRecord(
            recordWithoutSchema, Arrays.asList(columnName1, columnName2));

    assertThat(tableSchema.getColumnInfos().get(columnName1).getColumnType()).isEqualTo("VARCHAR");
    assertThat(tableSchema.getColumnInfos().get(columnName1).getComments()).isEqualTo("doc");
    assertThat(tableSchema.getColumnInfos().get(columnName2).getColumnType()).isEqualTo("VARCHAR");
    assertThat(tableSchema.getColumnInfos().get(columnName2).getComments()).isNull();
  }

  // ---- convertToJson tests ported from v3.2 RecordContentTest ----

  @Test
  public void convertToJson_whenInvalidInput_throwException() {
    Schema int32Schema = SchemaBuilder.int32().build();
    Assertions.assertThrows(
        SnowflakeKafkaConnectorException.class,
        () -> TableSchemaResolver.convertToJson(int32Schema, null, false));
  }

  @Test
  public void convertToJson_returnDefaultValue() {
    Schema schema = SchemaBuilder.int32().optional().defaultValue(123).build();
    Assertions.assertEquals(
        "123", TableSchemaResolver.convertToJson(schema, null, false).toString());
  }

  @Test
  public void testConvertToJsonReadOnlyByteBuffer() {
    String original = "bytes";
    String expected = "\"" + Base64.getEncoder().encodeToString(original.getBytes()) + "\"";
    ByteBuffer buffer = ByteBuffer.wrap(original.getBytes()).asReadOnlyBuffer();
    Schema schema = SchemaBuilder.bytes().build();

    assertEquals(expected, TableSchemaResolver.convertToJson(schema, buffer, false).toString());
  }
}
