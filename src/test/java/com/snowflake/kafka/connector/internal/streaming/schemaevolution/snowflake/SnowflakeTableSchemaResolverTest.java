package com.snowflake.kafka.connector.internal.streaming.schemaevolution.snowflake;

import static org.assertj.core.api.Assertions.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.ColumnInfos;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.TableSchema;
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

public class SnowflakeTableSchemaResolverTest {

  private final SnowflakeTableSchemaResolver schemaResolver = new SnowflakeTableSchemaResolver();

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
}
