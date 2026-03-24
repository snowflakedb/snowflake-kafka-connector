package com.snowflake.kafka.connector.internal.schemaevolution;

import static org.assertj.core.api.Assertions.*;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.records.SnowflakeMetadataConfig;
import com.snowflake.kafka.connector.records.SnowflakeSinkRecord;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

public class TableSchemaResolverTest {

  private static final SnowflakeMetadataConfig METADATA_CONFIG = new SnowflakeMetadataConfig();

  private final TableSchemaResolver schemaResolver = new TableSchemaResolver();

  private static SnowflakeSinkRecord toSinkRecord(SinkRecord kafkaRecord) {
    return SnowflakeSinkRecord.from(kafkaRecord, METADATA_CONFIG, true);
  }

  @Test
  public void testGetColumnTypesWithoutSchema() {
    // Schemaless: content map with string values
    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put("test", "value");
    SinkRecord kafkaRecord = new SinkRecord("topic", 0, null, null, null, jsonMap, 0);
    SnowflakeSinkRecord record = toSinkRecord(kafkaRecord);

    String processedColumnName = Utils.quoteNameIfNeeded("test");
    String processedNonExistingColumnName = Utils.quoteNameIfNeeded("random");
    TableSchema tableSchema =
        schemaResolver.resolveTableSchemaFromSnowflakeRecord(
            record, Collections.singletonList(processedColumnName));

    assertThat(tableSchema.getColumnInfos())
        .containsExactlyInAnyOrderEntriesOf(
            Collections.singletonMap(processedColumnName, new ColumnInfos("VARCHAR", null)));
    // Get non-existing column name should return nothing
    tableSchema =
        schemaResolver.resolveTableSchemaFromSnowflakeRecord(
            record, Collections.singletonList(processedNonExistingColumnName));
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

    SinkRecord kafkaRecord =
        new SinkRecord("topic", 0, null, null, schemaAndValue.schema(), schemaAndValue.value(), 0);
    SnowflakeSinkRecord record = toSinkRecord(kafkaRecord);

    TableSchema tableSchema =
        schemaResolver.resolveTableSchemaFromSnowflakeRecord(
            record, Arrays.asList(columnName1, columnName2));

    assertThat(tableSchema.getColumnInfos().get(columnName1).getColumnType()).isEqualTo("VARCHAR");
    assertThat(tableSchema.getColumnInfos().get(columnName1).getComments()).isEqualTo("doc");
    assertThat(tableSchema.getColumnInfos().get(columnName2).getColumnType()).isEqualTo("VARCHAR");
    assertThat(tableSchema.getColumnInfos().get(columnName2).getComments()).isNull();
  }
}
