package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.SchematizationTestUtils;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class SchematizationUtilsTest {
  @Rule public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Test
  public void testCollectSchemaFromTopics() throws Exception {
    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    schemaRegistry.register(
        "topic0-value",
        new AvroSchema(SchematizationTestUtils.AVRO_SCHEMA_FOR_SCHEMA_COLLECTION_0));
    schemaRegistry.register(
        "topic1-value",
        new AvroSchema(SchematizationTestUtils.AVRO_SCHEMA_FOR_SCHEMA_COLLECTION_1));
    Map<String, String> topicToTableMap = new HashMap<>();
    topicToTableMap.put("topic0", "table");
    topicToTableMap.put("topic1", "table");
    Map<String, String> schemaMap =
        SchematizationUtils.getSchemaMapForTableWithSchemaRegistryClient(
            "table", topicToTableMap, schemaRegistry);

    assert schemaMap.get("ID").equals("int");
    assert schemaMap.get("FIRST_NAME").equals("string");
    assert schemaMap.get("LAST_NAME").equals("string");
  }

  @Test
  public void testValidAvroValueConverter() {
    Map<String, String> config = new HashMap<>();
    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
        SnowflakeSinkConnectorConfig.CONFLUENT_AVRO_CONVERTER);
    assert SchematizationUtils.usesAvroValueConverter(config);

    config = new HashMap<>();
    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
        "com.snowflake.kafka.connector.records.SnowflakeAvroConverter");
    assert !SchematizationUtils.usesAvroValueConverter(config);
  }

  @Test
  public void testColumnNamesParsingFromMessage() {
    String message = "[a, b, c]";
    List<String> gold = new ArrayList<>();
    gold.add("a");
    gold.add("b");
    gold.add("c");
    List<String> columnNames = SchematizationUtils.getColumnNamesFromMessage(message);
    assert gold.equals(columnNames);

    message = "[a, \"Oh, I'm fine\", \"Terrible Column Name\", \",\"]";
    gold = new ArrayList<>();
    gold.add("a");
    gold.add("\"Oh, I'm fine\"");
    gold.add("\"Terrible Column Name\"");
    gold.add("\",\"");
    columnNames = SchematizationUtils.getColumnNamesFromMessage(message);
    assert gold.equals(columnNames);
  }

  @Test
  public void testCollectColumnToType() {
    String message =
        "Extra column: IMACOLUMN. Columns not present in the table shouldn't be specified.";
    Map<String, Object> recordMap = new HashMap<>();
    recordMap.put("ImAColumn", "AmI?");
    Map<String, String> schemaMap = new HashMap<>();
    Map<String, String> columnToType =
        SchematizationUtils.collectExtraColumnToType(recordMap, message, schemaMap);
    assert columnToType.containsKey("IMACOLUMN");
    assert columnToType.get("IMACOLUMN").equals("VARCHAR");

    recordMap = new HashMap<>();
    schemaMap.put("ImAColumn", "string");
    columnToType = SchematizationUtils.collectExtraColumnToType(recordMap, message, schemaMap);
    assert columnToType.containsKey("IMACOLUMN");
    assert columnToType.get("IMACOLUMN").equals("string");

    message =
        "Extra columns: [ImAColumn, \"ImInQuotes?\"]. Columns not present in the table shouldn't be"
            + " specified.";
    recordMap = new HashMap<>();
    recordMap.put("ImAColumn", "AmI?");
    recordMap.put("\"ImInQuotes?\"", true);
    schemaMap = new HashMap<>();
    columnToType = SchematizationUtils.collectExtraColumnToType(recordMap, message, schemaMap);
    assert columnToType.containsKey("ImAColumn");
    assert columnToType.get("ImAColumn").equals("VARCHAR");
    assert columnToType.containsKey("\"ImInQuotes?\"");
    assert columnToType.get("\"ImInQuotes?\"").equals("BOOLEAN");

    recordMap = new HashMap<>();
    schemaMap.put("ImAColumn", "string");
    schemaMap.put("\"ImInQuotes?\"", "boolean");
    columnToType = SchematizationUtils.collectExtraColumnToType(recordMap, message, schemaMap);
    assert columnToType.containsKey("ImAColumn");
    assert columnToType.get("ImAColumn").equals("string");
    assert columnToType.containsKey("\"ImInQuotes?\"");
    assert columnToType.get("\"ImInQuotes?\"").equals("boolean");

    message = "ImASentence, am I?";
    columnToType = SchematizationUtils.collectExtraColumnToType(recordMap, message, schemaMap);
    assert columnToType.isEmpty();
  }

  @Test
  public void testCollectNonNullableColumns() {
    String message =
        "Missing column: ImAColumn. Values for all non-nullable columns must be specified.";
    List<String> columnNames = SchematizationUtils.collectNonNullableColumns(message);
    assert columnNames.contains("ImAColumn");

    message =
        "Missing columns: [ImAColumn, \"ImInQuotes?\"]. Values for all non-nullable columns must be"
            + " specified.";
    columnNames = SchematizationUtils.collectNonNullableColumns(message);
    assert columnNames.contains("ImAColumn");
    assert columnNames.contains("\"ImInQuotes?\"");

    message = "ImASentence, am I?";
    columnNames = SchematizationUtils.collectNonNullableColumns(message);
    assert columnNames.isEmpty();
  }
}
