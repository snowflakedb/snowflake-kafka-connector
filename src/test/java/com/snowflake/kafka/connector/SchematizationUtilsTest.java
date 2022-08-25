package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.SchematizationTestUtils;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.util.HashMap;
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
}
