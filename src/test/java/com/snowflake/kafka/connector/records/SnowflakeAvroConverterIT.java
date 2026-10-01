package com.snowflake.kafka.connector.records;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link SnowflakeAvroConverter} end to end against a mock schema registry: a real
 * {@link KafkaAvroSerializer} produces the wire bytes, and the converter is driven through its
 * real {@link SnowflakeAvroConverter#configure} entry point rather than the test-only
 * constructor, so both schema resolution and the delegate Avro-to-Struct conversion run for real.
 */
class SnowflakeAvroConverterIT {

  private static final String SCHEMA_REGISTRY_SCOPE = "snowflake-avro-converter-it";
  private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
  private static final String TOPIC = "snowflake-avro-converter-it-topic";

  private static final Schema SCHEMA_V1 =
      new Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"Widget\",\"fields\":["
                  + "{\"name\":\"name\",\"type\":\"string\"}]}");

  private static final Schema SCHEMA_V2 =
      new Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"Widget\",\"fields\":["
                  + "{\"name\":\"name\",\"type\":\"string\"},"
                  + "{\"name\":\"quantity\",\"type\":\"int\",\"default\":0}]}");

  @AfterEach
  void afterEach() {
    MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
  }

  @Test
  void toConnectData_resolvesRealSchemaFromMockRegistry_andDelegatesRealConversion() {
    SchemaRegistryClient registryClient = MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE);
    byte[] wireBytes = serialize(registryClient, widgetRecord(SCHEMA_V1, "widget-a"));

    SnowflakeAvroConverter converter = new SnowflakeAvroConverter();
    converter.configure(converterConfig(), false);

    SchemaAndValue converted = converter.toConnectData(TOPIC, wireBytes);

    assertEquals(SCHEMA_V1, converter.getLatestAvroSchema(TOPIC));
    assertEquals("widget-a", ((Struct) converted.value()).getString("name"));
  }

  @Test
  void toConnectData_schemaChangesBetweenRecordsOnSameTopic_latestAvroSchemaTracksMostRecentRecord() {
    SchemaRegistryClient registryClient = MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE);
    SnowflakeAvroConverter converter = new SnowflakeAvroConverter();
    converter.configure(converterConfig(), false);

    converter.toConnectData(TOPIC, serialize(registryClient, widgetRecord(SCHEMA_V1, "widget-a")));
    assertEquals(SCHEMA_V1, converter.getLatestAvroSchema(TOPIC));

    GenericRecord v2Record = new GenericData.Record(SCHEMA_V2);
    v2Record.put("name", "widget-b");
    v2Record.put("quantity", 5);

    SchemaAndValue converted = converter.toConnectData(TOPIC, serialize(registryClient, v2Record));

    assertEquals(SCHEMA_V2, converter.getLatestAvroSchema(TOPIC));
    assertEquals(5, ((Struct) converted.value()).getInt32("quantity"));
  }

  private static GenericRecord widgetRecord(Schema schema, String name) {
    GenericRecord record = new GenericData.Record(schema);
    record.put("name", name);
    return record;
  }

  private static byte[] serialize(SchemaRegistryClient registryClient, GenericRecord record) {
    KafkaAvroSerializer serializer = new KafkaAvroSerializer(registryClient);
    serializer.configure(
        Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL),
        false);
    return serializer.serialize(TOPIC, record);
  }

  private static Map<String, String> converterConfig() {
    Map<String, String> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
    return config;
  }
}
