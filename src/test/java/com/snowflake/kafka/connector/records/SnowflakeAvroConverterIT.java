package com.snowflake.kafka.connector.records;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Exercises {@link SnowflakeAvroConverter} end to end against a mock schema registry: a real {@link
 * KafkaAvroSerializer} produces the wire bytes, and the converter is driven through its real {@link
 * SnowflakeAvroConverter#configure} entry point rather than the test-only constructor, so both
 * schema resolution and the delegate Avro-to-Struct conversion run for real.
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

  private static final Schema KEY_SCHEMA =
      new Schema.Parser()
          .parse(
              "{\"type\":\"record\",\"name\":\"WidgetKey\",\"fields\":["
                  + "{\"name\":\"id\",\"type\":\"int\"}]}");

  @AfterEach
  void afterEach() {
    MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
  }

  @Test
  void toConnectData_resolvesRealSchemaFromMockRegistry_andDelegatesRealConversion() {
    SchemaRegistryClient registryClient =
        MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE);
    byte[] wireBytes = serialize(registryClient, widgetRecord(SCHEMA_V1, "widget-a"));

    SnowflakeAvroConverter converter = new SnowflakeAvroConverter();
    converter.configure(converterConfig(), false);

    SchemaAndValue converted = converter.toConnectData(TOPIC, wireBytes);

    assertEquals(
        Optional.of(SCHEMA_V1), SnowflakeAvroConverter.extractAvroSchema(converted.schema()));
    assertEquals("widget-a", ((Struct) converted.value()).getString("name"));
  }

  @Test
  void toConnectData_schemaChangesBetweenRecordsOnSameTopic_eachRecordKeepsItsOwnSchema() {
    SchemaRegistryClient registryClient =
        MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE);
    SnowflakeAvroConverter converter = new SnowflakeAvroConverter();
    converter.configure(converterConfig(), false);

    byte[] v1Bytes = serialize(registryClient, widgetRecord(SCHEMA_V1, "widget-a"));
    SchemaAndValue v1Converted = converter.toConnectData(TOPIC, v1Bytes);

    GenericRecord v2Record = new GenericData.Record(SCHEMA_V2);
    v2Record.put("name", "widget-b");
    v2Record.put("quantity", 5);
    byte[] v2Bytes = serialize(registryClient, v2Record);
    SchemaAndValue v2Converted = converter.toConnectData(TOPIC, v2Bytes);

    // Converting the v2 record after v1 must not change what's already attached to the v1 record
    // -- unlike a converter-side cache keyed only by topic, each SchemaAndValue carries its own
    // schema.
    assertEquals(
        Optional.of(SCHEMA_V1), SnowflakeAvroConverter.extractAvroSchema(v1Converted.schema()));
    assertEquals(
        Optional.of(SCHEMA_V2), SnowflakeAvroConverter.extractAvroSchema(v2Converted.schema()));
    assertEquals(5, ((Struct) v2Converted.value()).getInt32("quantity"));
    // The same holds for the schema registry id.
    assertNotEquals(wireHeaderSchemaId(v1Bytes), wireHeaderSchemaId(v2Bytes));
    assertEquals(wireHeaderSchemaId(v1Bytes), attachedSchemaId(v1Converted));
    assertEquals(wireHeaderSchemaId(v2Bytes), attachedSchemaId(v2Converted));
  }

  @Test
  void toConnectData_keyAndValueConverters_schemaIdsReachRecordMetadata() {
    SchemaRegistryClient registryClient =
        MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE);
    GenericRecord key = new GenericData.Record(KEY_SCHEMA);
    key.put("id", 7);
    byte[] keyBytes = serialize(registryClient, key, true);
    byte[] valueBytes = serialize(registryClient, widgetRecord(SCHEMA_V1, "widget-a"), false);

    SnowflakeAvroConverter keyConverter = new SnowflakeAvroConverter();
    keyConverter.configure(converterConfig(), true);
    SnowflakeAvroConverter valueConverter = new SnowflakeAvroConverter();
    valueConverter.configure(converterConfig(), false);

    SchemaAndValue convertedKey = keyConverter.toConnectData(TOPIC, keyBytes);
    SchemaAndValue convertedValue = valueConverter.toConnectData(TOPIC, valueBytes);
    SinkRecord sinkRecord =
        new SinkRecord(
            TOPIC,
            0,
            convertedKey.schema(),
            convertedKey.value(),
            convertedValue.schema(),
            convertedValue.value(),
            0L);

    SnowflakeSinkRecord record =
        SnowflakeSinkRecord.from(sinkRecord, new SnowflakeMetadataConfig(), false, false);

    assertTrue(record.isValid());
    assertNotEquals(wireHeaderSchemaId(keyBytes), wireHeaderSchemaId(valueBytes));
    Map<String, Object> metadata = record.getMetadata();
    assertEquals(wireHeaderSchemaId(valueBytes), metadata.get(SnowflakeSinkRecord.SCHEMA_ID));
    assertEquals(wireHeaderSchemaId(keyBytes), metadata.get(SnowflakeSinkRecord.KEY_SCHEMA_ID));
  }

  private static GenericRecord widgetRecord(Schema schema, String name) {
    GenericRecord record = new GenericData.Record(schema);
    record.put("name", name);
    return record;
  }

  private static byte[] serialize(SchemaRegistryClient registryClient, GenericRecord record) {
    return serialize(registryClient, record, false);
  }

  private static byte[] serialize(
      SchemaRegistryClient registryClient, GenericRecord record, boolean isKey) {
    KafkaAvroSerializer serializer = new KafkaAvroSerializer(registryClient);
    serializer.configure(
        Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL),
        isKey);
    return serializer.serialize(TOPIC, record);
  }

  /** Reads the schema registry id that follows the magic byte in Confluent wire-format bytes. */
  private static int wireHeaderSchemaId(byte[] wireBytes) {
    return ByteBuffer.wrap(wireBytes, 1, 4).getInt();
  }

  private static int attachedSchemaId(SchemaAndValue converted) {
    return Integer.parseInt(
        converted.schema().parameters().get(SnowflakeAvroConverter.AVRO_SCHEMA_ID_PARAMETER_KEY));
  }

  private static Map<String, String> converterConfig() {
    Map<String, String> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
    return config;
  }
}
