package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.internal.TestUtils.assertTableColumnCount;
import static com.snowflake.kafka.connector.internal.TestUtils.assertWithRetry;

import com.snowflake.kafka.connector.internal.TestUtils;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.avro.Conversions;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration test for schema evolution using Avro with Schema Registry. Tests that the table is
 * updated with correct column types when records with different Avro schemas are sent from multiple
 * topics.
 */
class SchemaEvolutionAvroSrIT extends SchemaEvolutionBase {

  private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://test-schema-registry";

  private static final String PERFORMANCE_STRING = "PERFORMANCE_STRING";
  private static final String PERFORMANCE_CHAR = "PERFORMANCE_CHAR";
  private static final String RATING_INT = "RATING_INT";
  private static final String RATING_DOUBLE = "RATING_DOUBLE";
  private static final String APPROVAL = "APPROVAL";
  private static final String TIME_MILLIS = "TIME_MILLIS";
  private static final String TIMESTAMP_MILLIS = "TIMESTAMP_MILLIS";
  private static final String DATE = "DATE";
  private static final String DECIMAL = "DECIMAL";
  private static final String SOME_FLOAT_NAN = "SOME_FLOAT_NAN";
  private static final String RECORD_METADATA = "RECORD_METADATA";

  private static final Map<String, String> EXPECTED_SCHEMA = new HashMap();

  static {
    EXPECTED_SCHEMA.put(PERFORMANCE_STRING, "VARCHAR");
    EXPECTED_SCHEMA.put(PERFORMANCE_CHAR, "VARCHAR");
    EXPECTED_SCHEMA.put(RATING_INT, "NUMBER");
    EXPECTED_SCHEMA.put(
        RATING_DOUBLE, "NUMBER"); // no floats anymore in server side SSV2 schema evo)
    EXPECTED_SCHEMA.put(APPROVAL, "BOOLEAN");
    EXPECTED_SCHEMA.put(
        SOME_FLOAT_NAN, "VARCHAR"); // no floats anymore in server side SSV2 schema evo)
    EXPECTED_SCHEMA.put(TIME_MILLIS, "TIME");
    EXPECTED_SCHEMA.put(TIMESTAMP_MILLIS, "VARCHAR");
    EXPECTED_SCHEMA.put(DATE, "TIME");
    EXPECTED_SCHEMA.put(DECIMAL, "NUMBER");
    EXPECTED_SCHEMA.put(RECORD_METADATA, "VARIANT");
  }

  private static final String VALUE_SCHEMA_0 =
      "{\"type\": \"record\",\"name\": \"value_schema_0\",\"fields\": [  {\"name\":"
          + " \"PERFORMANCE_CHAR\", \"type\": \"string\"},  {\"name\": \"PERFORMANCE_STRING\","
          + " \"type\": \"string\"},"
          + " {\"name\":\"TIME_MILLIS\",\"type\":{\"type\":\"int\",\"logicalType\":\"time-millis\"}},"
          + "{\"name\":\"DATE\",\"type\":{\"type\":\"int\",\"logicalType\":\"date\"}},{\"name\":\"DECIMAL\",\"type\":{\"type\":\"bytes\",\"logicalType\":\"decimal\","
          + " \"precision\":4, \"scale\":2}},"
          + "{\"name\":\"TIMESTAMP_MILLIS\",\"type\":{\"type\":\"long\",\"logicalType\":\"timestamp-millis\"}},"
          + "  {\"name\": \"RATING_INT\", \"type\": \"int\"}]}";

  private static final String VALUE_SCHEMA_1 =
      "{"
          + "\"type\": \"record\","
          + "\"name\": \"value_schema_1\","
          + "\"fields\": ["
          + "  {\"name\": \"RATING_DOUBLE\", \"type\": \"float\"},"
          + "  {\"name\": \"PERFORMANCE_STRING\", \"type\": \"string\"},"
          + "  {\"name\": \"APPROVAL\", \"type\": \"boolean\"},"
          + "  {\"name\": \"SOME_FLOAT_NAN\", \"type\": \"float\"}"
          + "]"
          + "}";

  private static final String SCHEMA_REGISTRY_SCOPE = "test-schema-registry";
  private static final int COL_NUM = 11;

  private KafkaProducer<String, Object> avroProducer;

  @BeforeEach
  void beforeEach() {
    avroProducer = createAvroProducer();
  }

  @AfterEach
  void afterEach() {
    if (avroProducer != null) {
      avroProducer.close();
    }
    MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
  }

  @Test
  void testSchemaEvolutionWithMultipleTopicsAndAvroSr() throws Exception {
    // given
    final Map<String, String> config = createConnectorConfig();
    config.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, AvroConverter.class.getName());
    config.put("value.converter.schema.registry.url", MOCK_SCHEMA_REGISTRY_URL);
    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);

    // when
    sendRecordsToTopic0();
    sendRecordsToTopic1();

    // then
    final int expectedTotalRecords = TOPIC_COUNT * RECORD_COUNT;
    assertWithRetry(() -> snowflake.tableExist(tableName));
    assertWithRetry(() -> TestUtils.getNumberOfRows(tableName) == expectedTotalRecords);
    assertTableColumnCount(tableName, COL_NUM);
    TestUtils.checkTableSchema(tableName, EXPECTED_SCHEMA);
  }

  private KafkaProducer<String, Object> createAvroProducer() {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connectCluster.kafka().bootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put("schema.registry.url", MOCK_SCHEMA_REGISTRY_URL);
    return new KafkaProducer<>(props, new StringSerializer(), createAvroSerializer());
  }

  private KafkaAvroSerializer createAvroSerializer() {
    final SchemaRegistryClient schemaRegistryClient =
        MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE);
    final KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistryClient);
    serializer.configure(Map.of("schema.registry.url", MOCK_SCHEMA_REGISTRY_URL), false);
    return serializer;
  }

  private void sendRecordsToTopic0() {
    final Schema schema = new Schema.Parser().parse(VALUE_SCHEMA_0);
    for (int i = 0; i < RECORD_COUNT; i++) {
      final GenericRecord record = createTopic0Record(schema);
      avroProducer.send(new ProducerRecord<>(topic0, "key-" + i, record));
    }
    avroProducer.flush();
  }

  private void sendRecordsToTopic1() {
    final Schema schema = new Schema.Parser().parse(VALUE_SCHEMA_1);
    for (int i = 0; i < RECORD_COUNT; i++) {
      final GenericRecord record = createTopic1Record(schema);
      avroProducer.send(new ProducerRecord<>(topic1, "key-" + i, record));
    }
    avroProducer.flush();
  }

  private GenericRecord createTopic0Record(final Schema schema) {
    Schema decimalSchema = schema.getField(DECIMAL).schema();
    LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) decimalSchema.getLogicalType();
    BigDecimal value = new BigDecimal("0.03125");
    BigDecimal scaledValue = value.setScale(decimalType.getScale(), BigDecimal.ROUND_HALF_UP);
    ByteBuffer byteBuffer =
        new Conversions.DecimalConversion().toBytes(scaledValue, decimalSchema, decimalType);

    final GenericRecord record = new GenericData.Record(schema);
    record.put(PERFORMANCE_STRING, "Excellent");
    record.put(PERFORMANCE_CHAR, "A");
    record.put(RATING_INT, 100);
    record.put(TIME_MILLIS, 10);
    record.put(TIMESTAMP_MILLIS, 12);
    record.put(DECIMAL, byteBuffer);
    record.put(DATE, 11);
    return record;
  }

  private GenericRecord createTopic1Record(final Schema schema) {
    final GenericRecord record = new GenericData.Record(schema);
    record.put(PERFORMANCE_STRING, "Excellent");
    record.put(RATING_DOUBLE, 0.99f);
    record.put(APPROVAL, true);
    record.put(SOME_FLOAT_NAN, Float.NaN);
    return record;
  }
}
