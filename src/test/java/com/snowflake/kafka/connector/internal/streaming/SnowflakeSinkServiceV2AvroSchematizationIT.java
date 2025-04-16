package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.internal.TestUtils.getTableContentOneRow;
import static com.snowflake.kafka.connector.internal.streaming.channel.TopicPartitionChannel.NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE;
import static org.awaitility.Awaitility.await;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkService;
import com.snowflake.kafka.connector.internal.SnowflakeSinkServiceFactory;
import com.snowflake.kafka.connector.internal.TestUtils;
import io.confluent.connect.avro.AvroConverter;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SnowflakeSinkServiceV2AvroSchematizationIT {

  private static final int PARTITION = 0;
  private static final int START_OFFSET = 0;

  private static final String ID_INT8 = "ID_INT8";
  private static final String ID_INT8_OPTIONAL = "ID_INT8_OPTIONAL";
  private static final String ID_INT16 = "ID_INT16";
  private static final String ID_INT32 = "ID_INT32";
  private static final String ID_INT64 = "ID_INT64";
  private static final String FIRST_NAME = "FIRST_NAME";
  private static final String RATING_FLOAT32 = "RATING_FLOAT32";
  private static final String FLOAT_NAN = "FLOAT_NAN";
  private static final String FLOAT_POSITIVE_INFINITY = "FLOAT_POSITIVE_INFINITY";
  private static final String FLOAT_NEGATIVE_INFINITY = "FLOAT_NEGATIVE_INFINITY";
  private static final String RATING_FLOAT64 = "RATING_FLOAT64";
  private static final String APPROVAL = "APPROVAL";
  private static final String INFO_ARRAY_STRING = "INFO_ARRAY_STRING";
  private static final String INFO_ARRAY_INT = "INFO_ARRAY_INT";
  private static final String INFO_ARRAY_JSON = "INFO_ARRAY_JSON";
  private static final String INFO_MAP = "INFO_MAP";
  private static final String RECORD_METADATA = "RECORD_METADATA";

  private static final Map<String, String> EXPECTED_AVRO_SCHEMA =
      new HashMap<String, String>() {
        {
          put(ID_INT8, "NUMBER");
          put(ID_INT8_OPTIONAL, "NUMBER");
          put(ID_INT16, "NUMBER");
          put(ID_INT32, "NUMBER");
          put(ID_INT64, "NUMBER");
          put(FIRST_NAME, "VARCHAR");
          put(RATING_FLOAT32, "FLOAT");
          put(FLOAT_NAN, "FLOAT");
          put(FLOAT_POSITIVE_INFINITY, "FLOAT");
          put(FLOAT_NEGATIVE_INFINITY, "FLOAT");
          put(RATING_FLOAT64, "FLOAT");
          put(APPROVAL, "BOOLEAN");
          put(INFO_ARRAY_STRING, "ARRAY");
          put(INFO_ARRAY_INT, "ARRAY");
          put(INFO_ARRAY_JSON, "ARRAY");
          put(INFO_MAP, "VARIANT");
          put(RECORD_METADATA, "VARIANT");
        }
      };

  private String table;
  private SnowflakeConnectionService conn;
  private String topic;
  private TopicPartition topicPartition;

  private SnowflakeSinkService service;

  @BeforeEach
  void before() {
    table = TestUtils.randomTableName();
    topic = table;
    conn = TestUtils.getConnectionServiceForStreaming();
    topicPartition = new TopicPartition(topic, PARTITION);
  }

  @AfterEach
  void after() {
    service.closeAll();
  }

  @Test
  public void testSchematizationWithTableCreationAndAvroInput() throws Exception {
    // given
    conn.createTableWithOnlyMetadataColumn(table);
    SinkRecord avroRecordValue = createSinkRecord();
    service = createService();

    // when
    // The first insert should fail and schema evolution will kick in to update the schema
    service.insert(Collections.singletonList(avroRecordValue));

    // then
    waitUntilOffsetEquals(NO_OFFSET_TOKEN_REGISTERED_IN_SNOWFLAKE);
    TestUtils.checkTableSchema(table, EXPECTED_AVRO_SCHEMA);

    // when
    // Retry the insert should succeed now with the updated schema
    service.insert(Collections.singletonList(avroRecordValue));

    // then
    waitUntilOffsetEquals(START_OFFSET + 1);

    Map<String, Object> actual = getTableContentOneRow(topic);
    Assertions.assertEquals(actual.get(ID_INT8), 0L);
    Assertions.assertNull(actual.get(ID_INT8_OPTIONAL));
    Assertions.assertEquals(actual.get(ID_INT16), 42L);
    Assertions.assertEquals(actual.get(ID_INT32), 42L);
    Assertions.assertEquals(actual.get(ID_INT64), 42L);
    Assertions.assertEquals(actual.get(FIRST_NAME), "zekai");
    Assertions.assertEquals(actual.get(RATING_FLOAT32), 0.99);
    Assertions.assertEquals(
        actual.get(FLOAT_NAN), Double.NaN); // float is extended to double on SF side
    Assertions.assertEquals(
        actual.get(FLOAT_POSITIVE_INFINITY),
        Double.POSITIVE_INFINITY); // float is extended to double on SF side
    Assertions.assertEquals(
        actual.get(FLOAT_NEGATIVE_INFINITY),
        Double.NEGATIVE_INFINITY); // float is extended to double on SF side
    Assertions.assertEquals(actual.get(RATING_FLOAT64), 0.99);
    Assertions.assertEquals(actual.get(APPROVAL), true);
    Assertions.assertEquals(
        StringUtils.deleteWhitespace(actual.get(INFO_ARRAY_STRING).toString()), "[\"a\",\"b\"]");
    Assertions.assertEquals(
        StringUtils.deleteWhitespace(actual.get(INFO_ARRAY_INT).toString()), "[1,2]");
    Assertions.assertEquals(
        StringUtils.deleteWhitespace(actual.get(INFO_ARRAY_JSON).toString()),
        "[null,\"{\\\"a\\\":1,\\\"b\\\":null,\\\"c\\\":null,\\\"d\\\":\\\"89asda9s0a\\\"}\"]");
    Assertions.assertEquals(
        StringUtils.deleteWhitespace(actual.get(INFO_MAP).toString()), "{\"field\":3}");
  }

  private SnowflakeSinkService createService() {
    Map<String, String> config = prepareConfig();
    return SnowflakeSinkServiceFactory.builder(
            conn, IngestionMethodConfig.SNOWPIPE_STREAMING, config)
        .setErrorReporter(new InMemoryKafkaRecordErrorReporter())
        .setSinkTaskContext(new InMemorySinkTaskContext(Collections.singleton(topicPartition)))
        .addTask(table, new TopicPartition(topic, PARTITION))
        .build();
  }

  private SinkRecord createSinkRecord() {
    Schema schema = prepareSchema();
    Struct data = prepareData(schema);
    AvroConverter avroConverter = prepareAvroConverter();

    byte[] converted = avroConverter.fromConnectData(topic, data.schema(), data);
    conn.createTableWithOnlyMetadataColumn(table);

    SchemaAndValue avroInputValue = avroConverter.toConnectData(topic, converted);

    return new SinkRecord(
        topic,
        PARTITION,
        Schema.STRING_SCHEMA,
        "test",
        avroInputValue.schema(),
        avroInputValue.value(),
        START_OFFSET);
  }

  private AvroConverter prepareAvroConverter() {
    SchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
    AvroConverter avroConverter = new AvroConverter(schemaRegistry);
    avroConverter.configure(
        Collections.singletonMap("schema.registry.url", "http://fake-url"), false);
    return avroConverter;
  }

  private Map<String, String> prepareConfig() {
    Map<String, String> config = TestUtils.getConfForStreaming();
    config.put(SnowflakeSinkConnectorConfig.ENABLE_SCHEMATIZATION_CONFIG, "true");
    config.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD,
        "io.confluent.connect.avro.AvroConverter");
    config.put(SnowflakeSinkConnectorConfig.VALUE_SCHEMA_REGISTRY_CONFIG_FIELD, "http://fake-url");
    SnowflakeSinkConnectorConfig.setDefaultValues(config);
    return config;
  }

  private Schema prepareSchema() {
    SchemaBuilder schemaBuilder =
        SchemaBuilder.struct()
            .field(ID_INT8, Schema.INT8_SCHEMA)
            .field(ID_INT8_OPTIONAL, Schema.OPTIONAL_INT8_SCHEMA)
            .field(ID_INT16, Schema.INT16_SCHEMA)
            .field(ID_INT32, Schema.INT32_SCHEMA)
            .field(ID_INT64, Schema.INT64_SCHEMA)
            .field(FIRST_NAME, Schema.STRING_SCHEMA)
            .field(RATING_FLOAT32, Schema.FLOAT32_SCHEMA)
            .field(FLOAT_NAN, Schema.FLOAT32_SCHEMA)
            .field(FLOAT_POSITIVE_INFINITY, Schema.FLOAT32_SCHEMA)
            .field(FLOAT_NEGATIVE_INFINITY, Schema.FLOAT32_SCHEMA)
            .field(RATING_FLOAT64, Schema.FLOAT64_SCHEMA)
            .field(APPROVAL, Schema.BOOLEAN_SCHEMA)
            .field(INFO_ARRAY_STRING, SchemaBuilder.array(Schema.STRING_SCHEMA).build())
            .field(INFO_ARRAY_INT, SchemaBuilder.array(Schema.INT32_SCHEMA).build())
            .field(INFO_ARRAY_JSON, SchemaBuilder.array(Schema.OPTIONAL_STRING_SCHEMA).build())
            .field(INFO_MAP, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.INT32_SCHEMA).build());
    return schemaBuilder.build();
  }

  private Struct prepareData(Schema schema) {
    return new Struct(schema)
        .put(ID_INT8, (byte) 0)
        .put(ID_INT16, (short) 42)
        .put(ID_INT32, 42)
        .put(ID_INT64, 42L)
        .put(FIRST_NAME, "zekai")
        .put(RATING_FLOAT32, 0.99f)
        .put(FLOAT_NAN, Float.NaN)
        .put(FLOAT_POSITIVE_INFINITY, Float.POSITIVE_INFINITY)
        .put(FLOAT_NEGATIVE_INFINITY, Float.NEGATIVE_INFINITY)
        .put(RATING_FLOAT64, 0.99d)
        .put(APPROVAL, true)
        .put(INFO_ARRAY_STRING, Arrays.asList("a", "b"))
        .put(INFO_ARRAY_INT, Arrays.asList(1, 2))
        .put(
            INFO_ARRAY_JSON,
            Arrays.asList(null, "{\"a\": 1, \"b\": null, \"c\": null, \"d\": \"89asda9s0a\"}"))
        .put(INFO_MAP, Collections.singletonMap("field", 3));
  }

  private void waitUntilOffsetEquals(long expectedOffset) {
    await()
        .timeout(Duration.ofSeconds(60))
        .until(() -> service.getOffset(new TopicPartition(topic, PARTITION)) == expectedOffset);
  }
}
