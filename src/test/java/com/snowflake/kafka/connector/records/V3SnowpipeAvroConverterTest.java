package com.snowflake.kafka.connector.records;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.snowflake.kafka.connector.Utils;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class V3SnowpipeAvroConverterTest {

  private static final String TOPIC = "topic";

  // Same settings as the Snowpipe Streaming SDK row serializer.
  private static final ObjectMapper SDK_MAPPER =
      JsonMapper.builder()
          .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
          .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
          .configure(SerializationFeature.INDENT_OUTPUT, false)
          .configure(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES, true)
          .build();

  // Compares numbers exactly so a precision difference is not hidden by double parsing.
  private static final ObjectMapper EXACT =
      JsonMapper.builder()
          .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
          .enable(DeserializationFeature.USE_BIG_INTEGER_FOR_INTS)
          .build();

  private MockSchemaRegistryClient registry;
  private KafkaAvroSerializer serializer;
  private V3SnowpipeAvroReferenceDecoder reference;

  @BeforeEach
  void setUp() {
    registry = new MockSchemaRegistryClient();
    serializer = new KafkaAvroSerializer(registry);
    serializer.configure(registryConfig(), false);
    reference = new V3SnowpipeAvroReferenceDecoder(registry);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("corpus")
  void recordContentMatchesKc3(String name, Schema fieldSchema, Object fieldValue)
      throws Exception {
    Schema schema =
        SchemaBuilder.record("Row")
            .namespace("t")
            .fields()
            .name("v")
            .type(fieldSchema)
            .noDefault()
            .endRecord();
    GenericRecord record = new GenericData.Record(schema);
    record.put("v", fieldValue);

    assertRecordContentMatchesKc3(
        converter(Collections.emptyMap()), serializer.serialize(TOPIC, record), null);
  }

  static Stream<Arguments> corpus() {
    Schema fixed4 = Schema.createFixed("F4", null, "t", 4);
    Schema color = Schema.createEnum("Color", null, "t", Arrays.asList("RED", "GREEN"));
    Schema cat =
        SchemaBuilder.record("Cat").namespace("t").fields().requiredString("name").endRecord();
    Schema dog = SchemaBuilder.record("Dog").namespace("t").fields().requiredInt("age").endRecord();
    Schema multi =
        Schema.createUnion(
            Schema.create(Schema.Type.NULL),
            Schema.create(Schema.Type.STRING),
            Schema.create(Schema.Type.INT));
    Schema nullableString =
        Schema.createUnion(Schema.create(Schema.Type.NULL), Schema.create(Schema.Type.STRING));
    Schema nestedWithUnion =
        SchemaBuilder.record("Customer")
            .namespace("t")
            .fields()
            .name("email")
            .type(multi)
            .noDefault()
            .name("address")
            .type(
                SchemaBuilder.record("Address")
                    .namespace("t")
                    .fields()
                    .requiredString("city")
                    .endRecord())
            .noDefault()
            .endRecord();
    Schema node =
        SchemaBuilder.record("Node")
            .namespace("t")
            .fields()
            .requiredInt("value")
            .name("next")
            .type()
            .unionOf()
            .nullType()
            .and()
            .type("t.Node")
            .endUnion()
            .nullDefault()
            .endRecord();
    Schema decimalBytes = LogicalTypes.decimal(10, 2).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema decimal38 = LogicalTypes.decimal(38, 10).addToSchema(Schema.create(Schema.Type.BYTES));
    Schema decimalFixed =
        LogicalTypes.decimal(9, 3).addToSchema(Schema.createFixed("D8", null, "t", 8));

    GenericRecord catValue = new GenericData.Record(cat);
    catValue.put("name", "Tom");
    GenericRecord dogValue = new GenericData.Record(dog);
    dogValue.put("age", 3);
    GenericRecord customer = new GenericData.Record(nestedWithUnion);
    customer.put("email", "a@b.c");
    GenericRecord address = new GenericData.Record(nestedWithUnion.getField("address").schema());
    address.put("city", "Paris");
    customer.put("address", address);
    GenericRecord tail = new GenericData.Record(node);
    tail.put("value", 2);
    tail.put("next", null);
    GenericRecord head = new GenericData.Record(node);
    head.put("value", 1);
    head.put("next", tail);
    Map<String, Object> unionMap = new LinkedHashMap<>();
    unionMap.put("s", "x");
    unionMap.put("i", 5);
    unionMap.put("n", null);

    return Stream.of(
        Arguments.of("int", Schema.create(Schema.Type.INT), 7),
        Arguments.of("long", Schema.create(Schema.Type.LONG), 9_000_000_000L),
        Arguments.of("float", Schema.create(Schema.Type.FLOAT), 1.5f),
        Arguments.of("double", Schema.create(Schema.Type.DOUBLE), 0.1),
        Arguments.of("boolean", Schema.create(Schema.Type.BOOLEAN), true),
        Arguments.of(
            "string with escapes", Schema.create(Schema.Type.STRING), "café \"q\" \\ \n \u0001 😀"),
        Arguments.of(
            "bytes",
            Schema.create(Schema.Type.BYTES),
            ByteBuffer.wrap("hiÿ".getBytes(StandardCharsets.ISO_8859_1))),
        Arguments.of("fixed", fixed4, new GenericData.Fixed(fixed4, new byte[] {1, -1, 0, 127})),
        Arguments.of("enum", color, new GenericData.EnumSymbol(color, "GREEN")),
        Arguments.of(
            "uuid",
            LogicalTypes.uuid().addToSchema(Schema.create(Schema.Type.STRING)),
            "123e4567-e89b-12d3-a456-426614174000"),
        Arguments.of("nullable union with value", nullableString, "v"),
        Arguments.of("nullable union with null", nullableString, null),
        Arguments.of("multi-type union string branch", multi, "m"),
        Arguments.of("multi-type union int branch", multi, 42),
        Arguments.of("union of named records", Schema.createUnion(cat, dog), dogValue),
        Arguments.of("nested record with multi-type union", nestedWithUnion, customer),
        Arguments.of("recursive record", node, head),
        Arguments.of(
            "array of multi-type unions", Schema.createArray(multi), Arrays.asList("x", 5, null)),
        Arguments.of("map of multi-type unions", Schema.createMap(multi), unionMap),
        Arguments.of(
            "empty array",
            Schema.createArray(Schema.create(Schema.Type.LONG)),
            Collections.emptyList()),
        Arguments.of(
            "empty map", Schema.createMap(Schema.create(Schema.Type.LONG)), Collections.emptyMap()),
        Arguments.of("decimal", decimalBytes, unscaled("12345.67")),
        Arguments.of("negative decimal", decimalBytes, unscaled("-0.05")),
        Arguments.of(
            "38-digit decimal", decimal38, unscaled("1234567890123456789012345678.0123456789")),
        Arguments.of(
            "decimal on fixed",
            decimalFixed,
            new GenericData.Fixed(decimalFixed, fixedUnscaled("-123456.789", 8))),
        Arguments.of(
            "date", LogicalTypes.date().addToSchema(Schema.create(Schema.Type.INT)), 19737),
        Arguments.of(
            "time-millis",
            LogicalTypes.timeMillis().addToSchema(Schema.create(Schema.Type.INT)),
            37_800_000),
        Arguments.of(
            "time-micros",
            LogicalTypes.timeMicros().addToSchema(Schema.create(Schema.Type.LONG)),
            37_800_000_123L),
        Arguments.of(
            "timestamp-millis",
            LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG)),
            1_705_312_800_000L),
        Arguments.of(
            "timestamp-micros",
            LogicalTypes.timestampMicros().addToSchema(Schema.create(Schema.Type.LONG)),
            1_705_312_800_000_123L),
        Arguments.of(
            "local-timestamp-millis",
            LogicalTypes.localTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG)),
            1_705_312_800_000L),
        Arguments.of(
            "local-timestamp-micros",
            LogicalTypes.localTimestampMicros().addToSchema(Schema.create(Schema.Type.LONG)),
            1_705_312_800_000_123L),
        Arguments.of("double NaN", Schema.create(Schema.Type.DOUBLE), Double.NaN),
        Arguments.of(
            "double positive infinity",
            Schema.create(Schema.Type.DOUBLE),
            Double.POSITIVE_INFINITY),
        Arguments.of(
            "double negative infinity",
            Schema.create(Schema.Type.DOUBLE),
            Double.NEGATIVE_INFINITY),
        Arguments.of(
            "float positive infinity", Schema.create(Schema.Type.FLOAT), Float.POSITIVE_INFINITY));
  }

  @Test
  void readerSchemaProjectsAndFillsDefaultsLikeKc3() throws Exception {
    Schema writer =
        SchemaBuilder.record("Event")
            .namespace("t")
            .fields()
            .requiredString("id")
            .requiredString("dropped")
            .requiredInt("count")
            .endRecord();
    String readerJson =
        "{\"type\":\"record\",\"name\":\"Event\",\"namespace\":\"t\",\"fields\":["
            + "{\"name\":\"id\",\"type\":\"string\"},"
            + "{\"name\":\"count\",\"type\":\"long\"},"
            + "{\"name\":\"source\",\"type\":\"string\",\"default\":\"legacy\"}]}";
    GenericRecord record = new GenericData.Record(writer);
    record.put("id", "e1");
    record.put("dropped", "gone");
    record.put("count", 3);

    V3SnowpipeAvroConverter converter =
        converter(
            Collections.singletonMap(V3SnowpipeAvroConverter.READER_SCHEMA_CONFIG, readerJson));
    JsonNode actual =
        assertRecordContentMatchesKc3(
            converter, serializer.serialize(TOPIC, record), new Schema.Parser().parse(readerJson));

    assertEquals("{\"id\":\"e1\",\"count\":3,\"source\":\"legacy\"}", actual.toString());
  }

  @Test
  void interleavedSchemaVersionsEachMatchKc3() throws Exception {
    Schema v1 =
        SchemaBuilder.record("Evolving").namespace("t").fields().requiredString("a").endRecord();
    Schema v2 =
        SchemaBuilder.record("Evolving")
            .namespace("t")
            .fields()
            .requiredString("a")
            .name("b")
            .type()
            .unionOf()
            .nullType()
            .and()
            .stringType()
            .and()
            .intType()
            .endUnion()
            .nullDefault()
            .endRecord();
    GenericRecord r1 = new GenericData.Record(v1);
    r1.put("a", "one");
    GenericRecord r2 = new GenericData.Record(v2);
    r2.put("a", "two");
    r2.put("b", 2);
    V3SnowpipeAvroConverter converter = converter(Collections.emptyMap());

    for (GenericRecord record : Arrays.asList(r1, r2, r1, r2)) {
      assertRecordContentMatchesKc3(converter, serializer.serialize(TOPIC, record), null);
    }
  }

  @Test
  void defaultConstructorReadsFromConfiguredRegistry() throws Exception {
    String scope = "v3-snowpipe-avro-default-constructor";
    Map<String, Object> config = new HashMap<>();
    config.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://" + scope);
    KafkaAvroSerializer scopedSerializer = new KafkaAvroSerializer();
    scopedSerializer.configure(config, false);
    Schema schema =
        SchemaBuilder.record("Row")
            .namespace("t")
            .fields()
            .name("v")
            .type()
            .unionOf()
            .nullType()
            .and()
            .stringType()
            .and()
            .intType()
            .endUnion()
            .nullDefault()
            .endRecord();
    GenericRecord record = new GenericData.Record(schema);
    record.put("v", "m");
    V3SnowpipeAvroConverter converter = new V3SnowpipeAvroConverter();
    converter.configure(config, false);
    try {
      SchemaAndValue converted =
          converter.toConnectData(TOPIC, scopedSerializer.serialize(TOPIC, record));

      assertNull(converted.schema());
      assertEquals(Collections.singletonMap("v", "m"), converted.value());
    } finally {
      MockSchemaRegistry.dropScope(scope);
    }
  }

  @Test
  void nullPayloadIsTombstone() {
    assertSame(SchemaAndValue.NULL, converter(Collections.emptyMap()).toConnectData(TOPIC, null));
  }

  @Test
  void payloadWithoutConfluentFramingFails() {
    byte[] notFramed = "{\"a\":1}".getBytes(StandardCharsets.UTF_8);
    assertThrows(
        DataException.class,
        () -> converter(Collections.emptyMap()).toConnectData(TOPIC, notFramed));
  }

  @Test
  void unknownSchemaIdFails() {
    byte[] unknownId = {0, 0, 0, 0x7f, 0x7f, 2, 'x'};
    assertThrows(
        DataException.class,
        () -> converter(Collections.emptyMap()).toConnectData(TOPIC, unknownId));
  }

  @Test
  void invalidReaderSchemaFailsAtConfigure() {
    V3SnowpipeAvroConverter converter = new V3SnowpipeAvroConverter(registry);
    Map<String, Object> config = registryConfig();
    config.put(V3SnowpipeAvroConverter.READER_SCHEMA_CONFIG, "{not a schema");
    assertThrows(ConfigException.class, () -> converter.configure(config, false));
  }

  @Test
  void missingRegistryUrlFailsAtConfigure() {
    assertThrows(
        ConfigException.class,
        () -> new V3SnowpipeAvroConverter().configure(new HashMap<>(), false));
  }

  @Test
  void serializingIsRejected() {
    assertThrows(
        UnsupportedOperationException.class,
        () -> converter(Collections.emptyMap()).fromConnectData(TOPIC, null, "x"));
  }

  @Test
  void ruleExecutorDiscoveryIsOffByDefaultAndUserValueWins() {
    Map<String, Object> unset = new HashMap<>();
    assertEquals(
        false,
        V3SnowpipeAvroConverter.withDefaults(unset)
            .get(AbstractKafkaSchemaSerDeConfig.RULE_SERVICE_LOADER_ENABLE));
    assertTrue(unset.isEmpty());
    Map<String, Object> userSet = new HashMap<>();
    userSet.put(AbstractKafkaSchemaSerDeConfig.RULE_SERVICE_LOADER_ENABLE, true);
    assertEquals(
        true,
        V3SnowpipeAvroConverter.withDefaults(userSet)
            .get(AbstractKafkaSchemaSerDeConfig.RULE_SERVICE_LOADER_ENABLE));
  }

  /**
   * Runs {@code payload} through the converter and the KC v4 {@code RECORD_CONTENT} path with
   * schematization off, and asserts the resulting document equals the KC v3 one exactly.
   */
  private JsonNode assertRecordContentMatchesKc3(
      V3SnowpipeAvroConverter converter, byte[] payload, Schema readerSchema) throws Exception {
    String expected = reference.decode(payload, readerSchema).toString();

    SchemaAndValue converted = converter.toConnectData(TOPIC, payload);
    SinkRecord sinkRecord =
        new SinkRecord(TOPIC, 0, null, null, converted.schema(), converted.value(), 0);
    SnowflakeSinkRecord snowflakeRecord =
        SnowflakeSinkRecord.from(sinkRecord, new SnowflakeMetadataConfig(), false, false);
    String actual =
        SDK_MAPPER.writeValueAsString(snowflakeRecord.getContent().get(Utils.TABLE_COLUMN_CONTENT));

    assertEquals(
        EXACT.readTree(expected),
        EXACT.readTree(actual),
        "expected " + expected + " but was " + actual);
    return EXACT.readTree(actual);
  }

  private V3SnowpipeAvroConverter converter(Map<String, String> extraConfig) {
    V3SnowpipeAvroConverter converter = new V3SnowpipeAvroConverter(registry);
    Map<String, Object> config = registryConfig();
    config.putAll(extraConfig);
    converter.configure(config, false);
    return converter;
  }

  private static Map<String, Object> registryConfig() {
    Map<String, Object> config = new HashMap<>();
    config.put(
        AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://v3-snowpipe-avro");
    return config;
  }

  private static ByteBuffer unscaled(String decimal) {
    return ByteBuffer.wrap(new BigDecimal(decimal).unscaledValue().toByteArray());
  }

  private static byte[] fixedUnscaled(String decimal, int size) {
    byte[] raw = new BigDecimal(decimal).unscaledValue().toByteArray();
    byte[] padded = new byte[size];
    byte fill = (byte) (raw[0] < 0 ? -1 : 0);
    Arrays.fill(padded, 0, size - raw.length, fill);
    System.arraycopy(raw, 0, padded, size - raw.length, raw.length);
    return padded;
  }
}
