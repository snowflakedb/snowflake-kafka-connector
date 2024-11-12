package com.snowflake.kafka.connector.records;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.mock.MockSchemaRegistryClient;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

public class ProcessRecordTest {
  private static final String TOPIC = "test";
  private static final int PARTITION = 0;
  private static final String TEST_VALUE_FILE_NAME = "test.avro";
  private static final String TEST_KEY_FILE_NAME = "test_key.avro";
  private static final String TEST_MULTI_LINE_AVRO_FILE_NAME = "test_multi.avro";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @ParameterizedTest(name = "{index}: {0}")
  @MethodSource("data")
  public void test(Case testCase) throws IOException {
    RecordService service = RecordServiceFactory.createRecordService(false, false);

    SinkRecord record =
        new SinkRecord(
            TOPIC,
            PARTITION,
            testCase.key.schema(),
            testCase.key.value(),
            testCase.value.schema(),
            testCase.value.value(),
            PARTITION);

    String got = service.getProcessedRecordForSnowpipe(record);

    assertEquals(testCase.expected, MAPPER.readTree(got));
  }

  public static Iterable<Case> data() throws IOException {
    return Arrays.asList(
        new Case(
            "string key, avro value",
            getString(),
            getAvro(),
            MAPPER.readTree(
                "{\"content\":{\"int\":222},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"schema_id\":1,\"key\":\"string"
                    + " value\"}}")),
        new Case(
            "string key, avro without registry value",
            getString(),
            getAvroWithoutRegistryValue(),
            MAPPER.readTree(
                "{\"content\":{\"name\":\"foo\",\"age\":30},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":\"string"
                    + " value\"}}{\"content\":{\"name\":\"bar\",\"age\":29},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":\"string"
                    + " value\"}}")),
        new Case(
            "string key, json value",
            getString(),
            getJson(),
            MAPPER.readTree(
                "{\"content\":{\"some_field\":\"some_value\"},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":\"string"
                    + " value\"}}")),
        new Case(
            "avro key, avro value",
            getAvro(),
            getAvro(),
            MAPPER.readTree(
                "{\"content\":{\"int\":222},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"schema_id\":1,\"key\":{\"int\":222},\"key_schema_id\":1}}")),
        new Case(
            "avro key, avro without registry value",
            getAvro(),
            getAvroWithoutRegistryValue(),
            MAPPER.readTree(
                "{\"content\":{\"name\":\"foo\",\"age\":30},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":{\"int\":222},\"key_schema_id\":1}}{\"content\":{\"name\":\"bar\",\"age\":29},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":[{\"int\":222}],\"key_schema_id\":1}}")),
        new Case(
            "avro key, json value",
            getAvro(),
            getJson(),
            MAPPER.readTree(
                "{\"content\":{\"some_field\":\"some_value\"},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":{\"int\":222},\"key_schema_id\":1}}")),
        new Case(
            "avro without registry key, avro value",
            getAvroWithoutRegistryKey(),
            getAvro(),
            MAPPER.readTree(
                "{\"content\":{\"int\":222},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"schema_id\":1,\"key\":{\"id\":\"aabbccdd\"}}}")),
        new Case(
            "avro without registry key, avro without registry value",
            getAvroWithoutRegistryKey(),
            getAvroWithoutRegistryValue(),
            MAPPER.readTree(
                "{\"content\":{\"name\":\"foo\",\"age\":30},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":{\"id\":\"aabbccdd\"}}}{\"content\":{\"name\":\"bar\",\"age\":29},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":[{\"id\":\"aabbccdd\"}]}}")),
        new Case(
            "avro without registry key, json value",
            getAvroWithoutRegistryKey(),
            getJson(),
            MAPPER.readTree(
                "{\"content\":{\"some_field\":\"some_value\"},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":{\"id\":\"aabbccdd\"}}}")),
        new Case(
            "json key, avro value",
            getJson(),
            getAvro(),
            MAPPER.readTree(
                "{\"content\":{\"int\":222},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"schema_id\":1,\"key\":{\"some_field\":\"some_value\"}}}")),
        new Case(
            "json key, avro without registry value",
            getJson(),
            getAvroWithoutRegistryValue(),
            MAPPER.readTree(
                "{\"content\":{\"name\":\"foo\",\"age\":30},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":{\"some_field\":\"some_value\"}}}{\"content\":{\"name\":\"bar\",\"age\":29},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":[{\"some_field\":\"some_value\"}]}}")),
        new Case(
            "json key, json value",
            getJson(),
            getJson(),
            MAPPER.readTree(
                "{\"content\":{\"some_field\":\"some_value\"},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":{\"some_field\":\"some_value\"}}}")),
        new Case(
            "multi line avro key, multi line avro value",
            getAvroMultiLine(),
            getJson(),
            MAPPER.readTree(
                "{\"content\":{\"some_field\":\"some_value\"},\"meta\":{\"topic\":\"test\",\"offset\":0,\"partition\":0,\"key\":[{\"username\":\"miguno\",\"tweet\":\"Rock:"
                    + " Nerf paper, scissors is"
                    + " fine.\",\"timestamp\":1366150681},{\"username\":\"BlizzardCS\",\"tweet\":\"Works"
                    + " as intended.  Terran is IMBA.\",\"timestamp\":1366154481}]}}")),
        new Case(
            "json key, null value",
            getJson(),
            getNull(),
            MAPPER.readTree(
                "{\"content\":{},\"meta\":{\"topic\":\"test\",\"offset\":0,\"partition\":0,\"schema_id\":0,\"key\":{\"some_field\":\"some_value\"}}}")),
        new Case(
            "null key, json value",
            getNull(),
            getJson(),
            MAPPER.readTree(
                "{\"content\":{\"some_field\":\"some_value\"},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0}}")),
        new Case(
            "null key, null value",
            getNull(),
            getNull(),
            MAPPER.readTree(
                "{\"content\":{},\"meta\":{\"topic\":\"test\",\"offset\":0,\"partition\":0,\"schema_id\":0}}")));
  }

  public static SchemaAndValue getString() {
    String value = "string value";
    return new SchemaAndValue(Schema.STRING_SCHEMA, value);
  }

  public static SchemaAndValue getAvro() throws IOException {
    MockSchemaRegistryClient client = new MockSchemaRegistryClient();
    SnowflakeAvroConverter avroConverter = new SnowflakeAvroConverter();
    avroConverter.setSchemaRegistry(client);

    String value = "{\"int\" : 222}";

    return avroConverter.toConnectData(TOPIC, client.serializeJson(value));
  }

  public static SchemaAndValue getAvroWithoutRegistryKey() throws IOException {
    SnowflakeAvroConverterWithoutSchemaRegistry avroConverterWithoutSchemaRegistry =
        new SnowflakeAvroConverterWithoutSchemaRegistry();

    URL resource = ConverterTest.class.getResource(TEST_KEY_FILE_NAME);
    byte[] value = Files.readAllBytes(Paths.get(resource.getFile()));

    return avroConverterWithoutSchemaRegistry.toConnectData(TOPIC, value);
  }

  public static SchemaAndValue getAvroWithoutRegistryValue() throws IOException {
    SnowflakeAvroConverterWithoutSchemaRegistry avroConverterWithoutSchemaRegistry =
        new SnowflakeAvroConverterWithoutSchemaRegistry();

    URL resource = ConverterTest.class.getResource(TEST_VALUE_FILE_NAME);
    byte[] value = Files.readAllBytes(Paths.get(resource.getFile()));

    return avroConverterWithoutSchemaRegistry.toConnectData(TOPIC, value);
  }

  public static SchemaAndValue getAvroMultiLine() throws IOException {
    SnowflakeAvroConverterWithoutSchemaRegistry avroConverterWithoutSchemaRegistry =
        new SnowflakeAvroConverterWithoutSchemaRegistry();

    URL resource = ConverterTest.class.getResource(TEST_MULTI_LINE_AVRO_FILE_NAME);
    byte[] value = Files.readAllBytes(Paths.get(resource.getFile()));

    return avroConverterWithoutSchemaRegistry.toConnectData(TOPIC, value);
  }

  public static SchemaAndValue getJson() {
    SnowflakeJsonConverter jsonConverter = new SnowflakeJsonConverter();

    String value = "{\"some_field\" : \"some_value\"}";
    byte[] valueContents = (value).getBytes(StandardCharsets.UTF_8);

    return jsonConverter.toConnectData(TOPIC, valueContents);
  }

  public static SchemaAndValue getNull() {
    return new SchemaAndValue(Schema.STRING_SCHEMA, null);
  }

  private static class Case {
    String name;
    SchemaAndValue key;
    SchemaAndValue value;
    JsonNode expected;

    public Case(String name, SchemaAndValue key, SchemaAndValue value, JsonNode expected) {
      this.name = name;
      this.key = key;
      this.value = value;
      this.expected = expected;
    }

    @Override
    public String toString() {
      return this.name;
    }
  }
}
