package com.snowflake.kafka.connector.records;

import com.snowflake.kafka.connector.mock.MockSchemaRegistryClient;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;


@RunWith(Parameterized.class)
public class ProcessRecordTest
{
  private static String topic = "test";
  private static int partition = 0;
  private static final String TEST_VALUE_FILE_NAME = "test.avro";
  private static final String TEST_KEY_FILE_NAME = "test_key.avro";

  private static ObjectMapper mapper = new ObjectMapper();

  private Case testCase;

  public ProcessRecordTest(Case testCase)
  {
    this.testCase = testCase;
  }

  @Test
  public void test() throws IOException
  {
    RecordService service = new RecordService();

    SinkRecord record = new SinkRecord(
        topic, partition,
        testCase.key.schema(), testCase.key.value(),
        testCase.value.schema(), testCase.value.value(),
        partition
    );

    String got = service.processRecord(record);

    assertEquals(testCase.expected, mapper.readTree(got));

  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable<Case> data() throws IOException
  {
    return Arrays.asList(
        new Case("string key, avro value",
            getString(),
            getAvro(),
            mapper.readTree("{\"content\":{\"int\":222},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"schema_id\":1,\"key\":\"string value\"}}")
        ),
        new Case("string key, avro without registry value",
            getString(),
            getAvroWithoutRegistryValue(),
            mapper.readTree("{\"content\":{\"name\":\"foo\",\"age\":30},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":\"string value\"}}{\"content\":{\"name\":\"bar\",\"age\":29},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":\"string value\"}}")
        ),
        new Case("string key, json value",
            getString(),
            getJson(),
            mapper.readTree("{\"content\":{\"some_field\":\"some_value\"},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":\"string value\"}}")
        ),
        new Case("avro key, avro value",
            getAvro(),
            getAvro(),
            mapper.readTree("{\"content\":{\"int\":222},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"schema_id\":1,\"key\":[{\"int\":222}],\"key_schema_id\":1}}")
        ),
        new Case("avro key, avro without registry value",
            getAvro(),
            getAvroWithoutRegistryValue(),
            mapper.readTree("{\"content\":{\"name\":\"foo\",\"age\":30},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":[{\"int\":222}],\"key_schema_id\":1}}{\"content\":{\"name\":\"bar\",\"age\":29},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":[{\"int\":222}],\"key_schema_id\":1}}")
        ),
        new Case("avro key, json value",
            getAvro(),
            getJson(),
            mapper.readTree("{\"content\":{\"some_field\":\"some_value\"},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":[{\"int\":222}],\"key_schema_id\":1}}")
        ),
        new Case("avro without registry key, avro value",
            getAvroWithoutRegistryKey(),
            getAvro(),
            mapper.readTree("{\"content\":{\"int\":222},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"schema_id\":1,\"key\":[{\"id\":\"aabbccdd\"}]}}")
        ),
        new Case("avro without registry key, avro without registry value",
            getAvroWithoutRegistryKey(),
            getAvroWithoutRegistryValue(),
            mapper.readTree("{\"content\":{\"name\":\"foo\",\"age\":30},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":[{\"id\":\"aabbccdd\"}]}}{\"content\":{\"name\":\"bar\",\"age\":29},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":[{\"id\":\"aabbccdd\"}]}}")
        ),
        new Case("avro without registry key, json value",
            getAvroWithoutRegistryKey(),
            getJson(),
            mapper.readTree("{\"content\":{\"some_field\":\"some_value\"},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":[{\"id\":\"aabbccdd\"}]}}")
        ),
        new Case("json key, avro value",
            getJson(),
            getAvro(),
            mapper.readTree("{\"content\":{\"int\":222},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"schema_id\":1,\"key\":[{\"some_field\":\"some_value\"}]}}")
        ),
        new Case("json key, avro without registry value",
            getJson(),
            getAvroWithoutRegistryValue(),
            mapper.readTree("{\"content\":{\"name\":\"foo\",\"age\":30},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":[{\"some_field\":\"some_value\"}]}}{\"content\":{\"name\":\"bar\",\"age\":29},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":[{\"some_field\":\"some_value\"}]}}")
        ),
        new Case("json key, json value",
            getJson(),
            getJson(),
            mapper.readTree("{\"content\":{\"some_field\":\"some_value\"},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0,\"key\":[{\"some_field\":\"some_value\"}]}}")
        ),
        new Case("null key, json value",
            getNull(),
            getJson(),
            mapper.readTree("{\"content\":{\"some_field\":\"some_value\"},\"meta\":{\"offset\":0,\"topic\":\"test\",\"partition\":0}}")
        )
    );
  }

  public static SchemaAndValue getString()
  {
    String value = "string value";
    return new SchemaAndValue(Schema.STRING_SCHEMA, value);
  }


  public static SchemaAndValue getAvro() throws IOException
  {
    MockSchemaRegistryClient client = new MockSchemaRegistryClient();
    SnowflakeAvroConverter avroConverter = new SnowflakeAvroConverter();
    avroConverter.setSchemaRegistry(client);

    String value = "{\"int\" : 222}";

    return avroConverter.toConnectData(topic, client.serializeJson(value));
  }

  public static SchemaAndValue getAvroWithoutRegistryKey() throws IOException
  {
    SnowflakeAvroConverterWithoutSchemaRegistry avroConverterWithoutSchemaRegistry = new SnowflakeAvroConverterWithoutSchemaRegistry();

    URL resource = ConverterTest.class.getResource(TEST_KEY_FILE_NAME);
    byte[] value = Files.readAllBytes(Paths.get(resource.getFile()));

    return avroConverterWithoutSchemaRegistry.toConnectData(topic, value);
  }

  public static SchemaAndValue getAvroWithoutRegistryValue() throws IOException
  {
    SnowflakeAvroConverterWithoutSchemaRegistry avroConverterWithoutSchemaRegistry = new SnowflakeAvroConverterWithoutSchemaRegistry();

    URL resource = ConverterTest.class.getResource(TEST_VALUE_FILE_NAME);
    byte[] value = Files.readAllBytes(Paths.get(resource.getFile()));

    return avroConverterWithoutSchemaRegistry.toConnectData(topic, value);
  }

  public static SchemaAndValue getJson()
  {
    SnowflakeJsonConverter jsonConverter = new SnowflakeJsonConverter();

    String value = "{\"some_field\" : \"some_value\"}";
    byte[] valueContents = (value).getBytes(StandardCharsets.UTF_8);

    return jsonConverter.toConnectData(topic, valueContents);
  }

  public static SchemaAndValue getNull()
  {
    return new SchemaAndValue(Schema.STRING_SCHEMA, null);
  }

  private static class Case
  {
    String name;
    SchemaAndValue key;
    SchemaAndValue value;
    JsonNode expected;

    public Case(String name, SchemaAndValue key, SchemaAndValue value, JsonNode expected)
    {
      this.name = name;
      this.key = key;
      this.value = value;
      this.expected = expected;
    }

    @Override
    public String toString()
    {
      return this.name;
    }
  }
}
