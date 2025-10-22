package com.snowflake.kafka.connector.records;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
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

  public static SchemaAndValue getNull() {
    return new SchemaAndValue(Schema.STRING_SCHEMA, null);
  }

  private static class Case {
    final String name;
    final SchemaAndValue key;
    final SchemaAndValue value;
    final JsonNode expected;

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
