package com.snowflake.kafka.connector.records;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind
    .ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node
    .ObjectNode;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.storage.Converter;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ConverterTest
{

  ObjectMapper mapper = new ObjectMapper();
  private static String TEST_FILE_NAME = "test.avro";

  @Test
  public void JsonConverterTest()
  {
    SnowflakeConverter converter = new SnowflakeJsonConverter();

    ObjectNode node = mapper.createObjectNode();

    node.put("str","test");
    node.put("num", 123);

    SchemaAndValue sv =
        converter.toConnectData("test", node.toString().getBytes());

    assert sv.schema().name().equals(SnowflakeJsonSchema.NAME);

    assert sv.value() instanceof JsonNode[];

    JsonNode[] jsonNodes = (JsonNode[]) sv.value();

    assert jsonNodes.length == 1;
    assert node.toString().equals(jsonNodes[0].toString());
  }

  @Test
  public void AvroConverterTest() throws IOException
  {
    //todo: test schema registry
    URL resource = ConverterTest.class.getResource(TEST_FILE_NAME);

    byte[] testFile = Files.readAllBytes(Paths.get(resource.getFile()));

    SnowflakeConverter converter = new SnowflakeAvroConverter();

    SchemaAndValue sv = converter.toConnectData("test", testFile);

    assert sv.schema().name().equals(SnowflakeJsonSchema.NAME);

    assert sv.value() instanceof JsonNode[];

    JsonNode[] jsonNodes = (JsonNode[]) sv.value();

    assert jsonNodes.length == 2;

    assert jsonNodes[0].toString().equals("{\"name\":\"foo\",\"age\":30}");
    assert jsonNodes[1].toString().equals("{\"name\":\"bar\",\"age\":29}");
  }
}
