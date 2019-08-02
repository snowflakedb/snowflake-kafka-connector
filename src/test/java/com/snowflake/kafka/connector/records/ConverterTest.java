/*
 * Copyright (c) 2019 Snowflake Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.snowflake.kafka.connector.records;

import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind
    .ObjectMapper;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node
    .ObjectNode;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;

public class ConverterTest
{

  ObjectMapper mapper = new ObjectMapper();
  private static String TEST_FILE_NAME = "test.avro";

  @Test
  public void testJsonConverter()
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
  public void testAvroConverter() throws IOException
  {
    //todo: test schema registry
    URL resource = ConverterTest.class.getResource(TEST_FILE_NAME);

    byte[] testFile = Files.readAllBytes(Paths.get(resource.getFile()));

    SnowflakeConverter converter = new SnowflakeAvroConverterWithoutSchemaRegistry();

    SchemaAndValue sv = converter.toConnectData("test", testFile);

    assert sv.schema().name().equals(SnowflakeJsonSchema.NAME);

    assert sv.value() instanceof JsonNode[];

    JsonNode[] jsonNodes = (JsonNode[]) sv.value();

    assert jsonNodes.length == 2;

    assert jsonNodes[0].toString().equals("{\"name\":\"foo\",\"age\":30}");
    assert jsonNodes[1].toString().equals("{\"name\":\"bar\",\"age\":29}");
  }

  @Test
  public void testBrokenRecord()
  {
    byte[] data = "fasfas".getBytes();
    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue result = converter.toConnectData("test", data);
    assert result.schema().name().equals(SnowflakeBrokenRecordSchema.NAME);
    assert Arrays.equals(data, (byte[]) result.value());

    converter = new SnowflakeAvroConverter();
    result = converter.toConnectData("test", data);
    assert result.schema().name().equals(SnowflakeBrokenRecordSchema.NAME);
    assert Arrays.equals(data, (byte[]) result.value());

    converter = new SnowflakeAvroConverterWithoutSchemaRegistry();
    result = converter.toConnectData("test", data);
    assert result.schema().name().equals(SnowflakeBrokenRecordSchema.NAME);
    assert Arrays.equals(data, (byte[]) result.value());
  }
}
