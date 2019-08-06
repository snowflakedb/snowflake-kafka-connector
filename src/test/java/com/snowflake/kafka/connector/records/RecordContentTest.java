package com.snowflake.kafka.connector.records;

import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.TestUtils;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class RecordContentTest
{
  private ObjectMapper mapper = new ObjectMapper();

  @Test
  public void test() throws IOException
  {
    JsonNode data = mapper.readTree("{\"name\":123}");
    //json
    SnowflakeRecordContent content =
      new SnowflakeRecordContent(data);
    assert !content.isBroken();
    assert content.getSchemaID() == -1;
    assert content.getData().length == 1;
    assert content.getData()[0].asText().equals(data.asText());
    assert TestUtils.assertError(SnowflakeErrors.ERROR_5011,
      content::getBrokenData);

    //avro
    int schemaID = 123;
    content = new SnowflakeRecordContent(data, schemaID);
    assert !content.isBroken();
    assert content.getSchemaID() == schemaID;
    assert content.getData().length == 1;
    assert content.getData()[0].asText().equals(data.asText());
    assert TestUtils.assertError(SnowflakeErrors.ERROR_5011,
      content::getBrokenData);

    //avro without schema registry
    JsonNode[] data1 = new JsonNode[1];
    data1[0] = data;
    content = new SnowflakeRecordContent(data1);
    assert !content.isBroken();
    assert content.getSchemaID() == -1;
    assert content.getData().length == 1;
    assert content.getData()[0].asText().equals(data.asText());
    assert TestUtils.assertError(SnowflakeErrors.ERROR_5011,
      content::getBrokenData);

    //broken record
    byte[] brokenData = "123".getBytes(StandardCharsets.UTF_8);
    content = new SnowflakeRecordContent(brokenData);
    assert content.isBroken();
    assert content.getSchemaID() == -1;
    assert TestUtils.assertError(SnowflakeErrors.ERROR_5012,
      content::getData);
    assert new String(content.getBrokenData(), StandardCharsets.UTF_8).equals("123");
  }
}
