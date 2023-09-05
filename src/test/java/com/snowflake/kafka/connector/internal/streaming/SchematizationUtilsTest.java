package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonProcessingException;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class SchematizationUtilsTest {
  @Rule public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Test
  public void testFormatNames() {
    String[] inputList = new String[] {"role", "Role", "\"role\"", "\"rOle\""};
    String[] expectedOutputList = new String[] {"ROLE", "ROLE", "role", "rOle"};
    for (int idx = 0; idx < inputList.length; idx++) {
      Assert.assertEquals(expectedOutputList[idx], SchematizationUtils.formatName(inputList[idx]));
    }
  }

  @Test
  public void testGetColumnTypesWithoutSchema() throws JsonProcessingException {
    String columnName = "test";
    String nonExistingColumnName = "random";
    ObjectMapper mapper = new ObjectMapper();
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, ?> config = Collections.singletonMap("schemas.enable", false);
    jsonConverter.configure(config, false);
    Map<String, String> jsonMap = new HashMap<>();
    jsonMap.put(columnName, "value");
    SchemaAndValue schemaAndValue =
        jsonConverter.toConnectData("topic", mapper.writeValueAsBytes(jsonMap));
    SinkRecord recordWithoutSchema =
        new SinkRecord(
            "topic",
            0,
            null,
            null,
            schemaAndValue.schema(),
            schemaAndValue.value(),
            0,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME);

    String processedColumnName = Utils.quoteNameIfNeeded(columnName);
    String processedNonExistingColumnName = Utils.quoteNameIfNeeded(nonExistingColumnName);
    Map<String, String> columnToTypes =
        SchematizationUtils.getColumnTypes(
            recordWithoutSchema, Collections.singletonList(processedColumnName));
    Assert.assertEquals("VARCHAR", columnToTypes.get(processedColumnName));
    // Get non-existing column name should return nothing
    columnToTypes =
        SchematizationUtils.getColumnTypes(
            recordWithoutSchema, Collections.singletonList(processedNonExistingColumnName));
    Assert.assertTrue(columnToTypes.isEmpty());
  }

  @Test
  public void testGetColumnTypesWithSchema() throws JsonProcessingException {
    JsonConverter converter = new JsonConverter();
    Map<String, String> converterConfig = new HashMap<>();
    converterConfig.put("schemas.enable", "true");
    converter.configure(converterConfig, false);
    SchemaAndValue schemaAndValue =
        converter.toConnectData(
            "topic", TestUtils.JSON_WITH_SCHEMA.getBytes(StandardCharsets.UTF_8));
    String columnName1 = Utils.quoteNameIfNeeded("regionid");
    String columnName2 = Utils.quoteNameIfNeeded("gender");
    SinkRecord recordWithoutSchema =
        new SinkRecord(
            "topic",
            0,
            null,
            null,
            schemaAndValue.schema(),
            schemaAndValue.value(),
            0,
            System.currentTimeMillis(),
            TimestampType.CREATE_TIME);

    Map<String, String> columnToTypes =
        SchematizationUtils.getColumnTypes(
            recordWithoutSchema, Arrays.asList(columnName1, columnName2));
    Assert.assertEquals("VARCHAR", columnToTypes.get(columnName1));
    Assert.assertEquals("VARCHAR", columnToTypes.get(columnName2));
  }
}
