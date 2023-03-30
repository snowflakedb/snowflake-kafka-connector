package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.internal.TestUtils;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.snowflake.kafka.connector.records.RecordService;
import com.snowflake.kafka.connector.records.SnowflakeJsonConverter;
import com.snowflake.kafka.connector.records.SnowflakeJsonSchema;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.core.JsonProcessingException;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class SchematizationUtilsTest {
//  @Rule public EnvironmentVariables environmentVariables = new EnvironmentVariables();

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

    Map<String, String> columnToTypes =
            SchematizationUtils.getColumnTypes(
                    recordWithoutSchema, Collections.singletonList(columnName));
    Assert.assertEquals("VARCHAR", columnToTypes.get(columnName));
    // Get non-existing column name should return VARIANT data type
    columnToTypes =
            SchematizationUtils.getColumnTypes(
                    recordWithoutSchema, Collections.singletonList("random"));
    Assert.assertEquals("VARIANT", columnToTypes.get("random"));
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
    String columnName1 = "regionid";
    String columnName2 = "gender";
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

//  private void test() {
//    RecordService service = new RecordService();
//    Map<String, Object> got = service.getProcessedRecordForStreamingIngest(record);
//  }
  @Test
  public void testGetColumnTypesWithoutSchema2() throws JsonProcessingException {
    JsonConverter jsonConverter = new JsonConverter();
    Map<String, ?> config = Collections.singletonMap("schemas.enable", false);
    jsonConverter.configure(config, false);

    String value = "{\"outer_struct\": {\"name\":\"sf\",\"answer\":42}}";
    byte[] valueContents = (value).getBytes(StandardCharsets.UTF_8);

    SchemaAndValue schemaAndValue =
            jsonConverter.toConnectData("topic", valueContents);
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

    assert recordWithoutSchema != null;
//    Map<String, String> columnToTypes =
//            SchematizationUtils.getColumnTypes(
//                    recordWithoutSchema, Collections.singletonList(columnName));
//    Assert.assertEquals("VARCHAR", columnToTypes.get(columnName));
//    // Get non-existing column name should return VARIANT data type
//    columnToTypes =
//            SchematizationUtils.getColumnTypes(
//                    recordWithoutSchema, Collections.singletonList("random"));
//    Assert.assertEquals("VARIANT", columnToTypes.get("random"));
  }
// Integration test inc RecordService getProcessedRecord parsing
//  Test Escaped JSON Document inferance
  @Test
  public void test_IntRS_InferEscapedJSONDocument() {
    String value = "{\"payload\": \"{\\\"version\\\":\\\"0\\\",\\\"id\\\":\\\"44d7498a-f920-40b4-eff5-5ea8bf5579c2\\\",\\\"detail-type\\\":\\\"git_clone\\\",\\\"source\\\":\\\"github\\\",\\\"account\\\":\\\"123\\\",\\\"time\\\":\\\"2023-03-30T08:13:30Z\\\",\\\"region\\\":\\\"eu-west-1\\\",\\\"resources\\\":[],\\\"detail\\\":{\\\"action\\\":\\\"git.clone\\\",\\\"_document_id\\\":\\\"lR1EYOpB0kMt1g==\\\",\\\"actor_location\\\":{\\\"country_code\\\":\\\"IE\\\"},\\\"transport_protocol\\\":1,\\\"transport_protocol_name\\\":\\\"http\\\",\\\"user_agent\\\":\\\"git/2.38.4\\\",\\\"repository\\\":\\\"org/repo\\\",\\\"repo\\\":\\\"org/repo\\\",\\\"repository_public\\\":false,\\\"actor\\\":\\\"devops\\\",\\\"actor_id\\\":123,\\\"org\\\":\\\"org\\\",\\\"org_id\\\":123,\\\"business\\\":\\\"biz\\\",\\\"business_id\\\":123,\\\"user\\\":\\\"\\\",\\\"user_id\\\":0,\\\"hashed_token\\\":\\\"oWsDTN6d35W3HytkCsLnIh2Xv8Vss=\\\",\\\"token_id\\\":123,\\\"@timestamp\\\":1680164004588}}\"}";
    SnowflakeJsonConverter jsonConverter = new SnowflakeJsonConverter();
    byte[] valueContents = (value).getBytes(StandardCharsets.UTF_8);
    SchemaAndValue schemaAndValue =
            jsonConverter.toConnectData("topic", valueContents);

    SinkRecord recordWithoutSchema =
            new SinkRecord(
                    "topic", 0, Schema.STRING_SCHEMA, "string", schemaAndValue.schema(), schemaAndValue.value(), 0);


    RecordService rs = new RecordService();
    rs.setNestDepth(2);
    rs.setEnableSchematization(true);
    try {
      Map<String, Object> processedRecordForStreamingIngest = rs.getProcessedRecordForStreamingIngest(recordWithoutSchema);
      recordWithoutSchema =
              new SinkRecord(
                      "topic", 0, Schema.STRING_SCHEMA, "string", null, processedRecordForStreamingIngest, 0);
      Map<String, String> columnToTypes = SchematizationUtils.getColumnTypes(recordWithoutSchema, Collections.singletonList("PAYLOAD_DETAIL"));
      assert Objects.equals(columnToTypes.get("PAYLOAD_DETAIL"), "VARIANT");
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }


    assert true;
  }
}
