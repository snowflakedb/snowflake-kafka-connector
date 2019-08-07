package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.records.SnowflakeConverter;
import com.snowflake.kafka.connector.records.SnowflakeJsonConverter;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MetaColumnIT
{
  private String topic = "test";
  private int partition = 0;
  private String tableName = TestUtils.randomTableName();
  private String stageName = Utils.stageName(TestUtils.TEST_CONNECTOR_NAME,
    tableName);
  private String pipeName = Utils.stageName(TestUtils.TEST_CONNECTOR_NAME,
    tableName);
  private SnowflakeConnectionService conn = TestUtils.getConnectionService();
  private ObjectMapper mapper = new ObjectMapper();

  @After
  public void afterEach()
  {
    conn.dropStage(stageName);
    TestUtils.dropTable(tableName);
    conn.dropPipe(pipeName);
  }

  @Test
  public void testKey() throws InterruptedException, SQLException, IOException
  {
    conn.createTable(tableName);
    conn.createStage(stageName);


    SnowflakeSinkService service =
      SnowflakeSinkServiceFactory
        .builder(conn)
        .addTask(tableName, topic, partition)
        .setRecordNumber(3)
        .build();

    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue result = converter.toConnectData(topic, ("{\"name\":\"test\"}").getBytes(StandardCharsets.UTF_8));
    SinkRecord record =
      new SinkRecord(topic, partition, Schema.STRING_SCHEMA, "key1",
        result.schema(), result.value(), 0);

    service.insert(record);

    record =
      new SinkRecord(topic, partition, Schema.BYTES_SCHEMA, "key2",
        result.schema(), result.value(), 1);

    service.insert(record);

    record =
      new SinkRecord(topic, partition, Schema.STRING_SCHEMA, "key3",
        result.schema(), result.value(), 2);

    service.insert(record);

    Thread.sleep(90 * 1000);
    service.close();

    ResultSet resultSet = TestUtils.executeQuery("select RECORD_METADATA from" +
      " " + tableName);

    boolean hasKey1 = false;
    boolean hasKey3 = false;

    for (int i = 0; i < 3; i++)
    {
      assert resultSet.next();
      JsonNode node = mapper.readTree(resultSet.getString(1));
      if (node.has("key"))
      {
        if (node.get("key").asText().equals("key1"))
        {
          hasKey1 = true;
        }
        else if (node.get("key").asText().equals("key3"))
        {
          hasKey3 = true;
        }
      }
    }
    assert !resultSet.next();
    assert hasKey1 && hasKey3;
  }

}
