package com.snowflake.kafka.connector.streaming.iceberg;

import static com.snowflake.kafka.connector.internal.TestUtils.PROFILE_PATH;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.internal.streaming.SnowflakeSinkServiceV2;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class IcebergIngestionIT {
  private static final String tableName = "tbl1";
  private static final int PARTITION = 0;
  private static final String topic = tableName;
  private static final String testChannelName =
      SnowflakeSinkServiceV2.partitionChannelKey(topic, PARTITION);

  @Test
  @Disabled
  void shouldInsertValue() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    Properties props = new Properties();
    Iterator<Map.Entry<String, JsonNode>> propIt =
        mapper.readTree(new String(Files.readAllBytes(Paths.get(PROFILE_PATH)))).fields();
    while (propIt.hasNext()) {
      Map.Entry<String, JsonNode> prop = propIt.next();
      props.put(prop.getKey(), prop.getValue().asText());
    }
    SnowflakeStreamingIngestClient client =
        SnowflakeStreamingIngestClientFactory.builder("name")
            .setProperties(props)
            // .setIsIceberg(true)
            .build();

    OpenChannelRequest channelRequest =
        OpenChannelRequest.builder(testChannelName)
            .setDBName("TESTDB")
            .setSchemaName("TESTSCHEMA")
            .setTableName(tableName)
            .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
            .build();

    // Open a channel with same name will bump up the client sequencer number for this channel
    SnowflakeStreamingIngestChannel channel = client.openChannel(channelRequest);
    HashMap<String, Object> row = new HashMap<>();
    row.put("c1", 666);
    row.put("c2", "test");
    channel.insertRow(row, "0");
    channel.close().get();
    client.close();
  }
}
