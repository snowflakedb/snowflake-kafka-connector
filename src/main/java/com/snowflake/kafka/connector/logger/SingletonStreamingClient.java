package com.snowflake.kafka.connector.logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.streaming.StreamingUtils;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class SingletonStreamingClient {

  // Static variable reference of single_instance
  // of type Singleton
  private static SnowflakeStreamingIngestClient single_instance = null;

  private static SnowflakeStreamingIngestChannel channel = null;

  private static Map<String, String> propMap = null;

  private static String PROFILE_PATH = "profile_streaming_prod1.json";

  private static final ObjectMapper mapper = new ObjectMapper();


  // Constructor
  // Here we will be creating private constructor
  // restricted to this class itself
  private SingletonStreamingClient()
  {

  }

  // Static method
  // Static method to create instance of Singleton class
  private static SnowflakeStreamingIngestClient getInstance() throws IOException {
    if (single_instance == null) {
      single_instance = SnowflakeStreamingIngestClientFactory.builder("KC_LOGGER")
              .setProperties(getProps())
              .build();
    }
    return single_instance;
  }

  public static SnowflakeStreamingIngestChannel getChannel() throws IOException {
    SnowflakeStreamingIngestClient client = getInstance();
    if (channel == null) {
      OpenChannelRequest channelRequest =
              OpenChannelRequest.builder("LOGGER_CHANNEL")
                      .setDBName(propMap.get(Utils.SF_DATABASE))
                      .setSchemaName(propMap.get(Utils.SF_SCHEMA))
                      .setTableName("KC_TABLE_LOGGER")
                      .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                      .build();

      channel = client.openChannel(channelRequest);
    }
    return channel;
  }

  private static Properties getProps() throws IOException {
    Map<String, String> localMap = new HashMap<>();
    Iterator<Map.Entry<String, JsonNode>> propIt =
            mapper.readTree(new String(Files.readAllBytes(Paths.get(PROFILE_PATH)))).fields();
    while (propIt.hasNext()) {
      Map.Entry<String, JsonNode> prop = propIt.next();
      localMap.put(prop.getKey(), prop.getValue().asText());
    }

    if (propMap == null) {
      propMap = localMap;
    }

    Map<String, String> streamingPropertiesMap =
            StreamingUtils.convertConfigForStreamingClient(new HashMap<>(localMap));
    Properties streamingClientProps = new Properties();
    streamingClientProps.putAll(streamingPropertiesMap);
    return streamingClientProps;
  }
}
