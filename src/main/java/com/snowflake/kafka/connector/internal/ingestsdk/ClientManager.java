package com.snowflake.kafka.connector.internal.ingestsdk;

import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.internal.LoggerHandler;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.streaming.StreamingUtils;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.connect.errors.ConnectException;

/** This is a wrapper to help manage the streaming ingest clients */
public class ClientManager {
  private static final String STREAMING_CLIENT_PREFIX_NAME = "KC_CLIENT_";

  private LoggerHandler LOGGER;
  private int streamingIngestClientCount;
  private SnowflakeStreamingIngestClient streamingIngestClient;

  protected ClientManager() {
    LOGGER = new LoggerHandler(this.getClass().getName());
    this.streamingIngestClientCount = 0;
  }

  // ONLY FOR TESTING - use this to inject client
  @VisibleForTesting
  public ClientManager(SnowflakeStreamingIngestClient client) {
    this();
    this.streamingIngestClient = client;
  }

  /**
   * Calls the ingest sdk to create the streaming client
   *
   * @param connectorConfig properties for the streaming client
   * @param kcInstanceId identifier of the connector instance creating the client
   */
  public void createStreamingClient(Map<String, String> connectorConfig, String kcInstanceId) {
    Map<String, String> streamingPropertiesMap =
        StreamingUtils.convertConfigForStreamingClient(new HashMap<>(connectorConfig));
    Properties streamingClientProps = new Properties();
    streamingClientProps.putAll(streamingPropertiesMap);

    String streamingIngestClientName = this.getStreamingIngestClientName(kcInstanceId);

    try {
      LOGGER.info("Creating Streaming Client. ClientName:{}", streamingIngestClientName);
      this.streamingIngestClientCount++;
      this.streamingIngestClient =
          SnowflakeStreamingIngestClientFactory.builder(streamingIngestClientName)
              .setProperties(streamingClientProps)
              .build();
    } catch (SFException ex) {
      LOGGER.error(
          "Exception creating streamingIngestClient with name:{}", streamingIngestClientName);
      throw new ConnectException(ex);
    }
  }

  /**
   * Calls the ingest sdk to close the client sdk
   *
   * @return true if the client was successfully closed, false if not
   */
  public boolean closeStreamingClient() {
    if (this.streamingIngestClient == null || this.streamingIngestClient.isClosed()) {
      LOGGER.info("Streaming client is already closed or null");
      return true;
    }

    String streamingIngestClientName = this.streamingIngestClient.getName();
    LOGGER.info("Closing Streaming Client:{}", streamingIngestClientName);

    try {
      this.streamingIngestClient.close();
      return true;
    } catch (Exception e) {
      String message =
          e.getMessage() != null && !e.getMessage().isEmpty()
              ? e.getMessage()
              : "no error message provided";

      String cause =
          e.getCause() != null
                  && e.getCause().getStackTrace() != null
                  && !Arrays.toString(e.getCause().getStackTrace()).isEmpty()
              ? Arrays.toString(e.getCause().getStackTrace())
              : "no cause provided";

      LOGGER.error("Failure closing Streaming client msg:{}, cause:{}", message, cause);
      return false;
    }
  }

  /**
   * Gets the streaming client if it was created
   *
   * @return The streaming client, throws an exception if no client was initialized
   */
  public SnowflakeStreamingIngestClient getStreamingIngestClient() {
    if (this.streamingIngestClient != null && !this.streamingIngestClient.isClosed()) {
      return this.streamingIngestClient;
    }

    LOGGER.error("Streaming ingest client was null or closed. It must be initialized");
    throw SnowflakeErrors.ERROR_3009.getException();
  }

  /**
   * Gets the clients name by adding a prefix and client count
   *
   * @param kcInstanceId the indentifier for the connector creating this client
   * @return the streaming ingest client name
   */
  private String getStreamingIngestClientName(String kcInstanceId) {
    return STREAMING_CLIENT_PREFIX_NAME + kcInstanceId + this.streamingIngestClientCount;
  }
}
