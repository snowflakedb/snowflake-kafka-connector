package com.snowflake.kafka.connector.internal.ingestsdk;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.LoggerHandler;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.SFException;
import org.apache.kafka.connect.errors.ConnectException;

/** This is a wrapper to help define a contract with the streaming ingest clients */
public class KcStreamingIngestClient {
  private static final String STREAMING_CLIENT_PREFIX_NAME = "KC_CLIENT_";
  private LoggerHandler LOGGER = new LoggerHandler(this.getClass().getName());

  private final SnowflakeStreamingIngestClient client;

  protected KcStreamingIngestClient(Properties streamingClientProps, String clientName) {
    if (streamingClientProps == null || clientName == null) {
      throw new ConnectException("Given KC streaming client properties or name is null"); // TODO @rcheng: better exception handling here
    }

    try {
      LOGGER.info("Creating Streaming Client: {}", clientName);

      this.client = SnowflakeStreamingIngestClientFactory.builder(clientName)
              .setProperties(streamingClientProps)
              .build();
      assert this.client != null; // client is final, so never need to do another null check
      assert this.client.getName().equals(clientName); // TODO @rcheng: assert handling?
    } catch (SFException ex) {
      throw new ConnectException(ex);
    }
  }

  public SnowflakeStreamingIngestChannel openChannel(String channelName, Map<String, String> config, String tableName) {
    if (this.isClosed()) {
      throw new SFException(null); // TODO @rcheng: error handling
    }

    OpenChannelRequest channelRequest =
            OpenChannelRequest.builder(channelName)
                    .setDBName(config.get(Utils.SF_DATABASE))
                    .setSchemaName(config.get(Utils.SF_SCHEMA))
                    .setTableName(tableName)
                    .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                    .build();
    LOGGER.info("Opening a channel with name:{} for table name:{}", channelName, tableName);

    return this.client.openChannel(channelRequest);
  }

  /**
   * Calls the ingest sdk to close the client sdk
   * Ignores if the client is null or already closed
   * returns t/f if closed
   * TODO @rcheng: we should add retry here and create even in sdk retries bc network issues? esp with rowset api later. bubble up ingest exceptions, retry all others
   */
  public boolean close() {
    if (this.isClosed()) {
      LOGGER.info("Streaming client is already closed or null");
      return true;
    }

    LOGGER.info("Closing Streaming Client:{}", this.client.getName());

    try {
      this.client.close();
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

      // don't throw an exception because closing the client is best effort
      // TODO @rcheng: telemetry?
      LOGGER.error("Failure closing Streaming client msg:{}, cause:{}", message, cause);
      return false;
    }
  }

  // valid client is not null and open
  public boolean isClosed() {
    return this.client != null && !this.client.isClosed();
  }

  public SnowflakeStreamingIngestChannel openChannel(OpenChannelRequest openChannelRequest) {
    return null;
  }

  // client name is the only id we have for clients
  public String getName() {
    return this.client.getName();
  }

  /**
   * Gets the clients name by adding a prefix and client count
   *
   * @param kcInstanceId the indentifier for the connector creating this client
   * @return the streaming ingest client name
   */
  public static String buildStreamingIngestClientName(String kcInstanceId, int clientId) {
    return STREAMING_CLIENT_PREFIX_NAME + kcInstanceId + clientId;
  }

  // clients are equal if same name and same state
  @Override
  public boolean equals(Object o) {
    if (! (o instanceof KcStreamingIngestClient)) {
      return false;
    }

    KcStreamingIngestClient input = (KcStreamingIngestClient) o;
    return this.client.getName().equals(input.getName())
            && this.client.isClosed() && input.isClosed();
  }
}
