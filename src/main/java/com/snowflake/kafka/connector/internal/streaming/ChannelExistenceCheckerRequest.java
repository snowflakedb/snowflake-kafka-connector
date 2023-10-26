package com.snowflake.kafka.connector.internal.streaming;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** Class to deserialize a request from a channel status request */
class ChannelExistenceCheckerRequest {

  // Used to deserialize a channel request
  static class ChannelStatusRequestDTO {
    // Database name
    private final String databaseName;

    // Schema name
    private final String schemaName;

    // Table Name
    private final String tableName;

    // Channel Name
    private final String channelName;

    // Client Sequencer
    private final Long clientSequencer;

    public ChannelStatusRequestDTO(
        String databaseName,
        String schemaName,
        String tableName,
        String channelName,
        Long clientSequencer) {
      this.databaseName = databaseName;
      this.schemaName = schemaName;
      this.tableName = tableName;
      this.channelName = channelName;
      this.clientSequencer = clientSequencer;
    }

    @JsonProperty("table")
    String getTableName() {
      return tableName;
    }

    @JsonProperty("database")
    String getDatabaseName() {
      return databaseName;
    }

    @JsonProperty("schema")
    String getSchemaName() {
      return schemaName;
    }

    @JsonProperty("channel_name")
    String getChannelName() {
      return channelName;
    }

    @JsonProperty("client_sequencer")
    Long getClientSequencer() {
      return clientSequencer;
    }
  }

  // Optional Request ID. Used for diagnostic purposes.
  private String requestId;

  // Channels in request
  private List<ChannelStatusRequestDTO> channels;

  // Snowflake role used by client
  private String role;

  @JsonProperty("request_id")
  String getRequestId() {
    return requestId;
  }

  @JsonProperty("role")
  public String getRole() {
    return role;
  }

  @JsonProperty("role")
  public void setRole(String role) {
    this.role = role;
  }

  @JsonProperty("request_id")
  void setRequestId(String requestId) {
    this.requestId = requestId;
  }

  @JsonProperty("channels")
  void setChannels(List<ChannelStatusRequestDTO> channels) {
    this.channels = channels;
  }

  @JsonProperty("channels")
  List<ChannelStatusRequestDTO> getChannels() {
    return channels;
  }
}
