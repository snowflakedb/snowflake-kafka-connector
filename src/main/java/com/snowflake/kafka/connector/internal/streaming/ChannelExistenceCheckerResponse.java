package com.snowflake.kafka.connector.internal.streaming;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Class used to serialize a response for the channels status endpoint. Please keep this upto date
 * with {@link net.snowflake.ingest.streaming.internal.ChannelsStatusResponse}
 */
public class ChannelExistenceCheckerResponse {
  static class ChannelStatusResponseDTO {

    private Long statusCode;

    // Latest persisted offset token
    private String persistedOffsetToken;

    // Latest persisted client sequencer
    private Long persistedClientSequencer;

    // Latest persisted row sequencer
    private Long persistedRowSequencer;

    @JsonProperty("status_code")
    Long getStatusCode() {
      return statusCode;
    }

    @JsonProperty("status_code")
    void setStatusCode(Long statusCode) {
      this.statusCode = statusCode;
    }

    @JsonProperty("persisted_row_sequencer")
    Long getPersistedRowSequencer() {
      return persistedRowSequencer;
    }

    @JsonProperty("persisted_row_sequencer")
    void setPersistedRowSequencer(Long persistedRowSequencer) {
      this.persistedRowSequencer = persistedRowSequencer;
    }

    @JsonProperty("persisted_client_sequencer")
    Long getPersistedClientSequencer() {
      return persistedClientSequencer;
    }

    @JsonProperty("persisted_client_sequencer")
    void setPersistedClientSequencer(Long persistedClientSequencer) {
      this.persistedClientSequencer = persistedClientSequencer;
    }

    @JsonProperty("persisted_offset_token")
    String getPersistedOffsetToken() {
      return persistedOffsetToken;
    }

    @JsonProperty("persisted_offset_token")
    void setPersistedOffsetToken(String persistedOffsetToken) {
      this.persistedOffsetToken = persistedOffsetToken;
    }
  }

  // Channel array to return
  private List<ChannelStatusResponseDTO> channels;
  private Long statusCode;
  private String message;

  @JsonProperty("status_code")
  void setStatusCode(Long statusCode) {
    this.statusCode = statusCode;
  }

  @JsonProperty("status_code")
  Long getStatusCode() {
    return this.statusCode;
  }

  @JsonProperty("message")
  void setMessage(String message) {
    this.message = message;
  }

  @JsonProperty("message")
  String getMessage() {
    return this.message;
  }

  @JsonProperty("channels")
  void setChannels(List<ChannelStatusResponseDTO> channels) {
    this.channels = channels;
  }

  @JsonProperty("channels")
  List<ChannelStatusResponseDTO> getChannels() {
    return channels;
  }
}
