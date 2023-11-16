package com.snowflake.kafka.connector.internal.streaming;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * POJO used to serialize the System function response for migration offset from Source Channel to
 * Destination.
 */
public class ChannelMigrateOffsetTokenResponseDTO {
  private long responseCode;

  private String responseMessage;

  public ChannelMigrateOffsetTokenResponseDTO(long responseCode, String responseMessage) {
    this.responseCode = responseCode;
    this.responseMessage = responseMessage;
  }

  /** Default Ctor for Jackson */
  public ChannelMigrateOffsetTokenResponseDTO() {}

  @JsonProperty("responseCode")
  public long getResponseCode() {
    return responseCode;
  }

  @JsonProperty("responseMessage")
  public String getResponseMessage() {
    return responseMessage;
  }

  @Override
  public String toString() {
    return "ChannelMigrateOffsetTokenResponseDTO{"
        + "responseCode="
        + responseCode
        + ", responseMessage='"
        + responseMessage
        + '\''
        + '}';
  }
}
