/*
 * Copyright (c) 2023 Snowflake Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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
