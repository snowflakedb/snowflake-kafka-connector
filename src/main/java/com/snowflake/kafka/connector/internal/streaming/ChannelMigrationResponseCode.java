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

import com.google.common.annotations.VisibleForTesting;

/**
 * Response code sent from the system function to migrate offsets from Source to Destination
 * Channel. Please keep this code(values) in sync with what the server side is assigned.
 */
public enum ChannelMigrationResponseCode {
  ERR_TABLE_DOES_NOT_EXIST_NOT_AUTHORIZED(
      4, "The supplied table does not exist or is not authorized"),

  SUCCESS(50, "Success"),

  OFFSET_MIGRATION_SOURCE_CHANNEL_DOES_NOT_EXIST(
      51, "Source Channel does not exist for Offset Migration"),

  CHANNEL_OFFSET_TOKEN_MIGRATION_GENERAL_EXCEPTION(
      52, "Snowflake experienced a transient exception, please retry the migration request."),

  OFFSET_MIGRATION_SOURCE_AND_DESTINATION_CHANNEL_SAME(
      53, "Source and Destination Channel are same for Migration Offset Request"),
  ;

  private final long statusCode;

  private final String message;

  public static final String UNKNOWN_STATUS_MESSAGE =
      "Unknown status message. Please contact Snowflake support for further assistance";

  ChannelMigrationResponseCode(int statusCode, String message) {
    this.statusCode = statusCode;
    this.message = message;
  }

  @VisibleForTesting
  public long getStatusCode() {
    return statusCode;
  }

  @VisibleForTesting
  public String getMessage() {
    return message;
  }

  public static String getMessageByCode(Long statusCode) {
    if (statusCode != null) {
      for (ChannelMigrationResponseCode code : values()) {
        if (code.statusCode == statusCode) {
          return code.message;
        }
      }
    }
    return UNKNOWN_STATUS_MESSAGE;
  }

  /**
   * Given a response code which was received from server side, check if it as successful migration
   * or a failure.
   *
   * @param statusCodeToCheck response from JDBC system function call.
   * @return true or false
   */
  private static boolean isStatusCodeSuccessful(final long statusCodeToCheck) {
    return statusCodeToCheck == SUCCESS.getStatusCode()
        || statusCodeToCheck == OFFSET_MIGRATION_SOURCE_CHANNEL_DOES_NOT_EXIST.getStatusCode();
  }

  /**
   * Given a Response DTO, which was received from server side and serialized, check if it as
   * successful migration or a failure.
   *
   * @param channelMigrateOffsetTokenResponseDTO response from JDBC system function call serialized
   *     into a DTO object.
   * @return true or false
   */
  public static boolean isChannelMigrationResponseSuccessful(
      final ChannelMigrateOffsetTokenResponseDTO channelMigrateOffsetTokenResponseDTO) {
    return isStatusCodeSuccessful(channelMigrateOffsetTokenResponseDTO.getResponseCode());
  }
}
