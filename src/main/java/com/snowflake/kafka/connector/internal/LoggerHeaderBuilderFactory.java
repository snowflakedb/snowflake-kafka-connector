/*
 * Copyright (c) 2022 Snowflake Inc. All rights reserved.
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

package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.Utils;
import org.slf4j.Logger;

import java.util.UUID;

/** Creates the header tag for logging */
public class LoggerHeaderBuilderFactory {
  /*
   HEADER FORMATS
                      | with logger tag={tag} and {logId]  | without logger tag
   with kc id={kcid}  | "[KC:{kcId}|{tag}] log message"    | "[KC:{kcId}] log message"
   without kc id      | "[{tag}:{logId}] log message"      | "log message"

   note: logId is a fallback in case there is no kcId
  */
  private static UUID EMPTY_UUID = new UUID(0L, 0L);
  private static Logger logger;
  private static String logInstanceName;

  public static HeaderBuilder builder(Logger inLogger, String inLogName) {
    logger = inLogger;
    logInstanceName = inLogName;
    return new HeaderBuilder();
  }

  public static class HeaderBuilder {
    private String idDescriptor = "";
    private UUID id = EMPTY_UUID;
    private String tag = "";
    private UUID fallbackId = EMPTY_UUID;

    public HeaderBuilder setIdAndDescriptor(UUID id, String idDescriptor) {
      if (this.validateStr(idDescriptor) && this.validateUuid(id)) {
        this.idDescriptor = idDescriptor;
        this.id = id;
      }

      return this;
    }

    public HeaderBuilder setTagAndFallback(String tag, UUID fallbackId) {
      if (this.validateStr(tag)) {
        this.tag = tag;
        this.fallbackId = fallbackId == null ? EMPTY_UUID : fallbackId;
      }

      return this;
    }

    public String build() {
      String header = "";

      if (this.id != EMPTY_UUID && !this.idDescriptor.isEmpty()) {
        header += this.idDescriptor + ":" + this.id.toString();
      }

      if (!this.tag.isEmpty()) {
        if (!header.isEmpty()) {
          header += "|" + this.tag;
        } else if (!this.fallbackId.equals(EMPTY_UUID)) {
          header = this.tag + ":" + this.fallbackId.toString();
        }
      }

      if (header.isEmpty()) {
        return header;
      }

      logger.info(
          Utils.formatLogMessage("Setting {} instance id to '{}'", logInstanceName, header));

      return "[" + header + "]";
    }

    private boolean validateUuid(UUID id) {
      boolean isValid = id != null && !id.toString().isEmpty() && !id.equals(EMPTY_UUID);
      if (!isValid) {
        logger.warn(
            Utils.formatLogMessage(
                "Given {} instance id was invalid (null or empty), continuing to log without"
                    + " it"),
            logInstanceName);
      }

      return isValid;
    }

    private boolean validateStr(String str) {
      boolean isValid = str != null && !str.isEmpty();
      if (isValid) {
        if (str.length() > 50) {
          logger.info(
            Utils.formatLogMessage(
              "Given {} instance id descriptor '{}' is recommended to be below 50 characters",
              logInstanceName,
              idDescriptor));
        }

        return isValid;
      }

      logger.warn(
        Utils.formatLogMessage(
          "Descriptor given for {} instance id was invalid (null or empty), continuing to log"
            + " without it"),
        logInstanceName);
      return isValid;
    }
  }
}
