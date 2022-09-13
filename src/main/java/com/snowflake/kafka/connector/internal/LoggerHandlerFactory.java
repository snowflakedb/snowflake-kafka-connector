/*
 * Copyright (c) 2019 Snowflake Inc. All rights reserved.
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

import java.util.UUID;

import com.snowflake.kafka.connector.Utils;

public class LoggerHandlerFactory {
  // for logging within the LoggerHandlerFactory
  private final LoggerHandler metaLogger;

  private final UUID loggerCorrelationId;

  public LoggerHandlerFactory() {
    this.loggerCorrelationId = null;
    this.metaLogger = new LoggerHandler(this.getClass().getName());

    metaLogger.info("Created logger handler factory without correlationId. This should be initialized with a " +
      "correlationId before emitting logs");
  }

  public LoggerHandlerFactory(UUID loggerCorrelationId) {
    this.loggerCorrelationId = loggerCorrelationId;
    this.metaLogger = new LoggerHandler(this.loggerCorrelationId, this.getClass().getName());

    metaLogger.info("Created logger handler factory");
  }

  public LoggerHandler getLogger(String name) {
    if (this.loggerCorrelationId == null) {
      metaLogger.error(Utils.formatLogMessage("No correlationId set for loggerHandler in class: '{}'. " +
          "loggerHandlerFactory must be initialized first. Falling back to the default logger without a correlationId",
        name));
      return this.metaLogger;
    }

    return new LoggerHandler(this.loggerCorrelationId, name);
  }
}
