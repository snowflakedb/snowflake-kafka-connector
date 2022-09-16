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

// The purpose of this class is for non-static inheritors to easily log information
public abstract class EnableLogging {
  private final LoggerHandler logger = new LoggerHandler(this.getClass().getName());

  protected void LOG_TRACE_MSG(String format) {
    this.logger.trace(format);
  }

  protected void LOG_INFO_MSG(String format) {
    this.logger.info(format);
  }

  protected void LOG_WARN_MSG(String format) {
    this.logger.warn(format);
  }

  protected void LOG_DEBUG_MSG(String format) {
    this.logger.debug(format);
  }

  protected void LOG_ERROR_MSG(String format) {
    this.logger.error(format);
  }

  protected void LOG_TRACE_MSG(String format, Object... vars) {
    this.logger.trace(format, vars);
  }

  protected void LOG_INFO_MSG(String format, Object... vars) {
    this.logger.info(format, vars);
  }

  protected void LOG_WARN_MSG(String format, Object... vars) {
    this.logger.warn(format, vars);
  }

  protected void LOG_DEBUG_MSG(String format, Object... vars) {
    this.logger.debug(format, vars);
  }

  protected void LOG_ERROR_MSG(String format, Object... vars) {
    this.logger.error(format, vars);
  }
}
