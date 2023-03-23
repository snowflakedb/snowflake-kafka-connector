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

package com.snowflake.kafka.connector.internal;

/** This class provides an easy way for non-static inheritors to log information. */
public class SFLogger {
  private final LoggerHandler logger;

  /**
   * Constructs an SFLogger instance for the given class.
   *
   * @param clazz the class for which the logger is intended
   */
  public SFLogger(Class<?> clazz) {
    this.logger = new LoggerHandler(clazz.getName());
  }

  /**
   * Logs an info-level message.
   *
   * @param format the message format
   */
  public void LOG_INFO(String format) {
    logger.info(format);
  }

  /**
   * Logs a warning-level message.
   *
   * @param format the message format
   */
  public void LOG_WARN(String format) {
    logger.warn(format);
  }

  /**
   * Logs a debug-level message.
   *
   * @param format the message format
   */
  public void LOG_DEBUG(String format) {
    logger.debug(format);
  }

  /**
   * Logs an error-level message.
   *
   * @param format the message format
   */
  public void LOG_ERROR(String format) {
    logger.error(format);
  }

  /**
   * Logs an info-level message with additional variables.
   *
   * @param format the message format
   * @param vars the variables to be included in the message
   */
  public void LOG_INFO(String format, Object... vars) {
    logger.info(format, vars);
  }

  /**
   * Logs a warning-level message with additional variables.
   *
   * @param format the message format
   * @param vars the variables to be included in the message
   */
  public void LOG_WARN(String format, Object... vars) {
    logger.warn(format, vars);
  }

  /**
   * Logs a debug-level message with additional variables.
   *
   * @param format the message format
   * @param vars the variables to be included in the message
   */
  public void LOG_DEBUG(String format, Object... vars) {
    logger.debug(format, vars);
  }

  /**
   * Logs an error-level message with additional variables.
   *
   * @param format the message format
   * @param vars the variables to be included in the message
   */
  public void LOG_ERROR(String format, Object... vars) {
    logger.error(format, vars);
  }
}
