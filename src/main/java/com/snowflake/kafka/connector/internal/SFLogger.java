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

// The purpose of this class is for non-static inheritors to easily log information
public interface SFLogger {
  default LoggerHandler getLogger() {
    return new LoggerHandler(this.getClass().getName());
  }

  default void LOG_TRACE_MSG(String format) {
    getLogger().trace(format);
  }

  default void LOG_INFO_MSG(String format) {
    getLogger().info(format);
  }

  default void LOG_WARN_MSG(String format) {
    getLogger().warn(format);
  }

  default void LOG_DEBUG_MSG(String format) {
    getLogger().debug(format);
  }

  default void LOG_ERROR_MSG(String format) {
    getLogger().error(format);
  }

  default void LOG_TRACE_MSG(String format, Object... vars) {
    getLogger().trace(format, vars);
  }

  default void LOG_INFO_MSG(String format, Object... vars) {
    getLogger().info(format, vars);
  }

  default void LOG_WARN_MSG(String format, Object... vars) {
    getLogger().warn(format, vars);
  }

  default void LOG_DEBUG_MSG(String format, Object... vars) {
    getLogger().debug(format, vars);
  }

  default void LOG_ERROR_MSG(String format, Object... vars) {
    getLogger().error(format, vars);
  }
}
