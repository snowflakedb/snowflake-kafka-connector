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

import com.snowflake.kafka.connector.Utils;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.MDC;

public class KCLoggerTest {
  // test constants
  private final String name = "test.logger.name";

  // mock and test setup, inject logger into KCLogger
  @Mock(name = "logger")
  private Logger logger = Mockito.mock(Logger.class);

  @InjectMocks private KCLogger kcLogger = new KCLogger(this.name);

  @Before
  public void before() {
    this.kcLogger = new KCLogger(this.name);
    MockitoAnnotations.initMocks(this);
  }

  @Test
  public void testAllLogMessages() {
    String msg = "super useful logging msg";
    String expectedMsg = Utils.formatLogMessage(msg);
    String formatMsg = "super {} useful {} logging {} msg {}";
    String expectedFormattedMsg = Utils.formatLogMessage("super wow useful wow! logging 1 msg yay");

    KCLogger.toggleGlobalMdcLoggingContext(false);

    this.testLogMessagesRunner(msg, expectedMsg);
    this.testLogMessagesWithFormattingRunner(
        formatMsg, expectedFormattedMsg, "wow", "wow!", 1, "yay");
  }

  @Test
  public void testAllLogMessagesWithMDCContext() {
    String mdcContext = "[mdc context] ";
    KCLogger.toggleGlobalMdcLoggingContext(true);
    MDC.put(KCLogger.MDC_CONN_CTX_KEY, mdcContext);

    String msg = "super useful logging msg";
    String expectedMsg = Utils.formatLogMessage(mdcContext + msg);
    String formatMsg = "super {} useful {} logging {} msg {}";
    String expectedFormattedMsg =
        Utils.formatLogMessage(mdcContext + "super wow useful wow! logging 1 msg yay");

    this.testLogMessagesRunner(msg, expectedMsg);
    this.testLogMessagesWithFormattingRunner(
        formatMsg, expectedFormattedMsg, "wow", "wow!", 1, "yay");
  }

  private void testLogMessagesRunner(String msg, String expectedMsg) {
    // info
    Mockito.when(logger.isInfoEnabled()).thenReturn(true);
    kcLogger.info(msg);

    Mockito.verify(logger, Mockito.times(1)).info(expectedMsg);

    // trace
    Mockito.when(logger.isTraceEnabled()).thenReturn(true);
    kcLogger.trace(msg);

    Mockito.verify(logger, Mockito.times(1)).trace(expectedMsg);

    // debug
    Mockito.when(logger.isDebugEnabled()).thenReturn(true);
    kcLogger.debug(msg);

    Mockito.verify(logger, Mockito.times(1)).debug(expectedMsg);

    // warn
    Mockito.when(logger.isWarnEnabled()).thenReturn(true);
    kcLogger.warn(msg);

    Mockito.verify(logger, Mockito.times(1)).warn(expectedMsg);

    // error
    Mockito.when(logger.isErrorEnabled()).thenReturn(true);
    kcLogger.error(msg);

    Mockito.verify(logger, Mockito.times(1)).error(expectedMsg);
  }

  private void testLogMessagesWithFormattingRunner(
      String formatMsg, String expectedFormattedMsg, Object... vars) {
    // info
    Mockito.when(logger.isInfoEnabled()).thenReturn(true);
    kcLogger.info(formatMsg, vars);

    Mockito.verify(logger, Mockito.times(1)).info(expectedFormattedMsg);

    // trace
    Mockito.when(logger.isTraceEnabled()).thenReturn(true);
    kcLogger.trace(formatMsg, vars);

    Mockito.verify(logger, Mockito.times(1)).trace(expectedFormattedMsg);

    // debug
    Mockito.when(logger.isDebugEnabled()).thenReturn(true);
    kcLogger.debug(formatMsg, vars);

    Mockito.verify(logger, Mockito.times(1)).debug(expectedFormattedMsg);

    // warn
    Mockito.when(logger.isWarnEnabled()).thenReturn(true);
    kcLogger.warn(formatMsg, vars);

    Mockito.verify(logger, Mockito.times(1)).warn(expectedFormattedMsg);

    // error
    Mockito.when(logger.isErrorEnabled()).thenReturn(true);
    kcLogger.error(formatMsg, vars);

    Mockito.verify(logger, Mockito.times(1)).error(expectedFormattedMsg);
  }
}
