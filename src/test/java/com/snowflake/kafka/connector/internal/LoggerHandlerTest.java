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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

import java.util.UUID;

public class LoggerHandlerTest {
  // test constants
  private final String name = "test.logger.name";
  private final UUID kcGlobalInstanceId = UUID.randomUUID();
  private final UUID loggerInstanceId = UUID.randomUUID();

  // mock and test setup, inject logger into loggerHandler
  @Mock(name = "logger")
  private Logger logger = Mockito.mock(Logger.class);

  @InjectMocks private LoggerHandler loggerHandler = new LoggerHandler(this.name);

  @Before
  public void initMocks() {
    MockitoAnnotations.initMocks(this);
  }

  @After
  public void close() {
    LoggerHandler.setConnectGlobalInstanceId(null);
    this.loggerHandler = new LoggerHandler(this.name);
  }

  // test
  @Test
  public void testAllLogMessageNoInstanceIds() {
    String msg = "super useful logging msg";

    testAllLogMessagesRunner("");
  }

  @Test
  public void testAllLogMessageKcGlobalInstanceId() {
    String msg = "super useful logging msg";
    String formatMsg = "super {} useful {} logging {} msg {}";
    String expectedFormattedMsg = "super wow useful wow! logging 1 msg yay";

    LoggerHandler.setConnectGlobalInstanceId(this.kcGlobalInstanceId);
    MockitoAnnotations.initMocks(this);

    // [kc:id]
    testAllLogMessagesRunner("[KC:" + kcGlobalInstanceId + "] ");
  }

  @Test
  public void testAllLogMessageLoggingInstanceId() {
    String logTag = "TEST";
    String msg = "super useful logging msg";

    this.loggerHandler = new LoggerHandler(this.name);
    this.loggerHandler.setLoggerInstanceIdTag(logTag, loggerInstanceId);
    MockitoAnnotations.initMocks(this);

    // [logtag:id]
    testAllLogMessagesRunner(Utils.formatString("[{}:{}] ", logTag, loggerInstanceId));

    this.loggerHandler.clearLoggerInstanceIdTag();
    testAllLogMessagesRunner("");
  }

  @Test
  public void testAllLogMessageAllInstanceIds() {
    String logTag = "TEST";
    String msg = "super useful logging msg";

    LoggerHandler.setConnectGlobalInstanceId(this.kcGlobalInstanceId);
    loggerHandler = new LoggerHandler(name);
    this.loggerHandler.setLoggerInstanceIdTag(logTag, loggerInstanceId);
    MockitoAnnotations.initMocks(this);

    // [kc:id|tag]
    testAllLogMessagesRunner(Utils.formatString("[KC:{}|{}] ", kcGlobalInstanceId, logTag) );
  }

  @Test
  public void testInvalidKcId() {
    String msg = "super useful logging msg";

    LoggerHandler.setConnectGlobalInstanceId(null);
    MockitoAnnotations.initMocks(this);
    Mockito.when(logger.isInfoEnabled()).thenReturn(true);

    this.loggerHandler.info(msg);

    Mockito.verify(logger, Mockito.times(1)).info(Utils.formatLogMessage(msg));
  }

  @Test
  public void testInvalidLogTag() {
    String msg = "super useful logging msg";

    this.loggerHandler.setLoggerInstanceIdTag(null, this.loggerInstanceId);
    MockitoAnnotations.initMocks(this);
    Mockito.when(logger.isInfoEnabled()).thenReturn(true);

    this.loggerHandler.info(msg);

    Mockito.verify(logger, Mockito.times(1)).info(Utils.formatLogMessage(msg));
  }

  @Test
  public void testInvalidLogUuid() {
    String msg = "super useful logging msg";

    this.loggerHandler.setLoggerInstanceIdTag("TEST", null);
    MockitoAnnotations.initMocks(this);
    Mockito.when(logger.isInfoEnabled()).thenReturn(true);

    this.loggerHandler.info(msg);

    Mockito.verify(logger, Mockito.times(1)).info(Utils.formatLogMessage(msg));
  }

  private void testAllLogMessagesRunner(String expectedTag) {
    String msg = "super useful logging msg";
    String formatMsg = "super {} useful {} logging {} msg {}";
    String expectedFormattedMsg = "super wow useful wow! logging 1 msg yay";

    this.testLogMessagesRunner(msg, Utils.formatLogMessage(expectedTag + msg));
    this.testLogMessagesWithFormattingRunner(formatMsg, Utils.formatLogMessage(expectedTag + expectedFormattedMsg),
      "wow", "wow!", 1, "yay");
  }

  private void testLogMessagesRunner(String msg, String expectedMsg) {
    // info
    Mockito.when(logger.isInfoEnabled()).thenReturn(true);
    loggerHandler.info(msg);

    Mockito.verify(logger, Mockito.times(1)).info(expectedMsg);

    // trace
    Mockito.when(logger.isTraceEnabled()).thenReturn(true);
    loggerHandler.trace(msg);

    Mockito.verify(logger, Mockito.times(1)).trace(expectedMsg);

    // debug
    Mockito.when(logger.isDebugEnabled()).thenReturn(true);
    loggerHandler.debug(msg);

    Mockito.verify(logger, Mockito.times(1)).debug(expectedMsg);

    // warn
    Mockito.when(logger.isWarnEnabled()).thenReturn(true);
    loggerHandler.warn(msg);

    Mockito.verify(logger, Mockito.times(1)).warn(expectedMsg);

    // error
    Mockito.when(logger.isErrorEnabled()).thenReturn(true);
    loggerHandler.error(msg);

    Mockito.verify(logger, Mockito.times(1)).error(expectedMsg);
  }

  private void testLogMessagesWithFormattingRunner(
      String formatMsg, String expectedFormattedMsg, Object... vars) {
    // info
    Mockito.when(logger.isInfoEnabled()).thenReturn(true);
    loggerHandler.info(formatMsg, vars);

    Mockito.verify(logger, Mockito.times(1)).info(expectedFormattedMsg);

    // trace
    Mockito.when(logger.isTraceEnabled()).thenReturn(true);
    loggerHandler.trace(formatMsg, vars);

    Mockito.verify(logger, Mockito.times(1)).trace(expectedFormattedMsg);

    // debug
    Mockito.when(logger.isDebugEnabled()).thenReturn(true);
    loggerHandler.debug(formatMsg, vars);

    Mockito.verify(logger, Mockito.times(1)).debug(expectedFormattedMsg);

    // warn
    Mockito.when(logger.isWarnEnabled()).thenReturn(true);
    loggerHandler.warn(formatMsg, vars);

    Mockito.verify(logger, Mockito.times(1)).warn(expectedFormattedMsg);

    // error
    Mockito.when(logger.isErrorEnabled()).thenReturn(true);
    loggerHandler.error(formatMsg, vars);

    Mockito.verify(logger, Mockito.times(1)).error(expectedFormattedMsg);
  }
}
