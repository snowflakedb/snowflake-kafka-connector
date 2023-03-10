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
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;

public class LoggerHandlerTest {
  // test constants
  private final String name = "test.logger.name";
  private final String kcGlobalInstanceId = "[KC:testid123]";
  private final String taskLoggerTag = "[TASK:testid123.123]";

  // mock and test setup, inject logger into loggerHandler
  @Mock(name = "logger")
  private Logger logger = Mockito.mock(Logger.class);

  @InjectMocks private LoggerHandler loggerHandler = new LoggerHandler(this.name);

  @After
  public void close() {
    LoggerHandler.setKcGlobalInstanceId("");
    this.loggerHandler = new LoggerHandler(this.name);
  }

  // test
  @Test
  public void testAllLogMessageNoInstanceIds() {
    MockitoAnnotations.initMocks(this);

    testAllLogMessagesRunner("");
  }

  @Test
  public void testAllLogMessageKcGlobalInstanceId() {
    LoggerHandler.setKcGlobalInstanceId(this.kcGlobalInstanceId);
    MockitoAnnotations.initMocks(this);

    // [kc:id] with space at end
    testAllLogMessagesRunner(this.kcGlobalInstanceId + " ");
  }

  @Test
  public void testAllLogMessageLoggingTag() {
    this.loggerHandler = new LoggerHandler(this.name);
    this.loggerHandler.setLoggerInstanceTag(this.taskLoggerTag);
    MockitoAnnotations.initMocks(this);

    // [task:id.creationtime] with space at end
    testAllLogMessagesRunner(this.taskLoggerTag + " ");

    this.loggerHandler.clearLoggerInstanceIdTag();
    testAllLogMessagesRunner("");
  }

  @Test
  public void testAllLogMessageAllInstanceIds() {
    LoggerHandler.setKcGlobalInstanceId(this.kcGlobalInstanceId);
    loggerHandler = new LoggerHandler(name);
    this.loggerHandler.setLoggerInstanceTag(this.taskLoggerTag);
    MockitoAnnotations.initMocks(this);

    // [kc:id] [task:id.creationtime] with space at end
    testAllLogMessagesRunner(this.kcGlobalInstanceId + " " + this.taskLoggerTag + " ");
  }

  @Test
  public void testInvalidKcId() {
    String msg = "super useful logging msg";

    LoggerHandler.setKcGlobalInstanceId("");
    MockitoAnnotations.initMocks(this);
    Mockito.when(logger.isInfoEnabled()).thenReturn(true);

    this.loggerHandler.info(msg);

    Mockito.verify(logger, Mockito.times(1)).info(Utils.formatLogMessage(msg));
  }

  @Test
  public void testInvalidLogTag() {
    String msg = "super useful logging msg";

    this.loggerHandler.setLoggerInstanceTag(null);
    MockitoAnnotations.initMocks(this);
    Mockito.when(logger.isInfoEnabled()).thenReturn(true);

    this.loggerHandler.info(msg);

    Mockito.verify(logger, Mockito.times(1)).info(Utils.formatLogMessage(msg));
  }

  @Test
  public void testGetFormattedKcGlobalInstanceId() {
    long startTime = 123;
    String expectedId = "[KC:" + Math.abs(("" + 123).hashCode()) + "]";
    String gotId = LoggerHandler.getFormattedKcGlobalInstanceId(startTime);
    assert expectedId.equals(gotId);
  }

  @Test
  public void testGetFormattedTaskLoggingTag() {
    String taskId = "taskid";
    long startTime = 123;
    String expectedId = "[TASK:" + taskId + "." + Math.abs(("" + 123).hashCode()) + "]";
    String gotId = LoggerHandler.getFormattedTaskLoggingTag(taskId, startTime);
    assert expectedId.equals(gotId);
  }

  private void testAllLogMessagesRunner(String expectedTag) {
    String msg = "super useful logging msg";
    String formatMsg = "super {} useful {} logging {} msg {}";
    String expectedFormattedMsg = "super wow useful wow! logging 1 msg yay";

    this.testLogMessagesRunner(msg, Utils.formatLogMessage(expectedTag + msg));
    this.testLogMessagesWithFormattingRunner(
        formatMsg,
        Utils.formatLogMessage(expectedTag + expectedFormattedMsg),
        "wow",
        "wow!",
        1,
        "yay");
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
