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
  // constants
  private final String name = "test.logger.name";
  private final String msg = "super useful logging message";
  private final String formatMsg = "a formatted msg is totally more useful than a unformatted {}";
  private final UUID kcGlobalInstanceId = UUID.randomUUID();
  private final String kcGlobalInstanceIdTag = "[KC:" + kcGlobalInstanceId.toString() + "] ";

  // mock and test setup
  @Mock(name = "logger")
  private Logger logger = Mockito.mock(Logger.class);

  @InjectMocks private LoggerHandler loggerHandler = new LoggerHandler(name);

  @Before
  public void initMocks() {
    LoggerHandler.setKcGlobalInstanceId(kcGlobalInstanceId);
    MockitoAnnotations.initMocks(this);
  }

  @After
  public void close() {
    LoggerHandler.setKcGlobalInstanceId(null);
  }

  @Test
  public void testAllLogMessage() {
    // info
    Mockito.when(logger.isInfoEnabled()).thenReturn(true);
    loggerHandler.info(msg);

    Mockito.verify(logger, Mockito.times(1))
        .info(Utils.formatLogMessage(kcGlobalInstanceIdTag + msg));

    // trace
    Mockito.when(logger.isTraceEnabled()).thenReturn(true);
    loggerHandler.trace(msg);

    Mockito.verify(logger, Mockito.times(1))
        .trace(Utils.formatLogMessage(kcGlobalInstanceIdTag + msg));

    // debug
    Mockito.when(logger.isDebugEnabled()).thenReturn(true);
    loggerHandler.debug(msg);

    Mockito.verify(logger, Mockito.times(1))
        .debug(Utils.formatLogMessage(kcGlobalInstanceIdTag + msg));

    // warn
    Mockito.when(logger.isWarnEnabled()).thenReturn(true);
    loggerHandler.warn(msg);

    Mockito.verify(logger, Mockito.times(1))
        .warn(Utils.formatLogMessage(kcGlobalInstanceIdTag + msg));

    // error
    Mockito.when(logger.isErrorEnabled()).thenReturn(true);
    loggerHandler.error(msg);

    Mockito.verify(logger, Mockito.times(1))
        .error(Utils.formatLogMessage(kcGlobalInstanceIdTag + msg));
  }

  @Test
  public void testAllLogMessageWithFormatting() {
    // info
    Mockito.when(logger.isInfoEnabled()).thenReturn(true);
    loggerHandler.info(formatMsg, msg);

    Mockito.verify(logger, Mockito.times(1))
        .info(Utils.formatLogMessage(kcGlobalInstanceIdTag + formatMsg, msg));

    // trace
    Mockito.when(logger.isTraceEnabled()).thenReturn(true);
    loggerHandler.trace(formatMsg, msg);

    Mockito.verify(logger, Mockito.times(1))
        .trace(Utils.formatLogMessage(kcGlobalInstanceIdTag + formatMsg, msg));

    // debug
    Mockito.when(logger.isDebugEnabled()).thenReturn(true);
    loggerHandler.debug(formatMsg, msg);

    Mockito.verify(logger, Mockito.times(1))
        .debug(Utils.formatLogMessage(kcGlobalInstanceIdTag + formatMsg, msg));

    // warn
    Mockito.when(logger.isWarnEnabled()).thenReturn(true);
    loggerHandler.warn(formatMsg, msg);

    Mockito.verify(logger, Mockito.times(1))
        .warn(Utils.formatLogMessage(kcGlobalInstanceIdTag + formatMsg, msg));

    // error
    Mockito.when(logger.isErrorEnabled()).thenReturn(true);
    loggerHandler.error(formatMsg, msg);

    Mockito.verify(logger, Mockito.times(1))
        .error(Utils.formatLogMessage(kcGlobalInstanceIdTag + formatMsg, msg));
  }

  @Test
  public void testLogMessageDisabled() {
    Mockito.when(logger.isInfoEnabled()).thenReturn(false);
    loggerHandler.info(msg);

    Mockito.verify(logger, Mockito.times(0)).info(Utils.formatLogMessage(msg));
  }

  @Test
  public void testLogMessageWithKcGlobalInstanceId() {
    Mockito.when(logger.isInfoEnabled()).thenReturn(true);
    LoggerHandler.setKcGlobalInstanceId(kcGlobalInstanceId);
    loggerHandler.info(msg);

    Mockito.verify(logger, Mockito.times(1))
        .info(Utils.formatLogMessage(kcGlobalInstanceIdTag + msg));
  }

  @Test
  public void testLoggerHandlerCreationWithKcGlobalInstanceId() {
    LoggerHandler.setKcGlobalInstanceId(kcGlobalInstanceId);
    LoggerHandler loggingHandler = new LoggerHandler(name);
  }
}
