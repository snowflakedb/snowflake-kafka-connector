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

import org.junit.Test;

public class LoggingTest {

  @Test
  public void testLogMessageBasic() {
    // no variable
    String expected = Logging.SF_LOG_TAG + " test message";

    assert Logging.logMessage("test message").equals(expected);

    // 1 variable
    expected = Logging.SF_LOG_TAG + " 1 test message";

    assert Logging.logMessage("{} test message", 1).equals(expected);
  }

  @Test
  public void testLogMessageNulls() {
    // nulls
    String expected = Logging.SF_LOG_TAG + " null test message";
    assert Logging.logMessage("{} test message", (String) null).equals(expected);

    expected = Logging.SF_LOG_TAG + " some string test null message null";
    assert Logging.logMessage("{} test {} message {}", "some string", null, null).equals(expected);
  }

  @Test
  public void testLogMessageMultiLines() {
    // 2 variables
    String expected = Logging.SF_LOG_TAG + " 1 test message\n" + "2 test message";

    System.out.println(Logging.logMessage("{} test message\n{} test message", 1, 2));

    assert Logging.logMessage("{} test message\n{} test message", 1, 2).equals(expected);

    // 3 variables
    expected = Logging.SF_LOG_TAG + " 1 test message\n" + "2 test message\n" + "3 test message";

    assert Logging.logMessage("{} test message\n{} test message\n{} test " + "message", 1, 2, 3)
        .equals(expected);

    // 4 variables
    expected =
        Logging.SF_LOG_TAG
            + " 1 test message\n"
            + "2 test message\n"
            + "3 test message\n"
            + "4 test message";

    assert Logging.logMessage(
            "{} test message\n{} test message\n{} test " + "message\n{} test message", 1, 2, 3, 4)
        .equals(expected);
  }
}
