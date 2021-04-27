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
  public void testLogMessageGenerator() {
    // no variable
    String expected = "\n" + Logging.SF_LOG_TAG + " test message";

    assert Logging.logMessage("test message").equals(expected);

    // 1 variable
    expected = "\n" + Logging.SF_LOG_TAG + " 1 test message";

    assert Logging.logMessage("{} test message", 1).equals(expected);

    // 2 variables
    expected =
        "\n" + Logging.SF_LOG_TAG + " 1 test message\n" + Logging.SF_LOG_TAG + " 2 test message";

    assert Logging.logMessage("{} test message\n{} test message", 1, 2).equals(expected);

    // 3 variables
    expected =
        "\n"
            + Logging.SF_LOG_TAG
            + " 1 test message\n"
            + Logging.SF_LOG_TAG
            + " 2 test message\n"
            + Logging.SF_LOG_TAG
            + " 3 test "
            + "message";

    assert Logging.logMessage("{} test message\n{} test message\n{} test " + "message", 1, 2, 3)
        .equals(expected);

    // 4 variables
    expected =
        "\n"
            + Logging.SF_LOG_TAG
            + " 1 test message\n"
            + Logging.SF_LOG_TAG
            + " 2 test message\n"
            + Logging.SF_LOG_TAG
            + " 3 test "
            + "message\n"
            + Logging.SF_LOG_TAG
            + " 4 test message";

    assert Logging.logMessage(
            "{} test message\n{} test message\n{} test " + "message\n{} test message", 1, 2, 3, 4)
        .equals(expected);

    // nulls
    expected = "\n" + Logging.SF_LOG_TAG + " null test message";
    assert Logging.logMessage("{} test message", (String) null).equals(expected);

    expected = "\n" + Logging.SF_LOG_TAG + " some string test null message null";
    assert Logging.logMessage("{} test {} message {}", "some string", null, null).equals(expected);
  }
}
