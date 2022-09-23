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

import com.snowflake.kafka.connector.Utils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class LoggerHeaderBuilderTest {
  private final Logger testLogger = LoggerFactory.getLogger(this.getClass().getName());
  private final String descriptor = "descriptor";
  private final UUID id = UUID.randomUUID();
  private final String tag = "tag";
  private final UUID fallbackId = UUID.randomUUID();

  @Test
  public void testAllNull() {
    this.runner(null, null, null, null, "");
  }
  @Test
  public void testNullIdDescriptor() {
    this.runner(null, null, tag, null, "");
    this.runner(null, null, null, fallbackId, "");
    this.runner(null, null, tag, fallbackId, Utils.formatString("[{}:{}]", tag, fallbackId));

    this.runner(descriptor, null, tag, null, "");
    this.runner(descriptor, null, null, fallbackId, "");
    this.runner(descriptor, null, tag, fallbackId, Utils.formatString("[{}:{}]", tag, fallbackId));

    this.runner(null, id, tag, null, "");
    this.runner(null, id, null, fallbackId, "");
    this.runner(null, id, tag, fallbackId, Utils.formatString("[{}:{}]", tag, fallbackId));
  }
  @Test
  public void testNullTagFallbackId() {
    this.runner(descriptor, id, null, null, Utils.formatString("[{}:{}]", descriptor, id));
    this.runner(descriptor, id, tag, null, Utils.formatString("[{}:{}|{}]", descriptor, id, tag));
    this.runner(descriptor, id, null, fallbackId, Utils.formatString("[{}:{}]", descriptor, id));
  }
  @Test
  public void testAllValid() {
    this.runner(descriptor, id, tag, fallbackId, Utils.formatString("[{}:{}|{}]", descriptor, id, tag));
  }


  private void runner(String descriptor, UUID id, String tag, UUID fallbackId, String expectedHeader) {
    String resHeader = LoggerHeaderBuilderFactory.builder(testLogger, "test builder").setIdAndDescriptor(id,
      descriptor).setTagAndFallback(tag, fallbackId).build();

    assert resHeader.equals(expectedHeader) : Utils.formatString("got '{}', but expected '{}'", resHeader, expectedHeader);
  }
}
