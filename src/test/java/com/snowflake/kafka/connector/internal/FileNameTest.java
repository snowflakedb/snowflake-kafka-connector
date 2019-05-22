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

import com.snowflake.kafka.connector.TestUtils;
import com.snowflake.kafka.connector.Utils;
import org.junit.Test;

public class FileNameTest
{
  @Test
  public void fileNameTest() throws InterruptedException
  {
    int partition = 123;

    long startOffset = 456;

    long endOffset = 789;

    String topic = "test_topic";

    long time1 = System.currentTimeMillis();

    Thread.sleep(5); //error in maven without sleep

    String fileName = Utils.fileName(TestUtils.TEST_CONNECTOR_NAME, topic, partition, startOffset, endOffset);

    Thread.sleep(5);

    long time2 = System.currentTimeMillis();

    assert Utils.fileNameToStartOffset(fileName) == startOffset;

    assert Utils.fileNameToEndOffset(fileName) == endOffset;

    assert Utils.fileNameToPartition(fileName) == partition;

    long time3 = Utils.fileNameToTimeIngested(fileName);

    assert (time3 > time1) && (time3 < time2);

  }

  @Test
  public void fileExpiredTest()
  {
    long time = Utils.currentTime();

    String expiredFile = "test_app/test_topic/1/123_456_" +
        (time - Utils.MAX_RECOVERY_TIME - 3600 * 1000) + ".json.gz";

    String unexpiredFile = "test_app/test_topic/1/123_456_" +
        (time - Utils.MAX_RECOVERY_TIME + 3600 * 1000) + ".json.gz";

    assert Utils.isFileExpired(expiredFile);

    assert !Utils.isFileExpired(unexpiredFile);
  }

  @Test
  public void verifyFileNameTest()
  {
    String name1 = "app/topic/1/234_567_8901234.json.gz";

    String name2 = "asdasdasd.json.gz";

    assert Utils.verifyFileName(name1);

    assert !Utils.verifyFileName(name2);
  }
}
