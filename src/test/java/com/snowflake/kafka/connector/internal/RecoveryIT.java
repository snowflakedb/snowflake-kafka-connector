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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class RecoveryIT
{
  private SnowflakeJDBCWrapper jdbc;

  private String stage;

  private String pipe;

  private String table;

  @Before
  public void beforeAll() throws Exception
  {
    jdbc = new SnowflakeJDBCWrapper(TestUtils.getConf());

    stage = TestUtils.randomStageName();

    jdbc.createStage(stage);

    table = TestUtils.randomTableName();

    jdbc.createTable(table);

    pipe = TestUtils.randomPipeName();

    jdbc.createPipe(pipe, table, stage, false);


  }

  @After
  public void afterAll()
  {
    jdbc.dropPipe(pipe);

    jdbc.dropStage(stage);

    TestUtils.dropTable(table);
  }

  @Test
  public void recoveryTest() throws Exception
  {

    String file1 = "{\"meta\":123, \"content\":678}";

    String file2 = "{\"meta\":223, \"content\":567}";

    String file1Name = Utils.fileName("test_topic",0,0, 1);

    Thread.sleep(5);

    String file2Name = Utils.fileName("test_topic",0,1, 2);

    jdbc.ingestFile(pipe, file1Name, file1);

    //put one file to stage but not ingest
    jdbc.put(file2Name, file2, stage);

    //waiting for ingestion
    Thread.sleep(100000);

    Map<String, Utils.IngestedFileStatus> result = jdbc.recoverPipe(pipe, stage,
            Utils.subdirectoryName("test_topic", 0));

    assert result.get(file1Name).equals(Utils.IngestedFileStatus.LOADED);

    assert result.get(file2Name).equals(Utils.IngestedFileStatus.NOT_FOUND);

    Thread.sleep(100000);

    LinkedList<String> list = new LinkedList<>();

    list.add(file1Name);

    list.add(file2Name);

    result = jdbc.verifyFromIngestReport(pipe, list);

    assert result.get(file1Name).equals(Utils.IngestedFileStatus.LOADED);

    //auto re-ingest
    assert result.get(file2Name).equals(Utils.IngestedFileStatus.LOADED);

  }
}
