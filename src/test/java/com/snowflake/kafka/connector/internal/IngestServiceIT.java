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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class IngestServiceIT
{

  private SnowflakeIngestService ingestService = null;

  private SnowflakeJDBCWrapper jdbc = null;

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

    String fullPipeName = TestUtils.getFullPipeName(pipe);

    jdbc.createPipe(pipe, table, stage, false);

    SnowflakeURL url = TestUtils.getUrl();

    ingestService = new SnowflakeIngestService(
      url.getAccount(),
      TestUtils.getUser(),
      fullPipeName,
      url.getUrlWithoutPort(),
      TestUtils.getPrivateKey(),
      stage
    );

  }

  @After
  public void afterAll()
  {
    jdbc.dropPipe(pipe);

    jdbc.dropStage(stage);

    TestUtils.dropTable(table);
  }

  @Test
  public void ingestFileTest()
  {
    String file = "{\"aa\":123}";

    String fileName =
      Utils.fileName(TestUtils.TEST_CONNECTOR_NAME, "test_topic", 0, 0, 1);

    jdbc.put(fileName, file, stage);

    ingestService.ingestFile(fileName);

    List<String> names = new ArrayList<>(1);

    names.add(fileName);

    //ingest report
    assert ingestService.checkIngestReport(names, 120000);

    //load history
    Map<String, Utils.IngestedFileStatus> result =
      ingestService.checkOneHourHistory(names, System.currentTimeMillis() -
        SnowflakeIngestService.ONE_HOUR);

    assert result.get(fileName).equals(Utils.IngestedFileStatus.LOADED);
  }

}
