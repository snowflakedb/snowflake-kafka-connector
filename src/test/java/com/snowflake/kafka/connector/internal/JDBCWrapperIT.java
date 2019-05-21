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

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class JDBCWrapperIT
{

  SnowflakeJDBCWrapper jdbc;

  String tableName;

  String stageName;

  String pipeName;

  @Before
  public void beforeAll() throws Exception
  {
    Map<String, String> conf = TestUtils.getConf();

    jdbc = new SnowflakeJDBCWrapper(conf);

    tableName = TestUtils.randomTableName();

    stageName = TestUtils.randomStageName();

    pipeName = TestUtils.randomPipeName();
  }

  @After
  public void afterAll()
  {
    TestUtils.dropTable(tableName);

    jdbc.dropStage(stageName);

    jdbc.dropPipe(pipeName);

    jdbc.close();
  }


  @Test
  public void testTableFunctions() throws Exception
  {
    jdbc.createTable(tableName, true);

    assert jdbc.tableExist(tableName);

    TestUtils.executeQuery(
        "insert into " + tableName + " values(123, 223)"
    );

    ResultSet resultSet = TestUtils.showTable(tableName);

    assert Utils.resultSize(resultSet) == 1;

    jdbc.createTable(tableName);

    resultSet = TestUtils.showTable(tableName);

    assert Utils.resultSize(resultSet) == 1;

    jdbc.createTable(tableName, true);

    resultSet = TestUtils.showTable(tableName);

    assert Utils.resultSize(resultSet) == 0;

    TestUtils.dropTable(tableName);

    assert !jdbc.tableExist(tableName);

  }

  @Test
  public void testStageFunctions()
  {
    String fileName = "test.csv.gz";

    jdbc.createStage(stageName, true);

    assert jdbc.stageExist(stageName);

    String file = "123";

    jdbc.put(fileName, file, stageName);

    assert !jdbc.dropStageIfEmpty(stageName);

    assert jdbc.stageExist(stageName);

    jdbc.createStage(stageName);

    assert !jdbc.dropStageIfEmpty(stageName);

    jdbc.createStage(stageName, true);

    assert jdbc.dropStageIfEmpty(stageName);

    assert !jdbc.stageExist(stageName);

  }

  @Test
  public void testPipeFunctions()
  {
    jdbc.createTable(tableName);

    jdbc.createStage(stageName);

    jdbc.createPipe(pipeName, tableName, stageName, true);

    assert jdbc.pipeExist(pipeName);

    jdbc.createPipe(pipeName, tableName, stageName, false);

    assert jdbc.pipeExist(pipeName);

    jdbc.dropPipe(pipeName);

    assert !jdbc.pipeExist(pipeName);

    TestUtils.dropTable(tableName);

    jdbc.dropStage(stageName);

  }

  @Test
  public void testPurge()
  {
    jdbc.createStage(stageName);

    String fileName1 = "a/test.csv.gz";

    String fileName2 = "b/test.csv.gz";

    String file = "123";

    jdbc.put(fileName1, file, stageName);

    jdbc.put(fileName2, file, stageName);

    List<String> fileNames1 =  jdbc.listStage(stageName, "");

    assert fileNames1.size() == 2;

    assert fileNames1.get(0).equals(fileName1);

    assert fileNames1.get(1).equals(fileName2);

    List<String> fileNames2 = jdbc.listStage(stageName, "a");

    assert fileNames2.size() == 1;

    assert fileNames2.get(0).equals(fileName1);

    jdbc.purge(stageName, fileNames1);

    assert jdbc.listStage(stageName, "").size() == 0;

    jdbc.dropStage(stageName);
  }

  @Test
  public void moveFileFromStageToTableStage()
  {
    String fileName =
        Utils.fileName("test_topic",1,2,3);

    String content = "{\"meta\":1,\"content\":2}";

    jdbc.createStage(stageName);

    jdbc.createTable(tableName);

    jdbc.put(fileName, content, stageName);

    List<String> list = new ArrayList<>(1);

    list.add(fileName);

    jdbc.moveToTableStage(stageName, tableName, list);

    List<String> result = jdbc.listTableStage(tableName);

    assert result.size() == 1;

    assert result.get(0).equals(fileName);

    result = jdbc.listStage(stageName,"");

    assert result.isEmpty();

    jdbc.dropStage(stageName);

    TestUtils.dropTable(tableName);
  }

  @Test
  public void tableIsCompatibleTest()
  {
    jdbc.createTable(tableName);

    assert jdbc.tableIsCompatible(tableName);

    TestUtils.dropTable(tableName);

    TestUtils.executeQuery(
        "create table " + tableName +
            "(record_metadata variant, record_content variant)"
    );

    assert jdbc.tableIsCompatible(tableName);

    TestUtils.dropTable(tableName);

    //wrong name
    TestUtils.executeQuery(
        "create table " + tableName +
            "(recordmetadata variant, recordcontent variant)"
    );

    assert !jdbc.tableIsCompatible(tableName);

    TestUtils.dropTable(tableName);

    //wrong type
    TestUtils.executeQuery(
        "create table " + tableName +
            "(record_metadata string, record_content int)"
    );

    assert !jdbc.tableIsCompatible(tableName);

    TestUtils.dropTable(tableName);

    //wrong column number
    TestUtils.executeQuery(
        "create table " + tableName +
            "(record_metadata variant, record_content variant, something int)"
    );

    assert !jdbc.tableIsCompatible(tableName);

    TestUtils.dropTable(tableName);
  }

  @Test
  public void stageIsCompatibleTest()
  {
    jdbc.createStage(stageName);

    assert jdbc.stageIsCompatible(stageName);

    String fileName1 = Utils.fileName("test_topic",1,234,567);

    String fileContent = "123";

    jdbc.put(fileName1, fileContent, stageName);

    assert jdbc.stageIsCompatible(stageName);

    String fileName2 = "adasdsa.avro.gz";

    jdbc.put(fileName2, fileContent, stageName);

    assert ! jdbc.stageIsCompatible(stageName);

    jdbc.dropStage(stageName);
  }

  @Test
  public void pipeIsCompatibleTest()
  {
    jdbc.createStage(stageName);

    jdbc.createTable(tableName);

    jdbc.createPipe(pipeName, tableName, stageName, false);

    assert jdbc.pipeIsCompatible(pipeName, tableName, stageName);

    jdbc.dropPipe(pipeName);

    TestUtils.executeQuery(
        "create or replace pipe " + pipeName + " as copy into " + tableName +
            " from @" + stageName
    );

    assert !jdbc.pipeIsCompatible(pipeName, tableName, stageName);

    jdbc.dropPipe(pipeName);

    jdbc.dropStage(stageName);

    TestUtils.dropTable(tableName);



  }

  @Test
  public void putToTableStageTest()
  {
    jdbc.createTable(tableName);

    String file = "test";

    String fileName = Utils.fileName("test_topic", 0, 0, 1);

    jdbc.putToTableStage(fileName, file, tableName);

    assert jdbc.listTableStage(tableName).contains(fileName);

    TestUtils.dropTable(tableName);
  }


}
