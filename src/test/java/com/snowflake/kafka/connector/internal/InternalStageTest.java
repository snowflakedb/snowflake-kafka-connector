package com.snowflake.kafka.connector.internal;

import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import org.junit.After;
import org.junit.Test;

import java.util.List;

public class InternalStageTest {

  private final SnowflakeConnectionService service = TestUtils.getConnectionService();

  private final String stageName1 = TestUtils.randomStageName();
  private final String stageName2 = TestUtils.randomStageName();
  private final String stageName3 = TestUtils.randomStageName();

  @After
  public void afterEach()
  {
    service.dropStage(stageName1);
    service.dropStage(stageName2);
    service.dropStage(stageName3);
  }

  @Test
  public void testInternalStage() throws Exception
  {
    //create stage
    service.createStage(stageName1);
    service.createStage(stageName2);
    service.createStage(stageName3);

    SnowflakeInternalStage agent = new SnowflakeInternalStage((SnowflakeConnectionV1) service.getConnection());

    // PUT two files to stageName1
    long startTime = System.currentTimeMillis();
    agent.putWithCache(stageName1, "testCacheFileName1", "Any cache");
    agent.putWithCache(stageName1, "testCacheFileName2", "Any cache");
    List<String> files1 = service.listStage(stageName1, "testCache");
    assert files1.size() == 2;
    System.out.println(Logging.logMessage("Time: {} ms",
      (System.currentTimeMillis() - startTime)));

    // PUT 50 files to stageName2
    startTime = System.currentTimeMillis();
    int fileNumber = 50;
    for (int i = 0; i < fileNumber; i++)
    {
      agent.putWithCache(stageName2, "appName/tableName/partition/testCacheFileName" + i, "Any cache");
    }
    List<String> files2 = service.listStage(stageName2, "appName/tableName/partition/testCache");
    assert files2.size() == fileNumber;
    System.out.println(Logging.logMessage("Time: {} ms",
      (System.currentTimeMillis() - startTime)));

    // PUT 50 files to stageName3
    startTime = System.currentTimeMillis();
    for (int i = 0; i < fileNumber; i++)
    {
      service.put(stageName3, "appName/tableName/partition/testNoCacheFileName" + i, "Any cache");
    }
    List<String> files3 = service.listStage(stageName3, "appName/tableName/partition/testNoCache");
    assert files3.size() == fileNumber;
    System.out.println(Logging.logMessage("Time: {} ms",
      (System.currentTimeMillis() - startTime)));

  }

}