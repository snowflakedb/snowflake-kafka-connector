package com.snowflake.kafka.connector.internal;

import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import org.junit.After;
import org.junit.Test;

import java.util.List;

public class InternalStageTest {

  private final SnowflakeConnectionService service = TestUtils.getConnectionService();

  private final String stageName1 = TestUtils.randomStageName();
  private final String stageName2 = TestUtils.randomStageName();

  @After
  public void afterEach()
  {
    service.dropStage(stageName1);
    service.dropStage(stageName2);
  }

  @Test
  public void testInternalStage() throws Exception
  {
    //create stage
    service.createStage(stageName1);
    service.createStage(stageName2);

    SnowflakeInternalStage agent = new SnowflakeInternalStage();

    // PUT two files to stageName1
    agent.putWithCache((SnowflakeConnectionV1) service.getConnection(),
      stageName1, "testCacheFileName1", "Any cache".getBytes());
    agent.putWithCache((SnowflakeConnectionV1) service.getConnection(),
      stageName1, "testCacheFileName2", "Any cache".getBytes());

    List<String> files1 = service.listStage(stageName1, "testCache");
    assert files1.size() == 2;

    // PUT 50 files to stageName2
    int fileNumber = 50;
    for (int i = 0; i < fileNumber; i++)
    {
      agent.putWithCache((SnowflakeConnectionV1) service.getConnection(),
        stageName2, "appName/tableName/partition/testCacheFileName" + i, "Any cache".getBytes());
    }

    List<String> files2 = service.listStage(stageName2, "appName/tableName/partition/testCache");
    assert files2.size() == fileNumber;

  }

}
