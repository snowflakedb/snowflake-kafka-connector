package com.snowflake.kafka.connector.internal;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class IngestionServiceIT
{

  private SnowflakeIngestionService ingestService = null;
  private final SnowflakeConnectionService conn =
    SnowflakeConnectionServiceFactory
      .builder()
      .setProperties(TestUtils.getConf())
      .build();
  private String stage = TestUtils.randomStageName();
  private String pipe = TestUtils.randomPipeName();
  private String table = TestUtils.randomTableName();


  @Before
  public void before()
  {
    conn.createStage(stage);
    conn.createTable(table);
    conn.createPipe(table, stage, pipe);
    ingestService =  conn.buildIngestService(stage, pipe);

  }

  @After
  public void after()
  {
    conn.dropPipe(pipe);
    conn.dropStage(stage);
    TestUtils.dropTable(table);
  }

  @Test
  public void ingestFileTest() throws Exception
  {
    String file = "{\"aa\":123}";
    String fileName =
      FileNameUtils.fileName(TestUtils.TEST_CONNECTOR_NAME, table, 0, 0, 1);

    conn.put(stage, fileName, file);
    ingestService.ingestFile(fileName);
    List<String> names = new ArrayList<>(1);
    names.add(fileName);
    //ingest report
    TestUtils.assertWithRetry(() ->
    {
      Map<String, InternalUtils.IngestedFileStatus> result = ingestService.readIngestReport(names);
      return result.get(fileName).equals(InternalUtils.IngestedFileStatus.LOADED);
    }, 60, 9);
    //load history
    TestUtils.assertWithRetry(() ->
    {
      Map<String, InternalUtils.IngestedFileStatus> result =
        ingestService.readOneHourHistory(names, System.currentTimeMillis() -
          3600 * 1000);
      System.out.println(result.get(fileName));
      return result.get(fileName).equals(InternalUtils.IngestedFileStatus.LOADED);
    }, 15, 4);

    assert ingestService.getStageName().equals(stage);
  }
}
