package com.snowflake.test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.snowflake.Utils;
import com.snowflake.jdbc.ConnectionUtils;
import com.snowflake.producer.Producer;
import com.snowflake.reporter.JsonArray;
import com.snowflake.reporter.TestSuite;
import com.snowflake.test.Enums.TestCases;

public abstract class TestBase<T>
{
  private static final long TIME_OUT_SEC = 3600; // 40 mins
  private static final long SLEEP_TIME = 5000; // 5 sec
  private final JsonArray result = new JsonArray();
  private long time = 0;

  abstract protected Producer<T> getProducer();

  protected void test()
  {
    Utils.startConnector(getTestCase().getFormat());
    ExecutorService executor = Executors.newSingleThreadExecutor();
    Future<Boolean> task = executor.submit(() ->
    {
      while (!checkResult())
      {
        Thread.sleep(SLEEP_TIME);
      }
      return true;
    });

    try
    {
      task.get(TIME_OUT_SEC, TimeUnit.SECONDS);
    } catch (Exception e)
    {
      task.cancel(true);
      e.printStackTrace();
      System.exit(1);
    }

  }

  abstract protected TestCases getTestCase();

  private void beforeAll()
  {
    Utils.startConfluent();
    Utils.createTestTopic();
    ConnectionUtils.dropTestTable();
    ConnectionUtils.dropTestStage();
    ConnectionUtils.dropTestPipe();
    loadJsonData();
    System.out.println("Test on format: " + getTestCase().getFormatName() +
      " table: " + getTestCase().getTableName());
  }

  private void afterAll()
  {
    System.out.println("Test Completed, format: " + getTestCase().getFormatName() +
      " table: " + getTestCase().getTableName() + " time: " + time);
    Utils.stopConfluent();
  }

  private boolean checkResult()
  {
    return getTestCase().getTable().getSize() == ConnectionUtils.tableSize();
  }

  private void loadJsonData()
  {
    getProducer().send(getTestCase());
  }


  private void runTest()
  {
    Long time1 = System.currentTimeMillis();
    test();
    Long time2 = System.currentTimeMillis();
    time = time2 - time1;
    result.add(Utils.createTestCase(getTestCase(), time));
  }

  public final TestSuite run()
  {
    beforeAll();
    runTest();
    afterAll();
    return Utils.creaTestSuite(getTestCase().getFormat(), result);
  }

}