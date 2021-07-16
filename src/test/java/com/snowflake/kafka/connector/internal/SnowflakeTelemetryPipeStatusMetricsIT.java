package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.records.SnowflakeConverter;
import com.snowflake.kafka.connector.records.SnowflakeJsonConverter;
import java.lang.management.ManagementFactory;
import java.nio.charset.StandardCharsets;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class SnowflakeTelemetryPipeStatusMetricsIT {
  private String topic = "test";
  private int partition = 0;
  private String tableName = TestUtils.randomTableName();
  private String stageName = Utils.stageName(TestUtils.TEST_CONNECTOR_NAME, tableName);
  private String pipeName = Utils.pipeName(TestUtils.TEST_CONNECTOR_NAME, tableName, partition);
  private SnowflakeConnectionService conn = TestUtils.getConnectionService();
  private ObjectMapper mapper = new ObjectMapper();

  @After
  public void afterEach() {
    conn.dropStage(stageName);
    TestUtils.dropTable(tableName);
    conn.dropPipe(pipeName);
  }

  @Test
  public void testJMXMetricsInMBeanServer() throws Exception {
    conn.createTable(tableName);
    conn.createStage(stageName);
    final String pipeName = Utils.pipeName(conn.getConnectorName(), tableName, partition);

    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn)
            .addTask(tableName, topic, partition)
            .setRecordNumber(1)
            .build();

    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue result =
        converter.toConnectData(topic, ("{\"name\":\"test\"}").getBytes(StandardCharsets.UTF_8));
    SinkRecord record =
        new SinkRecord(
            topic, partition, Schema.STRING_SCHEMA, "key1", result.schema(), result.value(), 0);

    service.insert(record);

    record =
        new SinkRecord(
            topic, partition, Schema.STRING_SCHEMA, "key2", result.schema(), result.value(), 1);

    service.insert(record);

    // required for committedOffset metric
    service.callAllGetOffset();

    final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
    final ObjectName objectName =
        SnowflakeTelemetryBasicInfo.metricName(pipeName, TestUtils.TEST_CONNECTOR_NAME);

    Assert.assertTrue(mBeanServer.getAttribute(objectName, "ProcessedOffset").equals(1l));
    Assert.assertTrue(mBeanServer.getAttribute(objectName, "FlushedOffset").equals(1l));

    // wait for ingest
    // Since number of records are 2, we expect two entries in table
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(tableName) == 2, 30, 20);

    // change cleaner
    TestUtils.assertWithRetry(
        () ->
            conn.listStage(
                        stageName,
                        FileNameUtils.filePrefix(
                            TestUtils.TEST_CONNECTOR_NAME, tableName, partition))
                    .size()
                == 0,
        30,
        20);
    Assert.assertTrue(mBeanServer.getAttribute(objectName, "CommittedOffset").equals(1l));
    Assert.assertTrue(mBeanServer.getAttribute(objectName, "PurgedOffset").equals(1l));

    // Since we have purged everything, all other file counters will be 0
    Assert.assertTrue(mBeanServer.getAttribute(objectName, "FileCountOnInternalStage").equals(0l));
    Assert.assertTrue(mBeanServer.getAttribute(objectName, "FileCountOnIngestion").equals(0l));
    Assert.assertTrue(
        mBeanServer.getAttribute(objectName, "FileCountFailedIngestionOnTableStage").equals(0l));
    Assert.assertTrue(
        mBeanServer.getAttribute(objectName, "FileCountBrokenRecordOnTableStage").equals(0l));
  }
}
