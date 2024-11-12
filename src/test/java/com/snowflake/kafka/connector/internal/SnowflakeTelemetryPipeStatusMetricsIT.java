package com.snowflake.kafka.connector.internal;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.metrics.MetricsUtil;
import com.snowflake.kafka.connector.records.SnowflakeConverter;
import com.snowflake.kafka.connector.records.SnowflakeJsonConverter;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
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

    // This means that default is true.
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn)
            .addTask(tableName, new TopicPartition(topic, partition))
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

    MetricRegistry metricRegistry = service.getMetricRegistry(pipeName).get();
    Assert.assertFalse(metricRegistry.getMetrics().isEmpty());
    Assert.assertTrue(metricRegistry.getMetrics().size() == 14);

    Map<String, Gauge> registeredGauges = metricRegistry.getGauges();

    final String metricProcessedOffset =
        MetricsUtil.constructMetricName(
            pipeName, MetricsUtil.OFFSET_SUB_DOMAIN, MetricsUtil.PROCESSED_OFFSET);
    Assert.assertTrue(registeredGauges.containsKey(metricProcessedOffset));
    Assert.assertEquals(1L, (long) registeredGauges.get(metricProcessedOffset).getValue());

    final String metricFlushedOffset =
        MetricsUtil.constructMetricName(
            pipeName, MetricsUtil.OFFSET_SUB_DOMAIN, MetricsUtil.FLUSHED_OFFSET);
    Assert.assertTrue(registeredGauges.containsKey(metricFlushedOffset));
    Assert.assertEquals(1L, (long) registeredGauges.get(metricFlushedOffset).getValue());

    // wait for ingest
    // Since number of records are 2, we expect two entries in table
    TestUtils.assertWithRetry(() -> TestUtils.tableSize(tableName) == 2, 30, 20);

    // change cleaner
    TestUtils.assertWithRetry(
        () ->
            conn.listStage(
                        stageName,
                        FileNameUtils.filePrefix(
                            TestUtils.TEST_CONNECTOR_NAME, tableName, null, partition))
                    .size()
                == 0,
        30,
        20);

    final String metricCommittedOffset =
        MetricsUtil.constructMetricName(
            pipeName, MetricsUtil.OFFSET_SUB_DOMAIN, MetricsUtil.COMMITTED_OFFSET);
    Assert.assertTrue(registeredGauges.containsKey(metricCommittedOffset));
    Assert.assertEquals(1L, (long) registeredGauges.get(metricCommittedOffset).getValue());

    final String metricPurgedOffset =
        MetricsUtil.constructMetricName(
            pipeName, MetricsUtil.OFFSET_SUB_DOMAIN, MetricsUtil.PURGED_OFFSET);
    Assert.assertTrue(registeredGauges.containsKey(metricPurgedOffset));
    Assert.assertEquals(1L, (long) registeredGauges.get(metricPurgedOffset).getValue());

    // File count Gauges

    final String metricFileCountInternalStage =
        MetricsUtil.constructMetricName(
            pipeName, MetricsUtil.FILE_COUNT_SUB_DOMAIN, MetricsUtil.FILE_COUNT_ON_STAGE);
    Assert.assertTrue(registeredGauges.containsKey(metricFileCountInternalStage));
    Assert.assertEquals(0L, registeredGauges.get(metricFileCountInternalStage).getValue());

    final String metricFileCountIngestion =
        MetricsUtil.constructMetricName(
            pipeName, MetricsUtil.FILE_COUNT_SUB_DOMAIN, MetricsUtil.FILE_COUNT_ON_INGESTION);
    Assert.assertTrue(registeredGauges.containsKey(metricFileCountIngestion));
    Assert.assertEquals(0L, registeredGauges.get(metricFileCountIngestion).getValue());

    final String metricFileCountPurged =
        MetricsUtil.constructMetricName(
            pipeName, MetricsUtil.FILE_COUNT_SUB_DOMAIN, MetricsUtil.FILE_COUNT_PURGED);
    Assert.assertTrue(registeredGauges.containsKey(metricFileCountPurged));
    Assert.assertEquals(2L, registeredGauges.get(metricFileCountPurged).getValue());

    Map<String, Meter> registeredMeters = metricRegistry.getMeters();
    final String metricFileCountBrokenRecord =
        MetricsUtil.constructMetricName(
            pipeName,
            MetricsUtil.FILE_COUNT_SUB_DOMAIN,
            MetricsUtil.FILE_COUNT_TABLE_STAGE_BROKEN_RECORD);
    Assert.assertTrue(registeredMeters.containsKey(metricFileCountBrokenRecord));
    Assert.assertEquals(0L, registeredMeters.get(metricFileCountBrokenRecord).getCount());

    final String metricFileCountIngestionFailed =
        MetricsUtil.constructMetricName(
            pipeName,
            MetricsUtil.FILE_COUNT_SUB_DOMAIN,
            MetricsUtil.FILE_COUNT_TABLE_STAGE_INGESTION_FAIL);
    Assert.assertTrue(registeredMeters.containsKey(metricFileCountIngestionFailed));
    Assert.assertEquals(0L, registeredMeters.get(metricFileCountIngestionFailed).getCount());

    // buffer metrics
    Map<String, Histogram> registeredHistograms = metricRegistry.getHistograms();
    final String metricBufferRecordCount =
        MetricsUtil.constructMetricName(
            pipeName, MetricsUtil.BUFFER_SUB_DOMAIN, MetricsUtil.BUFFER_RECORD_COUNT);
    Assert.assertTrue(registeredHistograms.containsKey(metricBufferRecordCount));
    Assert.assertEquals(2L, registeredHistograms.get(metricBufferRecordCount).getCount());

    // two files will be generated both with one record each
    Assert.assertEquals(
        1.0, registeredHistograms.get(metricBufferRecordCount).getSnapshot().getMean(), 0.001);
    Assert.assertEquals(
        1.0, registeredHistograms.get(metricBufferRecordCount).getSnapshot().getMax(), 0.001);

    final String metricBufferSizeBytes =
        MetricsUtil.constructMetricName(
            pipeName, MetricsUtil.BUFFER_SUB_DOMAIN, MetricsUtil.BUFFER_SIZE_BYTES);
    Assert.assertTrue(registeredHistograms.containsKey(metricBufferSizeBytes));
    Assert.assertEquals(2L, registeredHistograms.get(metricBufferSizeBytes).getCount());

    // two files will be generated both with one record each
    Assert.assertEquals(2, registeredHistograms.get(metricBufferRecordCount).getSnapshot().size());
  }

  @Test
  public void testJMXDisabledInMBeanServer() {
    conn.createTable(tableName);
    conn.createStage(stageName);
    final String pipeName = Utils.pipeName(conn.getConnectorName(), tableName, partition);

    // This means that default is true.
    SnowflakeSinkService service =
        SnowflakeSinkServiceFactory.builder(conn)
            .setCustomJMXMetrics(false)
            .addTask(tableName, new TopicPartition(topic, partition))
            .setRecordNumber(1)
            .build();

    SnowflakeConverter converter = new SnowflakeJsonConverter();
    SchemaAndValue result =
        converter.toConnectData(topic, ("{\"name\":\"test\"}").getBytes(StandardCharsets.UTF_8));
    SinkRecord record =
        new SinkRecord(
            topic, partition, Schema.STRING_SCHEMA, "key1", result.schema(), result.value(), 0);

    service.insert(record);

    // required for committedOffset metric
    service.callAllGetOffset();

    MetricRegistry metricRegistry = service.getMetricRegistry(pipeName).get();
    Assert.assertTrue(metricRegistry.getMetrics().isEmpty());
  }
}
