package com.snowflake.kafka.connector.internal.telemetry;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.KEY_CONVERTER_CONFIG_FIELD;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelCreation;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryServiceV2;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryData;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class SnowflakeTelemetryServiceTest {
  @Parameterized.Parameters(name = "ingestionMethod: {0}")
  public static List<IngestionMethodConfig> input() {
    return Arrays.asList(IngestionMethodConfig.SNOWPIPE, IngestionMethodConfig.SNOWPIPE_STREAMING);
  }

  private final IngestionMethodConfig ingestionMethodConfig;
  private final SnowflakeTelemetryService snowflakeTelemetryService;
  private final long startTime;
  private final Map<String, String> config;
  private MockTelemetryClient mockTelemetryClient;

  public static final String KAFKA_STRING_CONVERTER =
      "org.apache.kafka.connect.storage.StringConverter";
  public static final String KAFKA_CONFLUENT_AVRO_CONVERTER =
      "io.confluent.connect.avro.AvroConverter";

  public SnowflakeTelemetryServiceTest(IngestionMethodConfig ingestionMethodConfig) {
    this.startTime = System.currentTimeMillis();
    this.ingestionMethodConfig = ingestionMethodConfig;
    this.mockTelemetryClient = new MockTelemetryClient();

    if (this.ingestionMethodConfig == IngestionMethodConfig.SNOWPIPE) {
      this.snowflakeTelemetryService = new SnowflakeTelemetryServiceV1(this.mockTelemetryClient);
      this.config = TestUtils.getConfig();
    } else {
      this.snowflakeTelemetryService = new SnowflakeTelemetryServiceV2(this.mockTelemetryClient);
      this.config = TestUtils.getConfForStreaming();
      SnowflakeSinkConnectorConfig.setDefaultValues(this.config);
    }

    this.snowflakeTelemetryService.setAppName("TEST_APP");
    this.snowflakeTelemetryService.setTaskID("1");
  }

  @Test
  public void testReportKafkaConnectStart() {
    addKeyAndValueConvertersToConfigMap(this.config);

    // test report start
    this.snowflakeTelemetryService.reportKafkaConnectStart(System.currentTimeMillis(), this.config);

    // validate data sent
    LinkedList<TelemetryData> sentData = this.mockTelemetryClient.getSentTelemetryData();
    Assert.assertEquals(1, sentData.size());

    JsonNode allNode = sentData.get(0).getMessage();
    Assert.assertEquals(
        SnowflakeTelemetryService.TelemetryType.KAFKA_START.toString(),
        allNode.get("type").asText());
    Assert.assertEquals("kafka_connector", allNode.get("source").asText());
    Assert.assertEquals(Utils.VERSION, allNode.get("version").asText());

    JsonNode dataNode = allNode.get("data");
    Assert.assertNotNull(dataNode);
    Assert.assertTrue(dataNode.has(INGESTION_METHOD_OPT));
    Assert.assertTrue(
        dataNode
            .get(INGESTION_METHOD_OPT)
            .asText()
            .equalsIgnoreCase(this.ingestionMethodConfig.toString()));
    Assert.assertTrue(
        dataNode.get(TelemetryConstants.START_TIME).asLong() <= System.currentTimeMillis()
            && dataNode.get(TelemetryConstants.START_TIME).asLong() >= this.startTime);

    validateBufferProperties(dataNode);
    validateKeyAndValueConverter(dataNode);
  }

  @Test
  public void testReportKafkaConnectStop() {
    // test report start
    this.snowflakeTelemetryService.reportKafkaConnectStop(System.currentTimeMillis());

    // validate data sent
    LinkedList<TelemetryData> sentData = this.mockTelemetryClient.getSentTelemetryData();
    Assert.assertEquals(1, sentData.size());

    JsonNode allNode = sentData.get(0).getMessage();
    Assert.assertEquals(
        SnowflakeTelemetryService.TelemetryType.KAFKA_STOP.toString(),
        allNode.get("type").asText());
    Assert.assertEquals("kafka_connector", allNode.get("source").asText());
    Assert.assertEquals(Utils.VERSION, allNode.get("version").asText());

    JsonNode dataNode = allNode.get("data");
    Assert.assertNotNull(dataNode);
    Assert.assertTrue(dataNode.has(INGESTION_METHOD_OPT));
    Assert.assertTrue(
        dataNode.get(INGESTION_METHOD_OPT).asInt() == this.ingestionMethodConfig.ordinal());
    Assert.assertTrue(
        dataNode.get(TelemetryConstants.START_TIME).asLong() <= System.currentTimeMillis()
            && dataNode.get(TelemetryConstants.START_TIME).asLong() >= this.startTime);
  }

  @Test
  public void testReportKafkaConnectFatalError() {
    final String expectedException =
        SnowflakeErrors.ERROR_0003.getException("test exception").getMessage();

    // test report start
    this.snowflakeTelemetryService.reportKafkaConnectFatalError(expectedException);

    // validate data sent
    LinkedList<TelemetryData> sentData = this.mockTelemetryClient.getSentTelemetryData();
    Assert.assertEquals(1, sentData.size());

    JsonNode allNode = sentData.get(0).getMessage();
    Assert.assertEquals(
        SnowflakeTelemetryService.TelemetryType.KAFKA_FATAL_ERROR.toString(),
        allNode.get("type").asText());
    Assert.assertEquals("kafka_connector", allNode.get("source").asText());
    Assert.assertEquals(Utils.VERSION, allNode.get("version").asText());

    JsonNode dataNode = allNode.get("data");
    Assert.assertNotNull(dataNode);
    Assert.assertTrue(dataNode.has(INGESTION_METHOD_OPT));
    Assert.assertTrue(
        dataNode.get(INGESTION_METHOD_OPT).asInt() == this.ingestionMethodConfig.ordinal());
    Assert.assertTrue(
        dataNode.get("time").asLong() <= System.currentTimeMillis()
            && dataNode.get("time").asLong() >= this.startTime);
    Assert.assertEquals(dataNode.get("error_number").asText(), expectedException);
  }

  @Test
  public void testReportKafkaPartitionUsage() {
    // expected values
    final String expectedTableName = "tableName";
    final String expectedStageName = "stageName";
    final String expectedPipeName = "pipeName";
    final String expectedConnectorName = "connectorName";
    final String expectedTpChannelName = "channelName";
    final long expectedTpChannelCreationTime = 1234;
    final long expectedProcessedOffset = 1;
    final long expectedFlushedOffset = 2;
    final long expectedCommittedOffset = 3;
    final long expectedOffsetPersistedInSnowflake = 4;
    final long expectedLatestConsumerOffset = 5;

    SnowflakeTelemetryBasicInfo partitionUsage;

    if (this.ingestionMethodConfig == IngestionMethodConfig.SNOWPIPE) {
      SnowflakeTelemetryPipeStatus pipeStatus =
          new SnowflakeTelemetryPipeStatus(
              expectedTableName, expectedStageName, expectedPipeName, false, null);
      pipeStatus.setFlushedOffset(expectedFlushedOffset);
      pipeStatus.setProcessedOffset(expectedProcessedOffset);
      pipeStatus.setCommittedOffset(expectedCommittedOffset);

      partitionUsage = pipeStatus;
    } else {
      SnowflakeTelemetryChannelStatus channelStatus =
          new SnowflakeTelemetryChannelStatus(
              expectedTableName,
              expectedConnectorName,
              expectedTpChannelName,
              expectedTpChannelCreationTime,
              false,
              null,
              new AtomicLong(expectedOffsetPersistedInSnowflake),
              new AtomicLong(expectedProcessedOffset),
              new AtomicLong(expectedLatestConsumerOffset));

      partitionUsage = channelStatus;
    }

    // test report start
    this.snowflakeTelemetryService.reportKafkaPartitionUsage(partitionUsage, false);

    // validate data sent
    LinkedList<TelemetryData> sentData = this.mockTelemetryClient.getSentTelemetryData();
    Assert.assertEquals(1, sentData.size());

    JsonNode allNode = sentData.get(0).getMessage();
    Assert.assertEquals("kafka_connector", allNode.get("source").asText());
    Assert.assertEquals(Utils.VERSION, allNode.get("version").asText());

    JsonNode dataNode = allNode.get("data");
    Assert.assertNotNull(dataNode);
    Assert.assertTrue(dataNode.has(INGESTION_METHOD_OPT));
    Assert.assertTrue(
        dataNode.get(INGESTION_METHOD_OPT).asInt() == this.ingestionMethodConfig.ordinal());
    Assert.assertEquals(
        expectedProcessedOffset, dataNode.get(TelemetryConstants.PROCESSED_OFFSET).asLong());
    Assert.assertEquals(expectedTableName, dataNode.get(TelemetryConstants.TABLE_NAME).asText());

    if (ingestionMethodConfig == IngestionMethodConfig.SNOWPIPE) {
      Assert.assertTrue(
          dataNode.get(TelemetryConstants.START_TIME).asLong() <= System.currentTimeMillis()
              && dataNode.get(TelemetryConstants.START_TIME).asLong() >= this.startTime);
      Assert.assertEquals(
          SnowflakeTelemetryService.TelemetryType.KAFKA_PIPE_USAGE.toString(),
          allNode.get("type").asText());
      Assert.assertEquals(
          expectedFlushedOffset, dataNode.get(TelemetryConstants.FLUSHED_OFFSET).asLong());
      Assert.assertEquals(
          expectedCommittedOffset, dataNode.get(TelemetryConstants.COMMITTED_OFFSET).asLong());
      Assert.assertEquals(expectedPipeName, dataNode.get(TelemetryConstants.PIPE_NAME).asText());
      Assert.assertEquals(expectedStageName, dataNode.get(TelemetryConstants.STAGE_NAME).asText());
    } else {
      Assert.assertTrue(
          dataNode.get(TelemetryConstants.TOPIC_PARTITION_CHANNEL_CREATION_TIME).asLong()
              == expectedTpChannelCreationTime);
      Assert.assertTrue(
          dataNode.get(TelemetryConstants.TOPIC_PARTITION_CHANNEL_CLOSE_TIME).asLong()
                  <= System.currentTimeMillis()
              && dataNode.get(TelemetryConstants.TOPIC_PARTITION_CHANNEL_CLOSE_TIME).asLong()
                  >= this.startTime);
      Assert.assertEquals(
          SnowflakeTelemetryService.TelemetryType.KAFKA_CHANNEL_USAGE.toString(),
          allNode.get("type").asText());
      Assert.assertEquals(
          expectedLatestConsumerOffset,
          dataNode.get(TelemetryConstants.LATEST_CONSUMER_OFFSET).asLong());
      Assert.assertEquals(
          expectedOffsetPersistedInSnowflake,
          dataNode.get(TelemetryConstants.OFFSET_PERSISTED_IN_SNOWFLAKE).asLong());
      Assert.assertEquals(
          expectedTpChannelName,
          dataNode.get(TelemetryConstants.TOPIC_PARTITION_CHANNEL_NAME).asText());
      Assert.assertEquals(
          expectedConnectorName, dataNode.get(TelemetryConstants.CONNECTOR_NAME).asText());
    }
  }

  @Test
  public void testReportKafkaPartitionStart() {
    SnowflakeTelemetryBasicInfo partitionCreation;
    final String expectedTableName = "tableName";
    final String expectedStageName = "stageName";
    final String expectedPipeName = "pipeName";
    final String expectedChannelName = "channelName";
    final long expectedChannelCreationTime = 1234;

    if (this.ingestionMethodConfig == IngestionMethodConfig.SNOWPIPE) {
      SnowflakeTelemetryPipeCreation pipeCreation =
          new SnowflakeTelemetryPipeCreation(
              expectedTableName, expectedStageName, expectedPipeName);
      pipeCreation.setReuseTable(true);
      pipeCreation.setReusePipe(true);
      pipeCreation.setReuseStage(true);
      pipeCreation.setFileCountReprocessPurge(10);
      pipeCreation.setFileCountRestart(11);

      partitionCreation = pipeCreation;
    } else {
      SnowflakeTelemetryChannelCreation channelCreation =
          new SnowflakeTelemetryChannelCreation(
              expectedTableName, expectedChannelName, expectedChannelCreationTime);
      channelCreation.setReuseTable(true);

      partitionCreation = channelCreation;
    }

    // test report start
    this.snowflakeTelemetryService.reportKafkaPartitionStart(partitionCreation);

    // validate data sent
    LinkedList<TelemetryData> sentData = this.mockTelemetryClient.getSentTelemetryData();
    Assert.assertEquals(1, sentData.size());

    JsonNode allNode = sentData.get(0).getMessage();
    Assert.assertEquals("kafka_connector", allNode.get("source").asText());
    Assert.assertEquals(Utils.VERSION, allNode.get("version").asText());

    JsonNode dataNode = allNode.get("data");
    Assert.assertNotNull(dataNode);
    Assert.assertTrue(dataNode.has(INGESTION_METHOD_OPT));
    Assert.assertTrue(
        dataNode.get(INGESTION_METHOD_OPT).asInt() == this.ingestionMethodConfig.ordinal());
    Assert.assertTrue(dataNode.get(TelemetryConstants.IS_REUSE_TABLE).asBoolean());
    Assert.assertEquals(expectedTableName, dataNode.get(TelemetryConstants.TABLE_NAME).asText());

    if (ingestionMethodConfig == IngestionMethodConfig.SNOWPIPE) {
      Assert.assertTrue(
          dataNode.get(TelemetryConstants.START_TIME).asLong() <= System.currentTimeMillis()
              && dataNode.get(TelemetryConstants.START_TIME).asLong() >= this.startTime);
      Assert.assertEquals(
          SnowflakeTelemetryService.TelemetryType.KAFKA_PIPE_START.toString(),
          allNode.get("type").asText());
      Assert.assertTrue(dataNode.get(TelemetryConstants.IS_REUSE_PIPE).asBoolean());
      Assert.assertTrue(dataNode.get(TelemetryConstants.IS_REUSE_STAGE).asBoolean());
      Assert.assertEquals(10, dataNode.get(TelemetryConstants.FILE_COUNT_REPROCESS_PURGE).asInt());
      Assert.assertEquals(11, dataNode.get(TelemetryConstants.FILE_COUNT_RESTART).asInt());
      Assert.assertEquals(expectedStageName, dataNode.get(TelemetryConstants.STAGE_NAME).asText());
      Assert.assertEquals(expectedPipeName, dataNode.get(TelemetryConstants.PIPE_NAME).asText());
    } else {
      Assert.assertTrue(
          dataNode.get(TelemetryConstants.TOPIC_PARTITION_CHANNEL_CREATION_TIME).asLong()
              == expectedChannelCreationTime);
      Assert.assertEquals(
          SnowflakeTelemetryService.TelemetryType.KAFKA_CHANNEL_START.toString(),
          allNode.get("type").asText());
      Assert.assertEquals(
          expectedChannelName,
          dataNode.get(TelemetryConstants.TOPIC_PARTITION_CHANNEL_NAME).asText());
    }
  }

  private void addKeyAndValueConvertersToConfigMap(Map<String, String> userProvidedConfig) {
    userProvidedConfig.put(KEY_CONVERTER_CONFIG_FIELD, KAFKA_STRING_CONVERTER);
    userProvidedConfig.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD, KAFKA_CONFLUENT_AVRO_CONVERTER);
  }

  private void validateKeyAndValueConverter(JsonNode dataNode) {
    Assert.assertTrue(dataNode.has(KEY_CONVERTER_CONFIG_FIELD));
    Assert.assertTrue(
        dataNode.get(KEY_CONVERTER_CONFIG_FIELD).asText().equalsIgnoreCase(KAFKA_STRING_CONVERTER));

    Assert.assertTrue(dataNode.has(VALUE_CONVERTER_CONFIG_FIELD));
    Assert.assertTrue(
        dataNode
            .get(VALUE_CONVERTER_CONFIG_FIELD)
            .asText()
            .equalsIgnoreCase(KAFKA_CONFLUENT_AVRO_CONVERTER));
  }

  private void validateBufferProperties(JsonNode dataNode) {
    Assert.assertTrue(dataNode.has(BUFFER_SIZE_BYTES));
    Assert.assertTrue(isNumeric(dataNode.get(BUFFER_SIZE_BYTES).asText()));

    Assert.assertTrue(dataNode.has(BUFFER_COUNT_RECORDS));
    Assert.assertTrue(isNumeric(dataNode.get(BUFFER_COUNT_RECORDS).asText()));

    Assert.assertTrue(dataNode.has(BUFFER_FLUSH_TIME_SEC));
    Assert.assertTrue(isNumeric(dataNode.get(BUFFER_FLUSH_TIME_SEC).asText()));
  }

  private static boolean isNumeric(String strNum) {
    if (strNum == null) {
      return false;
    }
    try {
      Long.parseLong(strNum);
    } catch (NumberFormatException nfe) {
      return false;
    }
    return true;
  }

  public static class MockTelemetryClient implements Telemetry {

    private final LinkedList<TelemetryData> telemetryDataList;

    private final LinkedList<TelemetryData> sentTelemetryData;

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    public MockTelemetryClient() {
      this.telemetryDataList = new LinkedList<>();
      this.sentTelemetryData = new LinkedList<>();
    }

    @Override
    public void addLogToBatch(TelemetryData telemetryData) {
      this.telemetryDataList.add(telemetryData);
    }

    @Override
    public void close() {
      this.telemetryDataList.clear();
      this.sentTelemetryData.clear();
    }

    @Override
    public Future<Boolean> sendBatchAsync() {
      return executor.submit(() -> true);
    }

    @Override
    public void postProcess(String s, String s1, int i, Throwable throwable) {}

    public LinkedList<TelemetryData> getSentTelemetryData() {
      this.sentTelemetryData.addAll(telemetryDataList);
      this.telemetryDataList.clear();
      return sentTelemetryData;
    }
  }
}
