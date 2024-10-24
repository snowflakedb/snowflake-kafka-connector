package com.snowflake.kafka.connector.internal.telemetry;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;
import static org.junit.jupiter.api.Assertions.*;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelCreation;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryServiceV2;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryData;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

public class SnowflakeTelemetryServiceTest {

  private long startTime;
  private MockTelemetryClient mockTelemetryClient;

  public static final String KAFKA_STRING_CONVERTER =
      "org.apache.kafka.connect.storage.StringConverter";
  public static final String KAFKA_CONFLUENT_AVRO_CONVERTER =
      "io.confluent.connect.avro.AvroConverter";

  @BeforeEach
  void setUp() {
    this.startTime = System.currentTimeMillis();
    this.mockTelemetryClient = new MockTelemetryClient();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void shouldReportSingleBufferUsageForStreaming(boolean singleBufferEnabled) {
    // given
    Map<String, String> connectorConfig =
        createConnectorConfig(IngestionMethodConfig.SNOWPIPE_STREAMING);
    connectorConfig.put(
        SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER, String.valueOf(singleBufferEnabled));
    SnowflakeTelemetryService snowflakeTelemetryService =
        createSnowflakeTelemetryService(IngestionMethodConfig.SNOWPIPE_STREAMING, connectorConfig);

    // when
    snowflakeTelemetryService.reportKafkaConnectStart(System.currentTimeMillis(), connectorConfig);

    // then
    assertEquals(
        String.valueOf(singleBufferEnabled),
        sentTelemetryDataField(SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER));
  }

  @Test
  public void shouldReportSingleBufferUsageDefaultValue() {
    // given
    Map<String, String> connectorConfig =
        createConnectorConfig(IngestionMethodConfig.SNOWPIPE_STREAMING);
    SnowflakeTelemetryService snowflakeTelemetryService =
        createSnowflakeTelemetryService(IngestionMethodConfig.SNOWPIPE_STREAMING, connectorConfig);

    // when
    snowflakeTelemetryService.reportKafkaConnectStart(System.currentTimeMillis(), connectorConfig);

    // then
    assertEquals(
        String.valueOf(SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER_DEFAULT),
        sentTelemetryDataField(SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER));
  }

  @ParameterizedTest
  @EnumSource(value = IngestionMethodConfig.class)
  public void testReportKafkaConnectStart(IngestionMethodConfig ingestionMethodConfig) {
    // given
    Map<String, String> connectorConfig = createConnectorConfig(ingestionMethodConfig);
    connectorConfig.put(KEY_CONVERTER_CONFIG_FIELD, KAFKA_STRING_CONVERTER);
    connectorConfig.put(
        SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD, KAFKA_CONFLUENT_AVRO_CONVERTER);
    SnowflakeTelemetryService snowflakeTelemetryService =
        createSnowflakeTelemetryService(ingestionMethodConfig, connectorConfig);

    // when
    snowflakeTelemetryService.reportKafkaConnectStart(System.currentTimeMillis(), connectorConfig);

    // then
    LinkedList<TelemetryData> sentData = this.mockTelemetryClient.getSentTelemetryData();
    assertEquals(1, sentData.size());

    JsonNode allNode = sentData.get(0).getMessage();
    assertEquals(
        SnowflakeTelemetryService.TelemetryType.KAFKA_START.toString(),
        allNode.get("type").asText());
    assertEquals("kafka_connector", allNode.get("source").asText());
    assertEquals(Utils.VERSION, allNode.get("version").asText());

    assertEquals(sentTelemetryDataField(INGESTION_METHOD_OPT), ingestionMethodConfig.toString());

    JsonNode dataNode = allNode.get("data");
    assertTrue(
        dataNode.get(TelemetryConstants.START_TIME).asLong() <= System.currentTimeMillis()
            && dataNode.get(TelemetryConstants.START_TIME).asLong() >= this.startTime);

    assertNotNull(dataNode.get("jdk_version"));
    assertNotNull(dataNode.get("jdk_distribution"));

    validateBufferProperties(dataNode);
    validateKeyAndValueConverter(dataNode);
  }

  @ParameterizedTest
  @EnumSource(value = IngestionMethodConfig.class)
  public void testReportKafkaConnectStop(IngestionMethodConfig ingestionMethodConfig) {
    // given
    Map<String, String> connectorConfig = createConnectorConfig(ingestionMethodConfig);
    SnowflakeTelemetryService snowflakeTelemetryService =
        createSnowflakeTelemetryService(ingestionMethodConfig, connectorConfig);

    // when
    snowflakeTelemetryService.reportKafkaConnectStop(System.currentTimeMillis());

    // then
    LinkedList<TelemetryData> sentData = this.mockTelemetryClient.getSentTelemetryData();
    assertEquals(1, sentData.size());

    JsonNode allNode = sentData.get(0).getMessage();
    assertEquals(
        SnowflakeTelemetryService.TelemetryType.KAFKA_STOP.toString(),
        allNode.get("type").asText());
    assertEquals("kafka_connector", allNode.get("source").asText());
    assertEquals(Utils.VERSION, allNode.get("version").asText());

    JsonNode dataNode = allNode.get("data");
    assertNotNull(dataNode);
    assertTrue(dataNode.has(INGESTION_METHOD_OPT));
    assertTrue(dataNode.get(INGESTION_METHOD_OPT).asInt() == ingestionMethodConfig.ordinal());
    assertTrue(
        dataNode.get(TelemetryConstants.START_TIME).asLong() <= System.currentTimeMillis()
            && dataNode.get(TelemetryConstants.START_TIME).asLong() >= this.startTime);
  }

  @ParameterizedTest
  @EnumSource(value = IngestionMethodConfig.class)
  public void testReportKafkaConnectFatalError(IngestionMethodConfig ingestionMethodConfig) {
    // given
    Map<String, String> connectorConfig = createConnectorConfig(ingestionMethodConfig);
    SnowflakeTelemetryService snowflakeTelemetryService =
        createSnowflakeTelemetryService(ingestionMethodConfig, connectorConfig);
    String expectedException =
        SnowflakeErrors.ERROR_0003.getException("test exception").getMessage();

    // when
    snowflakeTelemetryService.reportKafkaConnectFatalError(expectedException);

    // validate data sent
    LinkedList<TelemetryData> sentData = this.mockTelemetryClient.getSentTelemetryData();
    assertEquals(1, sentData.size());

    JsonNode allNode = sentData.get(0).getMessage();
    assertEquals(
        SnowflakeTelemetryService.TelemetryType.KAFKA_FATAL_ERROR.toString(),
        allNode.get("type").asText());
    assertEquals("kafka_connector", allNode.get("source").asText());
    assertEquals(Utils.VERSION, allNode.get("version").asText());

    JsonNode dataNode = allNode.get("data");
    assertNotNull(dataNode);
    assertTrue(dataNode.has(INGESTION_METHOD_OPT));
    assertTrue(dataNode.get(INGESTION_METHOD_OPT).asInt() == ingestionMethodConfig.ordinal());
    assertTrue(
        dataNode.get("time").asLong() <= System.currentTimeMillis()
            && dataNode.get("time").asLong() >= this.startTime);
    assertEquals(dataNode.get("error_number").asText(), expectedException);
  }

  @ParameterizedTest
  @EnumSource(value = IngestionMethodConfig.class)
  public void testReportKafkaPartitionUsage(IngestionMethodConfig ingestionMethodConfig) {
    // given
    Map<String, String> connectorConfig = createConnectorConfig(ingestionMethodConfig);
    SnowflakeTelemetryService snowflakeTelemetryService =
        createSnowflakeTelemetryService(ingestionMethodConfig, connectorConfig);

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

    if (ingestionMethodConfig == IngestionMethodConfig.SNOWPIPE) {
      SnowflakeTelemetryPipeStatus pipeStatus =
          new SnowflakeTelemetryPipeStatus(
              expectedTableName, expectedStageName, expectedPipeName, 0, false, null);
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

    // when
    snowflakeTelemetryService.reportKafkaPartitionUsage(partitionUsage, false);

    // then
    LinkedList<TelemetryData> sentData = this.mockTelemetryClient.getSentTelemetryData();
    assertEquals(1, sentData.size());

    JsonNode allNode = sentData.get(0).getMessage();
    assertEquals("kafka_connector", allNode.get("source").asText());
    assertEquals(Utils.VERSION, allNode.get("version").asText());

    JsonNode dataNode = allNode.get("data");
    assertNotNull(dataNode);
    assertTrue(dataNode.has(INGESTION_METHOD_OPT));
    assertTrue(dataNode.get(INGESTION_METHOD_OPT).asInt() == ingestionMethodConfig.ordinal());
    assertEquals(
        expectedProcessedOffset, dataNode.get(TelemetryConstants.PROCESSED_OFFSET).asLong());
    assertEquals(expectedTableName, dataNode.get(TelemetryConstants.TABLE_NAME).asText());

    if (ingestionMethodConfig == IngestionMethodConfig.SNOWPIPE) {
      assertTrue(
          dataNode.get(TelemetryConstants.START_TIME).asLong() <= System.currentTimeMillis()
              && dataNode.get(TelemetryConstants.START_TIME).asLong() >= this.startTime);
      assertEquals(
          SnowflakeTelemetryService.TelemetryType.KAFKA_PIPE_USAGE.toString(),
          allNode.get("type").asText());
      assertEquals(expectedFlushedOffset, dataNode.get(TelemetryConstants.FLUSHED_OFFSET).asLong());
      assertEquals(
          expectedCommittedOffset, dataNode.get(TelemetryConstants.COMMITTED_OFFSET).asLong());
      assertEquals(expectedPipeName, dataNode.get(TelemetryConstants.PIPE_NAME).asText());
      assertEquals(expectedStageName, dataNode.get(TelemetryConstants.STAGE_NAME).asText());
    } else {
      assertTrue(
          dataNode.get(TelemetryConstants.TOPIC_PARTITION_CHANNEL_CREATION_TIME).asLong()
              == expectedTpChannelCreationTime);
      assertTrue(
          dataNode.get(TelemetryConstants.TOPIC_PARTITION_CHANNEL_CLOSE_TIME).asLong()
                  <= System.currentTimeMillis()
              && dataNode.get(TelemetryConstants.TOPIC_PARTITION_CHANNEL_CLOSE_TIME).asLong()
                  >= this.startTime);
      assertEquals(
          SnowflakeTelemetryService.TelemetryType.KAFKA_CHANNEL_USAGE.toString(),
          allNode.get("type").asText());
      assertEquals(
          expectedLatestConsumerOffset,
          dataNode.get(TelemetryConstants.LATEST_CONSUMER_OFFSET).asLong());
      assertEquals(
          expectedOffsetPersistedInSnowflake,
          dataNode.get(TelemetryConstants.OFFSET_PERSISTED_IN_SNOWFLAKE).asLong());
      assertEquals(
          expectedTpChannelName,
          dataNode.get(TelemetryConstants.TOPIC_PARTITION_CHANNEL_NAME).asText());
      assertEquals(expectedConnectorName, dataNode.get(TelemetryConstants.CONNECTOR_NAME).asText());
    }
  }

  @ParameterizedTest
  @EnumSource(value = IngestionMethodConfig.class)
  public void testReportKafkaPartitionStart(IngestionMethodConfig ingestionMethodConfig) {
    // given
    Map<String, String> connectorConfig = createConnectorConfig(ingestionMethodConfig);
    SnowflakeTelemetryService snowflakeTelemetryService =
        createSnowflakeTelemetryService(ingestionMethodConfig, connectorConfig);

    SnowflakeTelemetryBasicInfo partitionCreation;
    final String expectedTableName = "tableName";
    final String expectedStageName = "stageName";
    final String expectedPipeName = "pipeName";
    final String expectedChannelName = "channelName";
    final long expectedChannelCreationTime = 1234;

    if (ingestionMethodConfig == IngestionMethodConfig.SNOWPIPE) {
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

    // when
    snowflakeTelemetryService.reportKafkaPartitionStart(partitionCreation);

    // then
    LinkedList<TelemetryData> sentData = this.mockTelemetryClient.getSentTelemetryData();
    assertEquals(1, sentData.size());

    JsonNode allNode = sentData.get(0).getMessage();
    assertEquals("kafka_connector", allNode.get("source").asText());
    assertEquals(Utils.VERSION, allNode.get("version").asText());

    JsonNode dataNode = allNode.get("data");
    assertNotNull(dataNode);
    assertTrue(dataNode.has(INGESTION_METHOD_OPT));
    assertTrue(dataNode.get(INGESTION_METHOD_OPT).asInt() == ingestionMethodConfig.ordinal());
    assertTrue(dataNode.get(TelemetryConstants.IS_REUSE_TABLE).asBoolean());
    assertEquals(expectedTableName, dataNode.get(TelemetryConstants.TABLE_NAME).asText());

    if (ingestionMethodConfig == IngestionMethodConfig.SNOWPIPE) {
      assertTrue(
          dataNode.get(TelemetryConstants.START_TIME).asLong() <= System.currentTimeMillis()
              && dataNode.get(TelemetryConstants.START_TIME).asLong() >= this.startTime);
      assertEquals(
          SnowflakeTelemetryService.TelemetryType.KAFKA_PIPE_START.toString(),
          allNode.get("type").asText());
      assertTrue(dataNode.get(TelemetryConstants.IS_REUSE_PIPE).asBoolean());
      assertTrue(dataNode.get(TelemetryConstants.IS_REUSE_STAGE).asBoolean());
      assertEquals(10, dataNode.get(TelemetryConstants.FILE_COUNT_REPROCESS_PURGE).asInt());
      assertEquals(11, dataNode.get(TelemetryConstants.FILE_COUNT_RESTART).asInt());
      assertEquals(expectedStageName, dataNode.get(TelemetryConstants.STAGE_NAME).asText());
      assertEquals(expectedPipeName, dataNode.get(TelemetryConstants.PIPE_NAME).asText());
    } else {
      assertTrue(
          dataNode.get(TelemetryConstants.TOPIC_PARTITION_CHANNEL_CREATION_TIME).asLong()
              == expectedChannelCreationTime);
      assertEquals(
          SnowflakeTelemetryService.TelemetryType.KAFKA_CHANNEL_START.toString(),
          allNode.get("type").asText());
      assertEquals(
          expectedChannelName,
          dataNode.get(TelemetryConstants.TOPIC_PARTITION_CHANNEL_NAME).asText());
    }
  }

  private Map<String, String> createConnectorConfig(IngestionMethodConfig ingestionMethodConfig) {
    if (ingestionMethodConfig == IngestionMethodConfig.SNOWPIPE) {
      return TestUtils.getConfig();
    } else {
      return TestUtils.getConfForStreaming();
    }
  }

  private SnowflakeTelemetryService createSnowflakeTelemetryService(
      IngestionMethodConfig ingestionMethodConfig, Map<String, String> connectorConfig) {
    SnowflakeTelemetryService snowflakeTelemetryService;

    if (ingestionMethodConfig == IngestionMethodConfig.SNOWPIPE) {
      snowflakeTelemetryService = new SnowflakeTelemetryServiceV1(mockTelemetryClient);
    } else {
      snowflakeTelemetryService = new SnowflakeTelemetryServiceV2(mockTelemetryClient);
      SnowflakeSinkConnectorConfig.setDefaultValues(connectorConfig);
    }

    snowflakeTelemetryService.setAppName("TEST_APP");
    snowflakeTelemetryService.setTaskID("1");

    return snowflakeTelemetryService;
  }

  private String sentTelemetryDataField(String field) {
    LinkedList<TelemetryData> sentData = this.mockTelemetryClient.getSentTelemetryData();
    assertEquals(1, sentData.size());
    JsonNode allNode = sentData.get(0).getMessage();
    return allNode.get("data").get(field).asText();
  }

  private void validateKeyAndValueConverter(JsonNode dataNode) {
    assertTrue(dataNode.has(KEY_CONVERTER_CONFIG_FIELD));
    assertTrue(
        dataNode.get(KEY_CONVERTER_CONFIG_FIELD).asText().equalsIgnoreCase(KAFKA_STRING_CONVERTER));

    assertTrue(dataNode.has(VALUE_CONVERTER_CONFIG_FIELD));
    assertTrue(
        dataNode
            .get(VALUE_CONVERTER_CONFIG_FIELD)
            .asText()
            .equalsIgnoreCase(KAFKA_CONFLUENT_AVRO_CONVERTER));
  }

  private void validateBufferProperties(JsonNode dataNode) {
    assertTrue(dataNode.has(BUFFER_SIZE_BYTES));
    assertTrue(isNumeric(dataNode.get(BUFFER_SIZE_BYTES).asText()));

    assertTrue(dataNode.has(BUFFER_COUNT_RECORDS));
    assertTrue(isNumeric(dataNode.get(BUFFER_COUNT_RECORDS).asText()));

    assertTrue(dataNode.has(BUFFER_FLUSH_TIME_SEC));
    assertTrue(isNumeric(dataNode.get(BUFFER_FLUSH_TIME_SEC).asText()));
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
