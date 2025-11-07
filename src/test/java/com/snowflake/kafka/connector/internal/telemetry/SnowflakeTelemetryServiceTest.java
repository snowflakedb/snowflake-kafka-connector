package com.snowflake.kafka.connector.internal.telemetry;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.KEY_CONVERTER_CONFIG_FIELD;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD;
import static com.snowflake.kafka.connector.internal.telemetry.SnowflakeTelemetryService.INGESTION_METHOD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

public class SnowflakeTelemetryServiceTest {

  public static final String KAFKA_STRING_CONVERTER =
      "org.apache.kafka.connect.storage.StringConverter";
  public static final String KAFKA_CONFLUENT_AVRO_CONVERTER =
      "io.confluent.connect.avro.AvroConverter";
  private long startTime;
  private MockTelemetryClient mockTelemetryClient;

  @BeforeEach
  void setUp() {
    this.startTime = System.currentTimeMillis();
    this.mockTelemetryClient = new MockTelemetryClient();
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

    assertEquals(ingestionMethodConfig.toString(), sentTelemetryDataField(INGESTION_METHOD));

    JsonNode dataNode = allNode.get("data");
    assertTrue(
        dataNode.get(TelemetryConstants.START_TIME).asLong() <= System.currentTimeMillis()
            && dataNode.get(TelemetryConstants.START_TIME).asLong() >= this.startTime);

    assertNotNull(dataNode.get("jdk_version"));
    assertNotNull(dataNode.get("jdk_distribution"));

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
    assertTrue(dataNode.has(INGESTION_METHOD));
    assertEquals(dataNode.get(INGESTION_METHOD).asInt(), ingestionMethodConfig.ordinal());
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
    assertTrue(dataNode.has(INGESTION_METHOD));
    assertEquals(dataNode.get(INGESTION_METHOD).asInt(), ingestionMethodConfig.ordinal());
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
    assertTrue(dataNode.has(INGESTION_METHOD));
    assertEquals(dataNode.get(INGESTION_METHOD).asInt(), ingestionMethodConfig.ordinal());
    assertEquals(
        expectedProcessedOffset, dataNode.get(TelemetryConstants.PROCESSED_OFFSET).asLong());
    assertEquals(expectedTableName, dataNode.get(TelemetryConstants.TABLE_NAME).asText());

    assertEquals(
        expectedTpChannelCreationTime,
        dataNode.get(TelemetryConstants.TOPIC_PARTITION_CHANNEL_CREATION_TIME).asLong());
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

    SnowflakeTelemetryChannelCreation channelCreation =
        new SnowflakeTelemetryChannelCreation(
            expectedTableName, expectedChannelName, expectedChannelCreationTime);
    channelCreation.setReuseTable(true);

    partitionCreation = channelCreation;

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
    assertTrue(dataNode.has(INGESTION_METHOD));
    assertEquals(dataNode.get(INGESTION_METHOD).asInt(), ingestionMethodConfig.ordinal());
    assertTrue(dataNode.get(TelemetryConstants.IS_REUSE_TABLE).asBoolean());
    assertEquals(expectedTableName, dataNode.get(TelemetryConstants.TABLE_NAME).asText());
    assertEquals(
        expectedChannelCreationTime,
        dataNode.get(TelemetryConstants.TOPIC_PARTITION_CHANNEL_CREATION_TIME).asLong());
    assertEquals(
        SnowflakeTelemetryService.TelemetryType.KAFKA_CHANNEL_START.toString(),
        allNode.get("type").asText());
    assertEquals(
        expectedChannelName,
        dataNode.get(TelemetryConstants.TOPIC_PARTITION_CHANNEL_NAME).asText());
  }

  private Map<String, String> createConnectorConfig(IngestionMethodConfig ingestionMethodConfig) {
    return TestUtils.getConfForStreaming();
  }

  private SnowflakeTelemetryService createSnowflakeTelemetryService(
      IngestionMethodConfig ingestionMethodConfig, Map<String, String> connectorConfig) {
    SnowflakeTelemetryService snowflakeTelemetryService;

    snowflakeTelemetryService = new SnowflakeTelemetryServiceV2(mockTelemetryClient);
    SnowflakeSinkConnectorConfig.setDefaultValues(connectorConfig);

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

  public static class MockTelemetryClient implements Telemetry {

    private final LinkedList<TelemetryData> telemetryDataList;

    private final LinkedList<TelemetryData> sentTelemetryData;

    private final ExecutorService executor = Executors.newSingleThreadExecutor();

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
