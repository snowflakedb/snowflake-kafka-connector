package com.snowflake.kafka.connector.internal.telemetry;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.KEY_CONVERTER_CONFIG_FIELD;
import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.VALUE_CONVERTER_CONFIG_FIELD;
import static com.snowflake.kafka.connector.internal.TestUtils.getConfig;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.JsonNode;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.node.ObjectNode;
import net.snowflake.client.jdbc.telemetry.Telemetry;
import net.snowflake.client.jdbc.telemetry.TelemetryData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class SnowflakeTelemetryServiceV1Test {

  MockTelemetryClient mockTelemetryClient;

  SnowflakeTelemetryServiceV1 snowflakeTelemetryServiceV1;

  public static final String KAFKA_STRING_CONVERTER =
      "org.apache.kafka.connect.storage.StringConverter";
  public static final String KAFKA_CONFLUENT_AVRO_CONVERTER =
      "io.confluent.connect.avro.AvroConverter";

  @Before
  public void beforeEachTest() {
    mockTelemetryClient = new MockTelemetryClient();
    snowflakeTelemetryServiceV1 = new SnowflakeTelemetryServiceV1(mockTelemetryClient);
    snowflakeTelemetryServiceV1.setAppName("TEST_APP");

    snowflakeTelemetryServiceV1.setTaskID("1");
  }

  @Test
  public void testReportKafkaStartSnowpipeAtleastOnce() {

    Map<String, String> userProvidedConfig = getConfig();
    userProvidedConfig.put(INGESTION_METHOD_OPT, IngestionMethodConfig.SNOWPIPE.toString());
    addKeyAndValueConvertersToConfigMap(userProvidedConfig);

    snowflakeTelemetryServiceV1.reportKafkaConnectStart(
        System.currentTimeMillis(), userProvidedConfig);

    Assert.assertEquals(1, mockTelemetryClient.getSentTelemetryData().size());

    TelemetryData kafkaStartTelemetryData = mockTelemetryClient.getSentTelemetryData().get(0);

    ObjectNode messageSent = kafkaStartTelemetryData.getMessage();

    JsonNode dataNode = messageSent.get("data");

    Assert.assertNotNull(dataNode);
    Assert.assertTrue(dataNode.has(INGESTION_METHOD_OPT));
    Assert.assertTrue(
        dataNode
            .get(INGESTION_METHOD_OPT)
            .asText()
            .equalsIgnoreCase(IngestionMethodConfig.SNOWPIPE.toString()));

    validateBufferProperties(dataNode);

    validateKeyAndValueConverter(dataNode);
  }

  @Test
  public void testReportKafkaStartSnowpipeStreaming() {

    Map<String, String> userProvidedConfig = getConfig();
    userProvidedConfig.put(
        INGESTION_METHOD_OPT, IngestionMethodConfig.SNOWPIPE_STREAMING.toString());

    addKeyAndValueConvertersToConfigMap(userProvidedConfig);

    snowflakeTelemetryServiceV1.reportKafkaConnectStart(
        System.currentTimeMillis(), userProvidedConfig);

    Assert.assertEquals(1, mockTelemetryClient.getSentTelemetryData().size());

    TelemetryData kafkaStartTelemetryData = mockTelemetryClient.getSentTelemetryData().get(0);

    ObjectNode messageSent = kafkaStartTelemetryData.getMessage();

    JsonNode dataNode = messageSent.get("data");

    Assert.assertNotNull(dataNode);
    Assert.assertTrue(dataNode.has(INGESTION_METHOD_OPT));
    Assert.assertTrue(
        dataNode
            .get(INGESTION_METHOD_OPT)
            .asText()
            .equalsIgnoreCase(IngestionMethodConfig.SNOWPIPE_STREAMING.toString()));

    validateBufferProperties(dataNode);

    validateKeyAndValueConverter(dataNode);
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

  private static class MockTelemetryClient implements Telemetry {

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
