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
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryChannelStatus;
import com.snowflake.kafka.connector.internal.streaming.telemetry.SnowflakeTelemetryServiceV2;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
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
