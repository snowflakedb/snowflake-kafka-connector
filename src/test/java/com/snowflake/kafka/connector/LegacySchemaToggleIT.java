package com.snowflake.kafka.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.FakeSnowflakeStreamingIngestChannel;
import com.snowflake.kafka.connector.internal.streaming.v2.client.StreamingClientFactory;
import java.time.Duration;
import java.util.Map;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith({InjectSnowflakeDataSourceExtension.class, InjectQueryRunnerExtension.class})
public class LegacySchemaToggleIT extends ConnectClusterBaseIT {

  private static final int PARTITION_COUNT = 1;
  private static final String RECORD_CONTENT = "RECORD_CONTENT";
  private static final String RECORD_METADATA = "RECORD_METADATA";
  private String topicName;
  private String connectorName;
  private ObjectMapper objectMapper = new ObjectMapper();

  @InjectQueryRunner private QueryRunner queryRunner;

  @BeforeEach
  void before() {
    topicName = TestUtils.randomTableName();
    connectorName = String.format("%s_connector", topicName);
    connectCluster.kafka().createTopic(topicName, PARTITION_COUNT);
    TestUtils.getConnectionServiceWithEncryptedKey().createTableWithMetadataColumn(topicName);
    StreamingClientFactory.setStreamingClientSupplier(fakeClientSupplier);
  }

  @AfterEach
  void after() {
    connectCluster.kafka().deleteTopic(topicName);
    connectCluster.deleteConnector(connectorName);
    StreamingClientFactory.resetStreamingClientSupplier();
    TestUtils.dropTable(topicName);
    TestUtils.dropPipe(topicName + "-STREAMING");
  }

  @Test
  void test_legacyMode_jsonConverter_wrapsInRecordContent() throws Exception {
    final Map<String, String> config = defaultProperties(topicName, connectorName);
    config.put("snowflake.enable.schematization", "false");
    config.put(Constants.KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "false");

    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);
    waitForOpenedFakeIngestClient(connectorName);
    connectCluster
        .kafka()
        .produce(topicName, objectMapper.writeValueAsString(Map.of("city", "Portland", "age", 25)));

    await()
        .timeout(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              assertThat(getOpenedFakeIngestClient(connectorName).getAppendedRowCount())
                  .isEqualTo(1);
              FakeSnowflakeStreamingIngestChannel channel =
                  getOpenedFakeIngestClient(connectorName).getOpenedChannels().get(0);
              final Map<String, Object> row = channel.getAppendedRows().get(0);
              assertThat(row).containsKeys(RECORD_METADATA, RECORD_CONTENT);
              assertThat(row.get(RECORD_CONTENT)).isInstanceOf(String.class);
              String jsonString = (String) row.get(RECORD_CONTENT);
              assertThat(jsonString).contains("\"city\":\"Portland\"");
              assertThat(jsonString).contains("\"age\":25");
            });
  }

  @Test
  void test_legacyMode_stringConverter_wrapsInRecordContent() {
    final Map<String, String> config = defaultProperties(topicName, connectorName);
    config.put("snowflake.enable.schematization", "false");
    config.put(Constants.KafkaConnectorConfigParams.SNOWFLAKE_CLIENT_VALIDATION_ENABLED, "false");
    config.put("value.converter", "org.apache.kafka.connect.storage.StringConverter");

    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);
    waitForOpenedFakeIngestClient(connectorName);
    connectCluster.kafka().produce(topicName, "raw string payload");

    await()
        .timeout(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              assertThat(getOpenedFakeIngestClient(connectorName).getAppendedRowCount())
                  .isEqualTo(1);
              FakeSnowflakeStreamingIngestChannel channel =
                  getOpenedFakeIngestClient(connectorName).getOpenedChannels().get(0);
              final Map<String, Object> row = channel.getAppendedRows().get(0);
              assertThat(row).containsKeys(RECORD_METADATA, RECORD_CONTENT);
              assertThat(row.get(RECORD_CONTENT)).isEqualTo("\"raw string payload\"");
            });
  }

  @Test
  void test_legacyMode_defaultSchematization_doesNotWrap() throws Exception {
    final Map<String, String> config = defaultProperties(topicName, connectorName);

    connectCluster.configureConnector(connectorName, config);
    waitForConnectorRunning(connectorName);
    waitForOpenedFakeIngestClient(connectorName);
    connectCluster
        .kafka()
        .produce(topicName, objectMapper.writeValueAsString(Map.of("city", "Portland")));

    await()
        .timeout(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              assertThat(getOpenedFakeIngestClient(connectorName).getAppendedRowCount())
                  .isEqualTo(1);
              FakeSnowflakeStreamingIngestChannel channel =
                  getOpenedFakeIngestClient(connectorName).getOpenedChannels().get(0);
              final Map<String, Object> row = channel.getAppendedRows().get(0);
              assertThat(row).containsKey("city");
              assertThat(row).doesNotContainKey(RECORD_CONTENT);
            });
  }
}
