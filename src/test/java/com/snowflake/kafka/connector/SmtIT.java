package com.snowflake.kafka.connector;

import static org.apache.kafka.connect.runtime.ConnectorConfig.TRANSFORMS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.FakeSnowflakeStreamingIngestChannel;
import com.snowflake.kafka.connector.internal.streaming.v2.StreamingClientManager;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@ExtendWith({InjectSnowflakeDataSourceExtension.class, InjectQueryRunnerExtension.class})
public class SmtIT extends ConnectClusterBaseIT {

  private static final int PARTITION_COUNT = 1;
  public static final String RECORD_METADATA = "RECORD_METADATA";
  public static final String RECORD_CONTENT = "record_content";
  private String topicName;
  private String connectorName;
  private String tableName;
  private ObjectMapper objectMapper = new ObjectMapper();

  @InjectQueryRunner private QueryRunner queryRunner;

  @BeforeEach
  void before() {
    topicName = TestUtils.randomTableName();
    tableName = topicName;
    connectorName = String.format("%s_connector", topicName);
    connectCluster.kafka().createTopic(topicName, PARTITION_COUNT);
    TestUtils.getConnectionServiceWithEncryptedKey().createTable(topicName);
    StreamingClientManager.setIngestClientSupplier(fakeClientSupplier);
  }

  @AfterEach
  void after() {
    connectCluster.kafka().deleteTopic(topicName);
    connectCluster.deleteConnector(connectorName);
    StreamingClientManager.resetIngestClientSupplier();
    TestUtils.dropTable(topicName);
  }

  @Test
  void test_with_record_content_variant_added_by_smt() throws Exception {
    final Map<String, String> config = defaultProperties(topicName, connectorName);
    config.put("transforms", "add_record_content");
    config.put(
        "transforms.add_record_content.type",
        "org.apache.kafka.connect.transforms.HoistField$Value");
    config.put("transforms.add_record_content.field", RECORD_CONTENT);

    connectCluster.configureConnector(connectorName, config);

    waitForConnectorRunning(connectorName);
    waitForOpenedFakeIngestClient(connectorName);
    connectCluster.kafka().produce(topicName, getTestJsonContent());

    // then
    await()
        .timeout(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              assertThat(getOpenedFakeIngestClient(connectorName).getAppendedRowCount())
                  .isEqualTo(1);
              // get first open channel, there is going to be only one because partition count is 1
              FakeSnowflakeStreamingIngestChannel openedChannels =
                  getOpenedFakeIngestClient(connectorName).getOpenedChannels().get(0);

              assertThat(openedChannels.getAppendedRows()).hasSize(1);
              final Map<String, Object> firstRow = openedChannels.getAppendedRows().get(0);
              assertThat(firstRow).containsKeys(RECORD_METADATA, RECORD_CONTENT);
              assertThat(firstRow)
                  .hasEntrySatisfying(
                      RECORD_METADATA,
                      value -> {
                        assertThat(value).isInstanceOf(Map.class);
                      });
              assertThat(firstRow)
                  .hasEntrySatisfying(
                      RECORD_CONTENT,
                      value -> {
                        assertThat(value).isInstanceOf(Map.class);
                      });
            });
  }

  @ParameterizedTest
  @CsvSource({"DEFAULT, 20", "IGNORE, 10"})
  void testIfSmtReturningNullsIngestDataCorrectly(String behaviorOnNull, int expectedRecordNumber) {
    // given
    connectCluster.configureConnector(
        connectorName, smtProperties(topicName, connectorName, behaviorOnNull));
    waitForConnectorRunning(connectorName);
    waitForOpenedFakeIngestClient(connectorName);

    // when
    Stream.iterate(0, UnaryOperator.identity())
        .limit(10)
        .flatMap(v -> Stream.of("{}", "{\"message\":\"value\"}"))
        .forEach(message -> connectCluster.kafka().produce(topicName, message));

    // then
    await()
        .timeout(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              assertThat(getOpenedFakeIngestClient(connectorName).getAppendedRowCount())
                  .isEqualTo(expectedRecordNumber);
              List<FakeSnowflakeStreamingIngestChannel> openedChannels =
                  getOpenedFakeIngestClient(connectorName).getOpenedChannels();
              // get first open channel, there is going to be only one because partition count is 1
              String offsetToken = openedChannels.get(0).getLatestCommittedOffsetToken();

              assertThat(openedChannels).hasSize(PARTITION_COUNT);
              assertThat(offsetToken).isEqualTo("19");
            });
  }

  private Map<String, String> smtProperties(
      String smtTopic, String smtConnector, String behaviorOnNull) {
    Map<String, String> config = defaultProperties(smtTopic, smtConnector);

    config.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    config.put("value.converter.schemas.enable", "false");
    config.put("behavior.on.null.values", behaviorOnNull);

    config.put(TRANSFORMS_CONFIG, "extractField");
    config.put(
        "transforms.extractField.type", "org.apache.kafka.connect.transforms.ExtractField$Value");
    config.put("transforms.extractField.field", "message");

    return config;
  }

  private String getTestJsonContent() throws JsonProcessingException {
    return objectMapper.writeValueAsString(
        Map.of(
            "city",
            "Pcim Górny",
            "age",
            30,
            "married",
            true,
            "has cat",
            true,
            "! @&$#* has Łułósżź",
            true,
            "skills",
            List.of("sitting", "standing", "eating"),
            "family",
            Map.of("son", "Jack", "daughter", "Anna")));
  }
}
