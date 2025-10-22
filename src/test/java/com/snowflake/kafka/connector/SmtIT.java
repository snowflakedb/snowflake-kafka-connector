package com.snowflake.kafka.connector;

import static org.apache.kafka.connect.runtime.ConnectorConfig.TRANSFORMS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.FakeSnowflakeStreamingIngestChannel;
import com.snowflake.kafka.connector.internal.streaming.v2.StreamingIngestClientProvider;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/** Integration test for Single Message Transformations (SMT) with null value handling. */
public class SmtIT extends ConnectClusterBaseIT {

  private String smtTopic;
  private String smtConnector;
  private static final int PARTITION_COUNT = 1;

  @BeforeEach
  void before() {
    smtTopic = TestUtils.randomTableName();
    smtConnector = String.format("%s_connector", smtTopic);
    connectCluster.kafka().createTopic(smtTopic, PARTITION_COUNT);
    StreamingIngestClientProvider.setIngestClientSupplier(fakeClientSupplier);
  }

  @AfterEach
  void after() {
    connectCluster.kafka().deleteTopic(smtTopic);
    connectCluster.deleteConnector(smtConnector);
    StreamingIngestClientProvider.resetIngestClientSupplier();
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

  @ParameterizedTest
  @CsvSource({"DEFAULT, 20", "IGNORE, 10"})
  void testIfSmtReturningNullsIngestDataCorrectly(String behaviorOnNull, int expectedRecordNumber) {
    // given
    connectCluster.configureConnector(
        smtConnector, smtProperties(smtTopic, smtConnector, behaviorOnNull));
    waitForConnectorRunning(smtConnector);
    waitForOpenedFakeIngestClient(smtConnector);

    // when
    Stream.iterate(0, UnaryOperator.identity())
        .limit(10)
        .flatMap(v -> Stream.of("{}", "{\"message\":\"value\"}"))
        .forEach(message -> connectCluster.kafka().produce(smtTopic, message));

    // then
    await()
        .timeout(Duration.ofSeconds(30))
        .pollInterval(Duration.ofSeconds(1))
        .untilAsserted(
            () -> {
              assertThat(getOpenedFakeIngestClient(smtConnector).getAppendedRowCount())
                  .isEqualTo(expectedRecordNumber);
              Set<FakeSnowflakeStreamingIngestChannel> openedChannels =
                  getOpenedFakeIngestClient(smtConnector).getOpenedChannels();
              // get first open channel, there is going to be only one because partition count is 1
              String offsetToken =
                  openedChannels.stream().findFirst().orElseThrow().getLatestCommittedOffsetToken();

              assertThat(openedChannels).hasSize(PARTITION_COUNT);
              assertThat(offsetToken).isEqualTo("19");
            });
  }
}
