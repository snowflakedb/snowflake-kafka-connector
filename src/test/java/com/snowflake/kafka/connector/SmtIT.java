package com.snowflake.kafka.connector;

import static org.apache.kafka.connect.runtime.ConnectorConfig.TRANSFORMS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.snowflake.kafka.connector.internal.TestUtils;
import java.time.Duration;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.apache.kafka.connect.json.JsonConverter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class SmtIT extends ConnectClusterBaseIT {

  private String smtTopic;
  private String smtConnector;

  @BeforeEach
  void before() {
    smtTopic = TestUtils.randomTableName();
    smtConnector = String.format("%s_connector", smtTopic);
    connectCluster.kafka().createTopic(smtTopic);
  }

  @AfterEach
  void after() {
    connectCluster.kafka().deleteTopic(smtTopic);
    connectCluster.deleteConnector(smtConnector);
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
              assertThat(fakeStreamingClientHandler.ingestedRows()).hasSize(expectedRecordNumber);
              assertThat(fakeStreamingClientHandler.getLatestCommittedOffsetTokensPerChannel())
                  .hasSize(1)
                  .containsValue("19");
            });
  }
}
