package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.fake.SnowflakeFakeSinkTask;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.NAME;
import static org.apache.kafka.connect.runtime.ConnectorConfig.TRANSFORMS_CONFIG;
import static org.apache.kafka.connect.runtime.ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG;
import static org.apache.kafka.connect.sink.SinkConnector.TOPICS_CONFIG;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class SmtIT extends ConnectClusterBaseIT {

    private static final String SMT_TOPIC = "smtTopic";
    private static final String SMT_CONNECTOR = "smtConnector";

    @BeforeAll
    public void createConnector() {
        connectCluster.kafka().createTopic(SMT_TOPIC);
        connectCluster.configureConnector(SMT_CONNECTOR, smtProperties());
        await().timeout(CONNECTOR_MAX_STARTUP_TIME).until(isConnectorRunning(SMT_CONNECTOR));
    }

    @AfterAll
    public void deleteConnector() {
        connectCluster.deleteConnector(SMT_CONNECTOR);
        connectCluster.kafka().deleteTopic(SMT_TOPIC);
    }

    private Map<String, String> smtProperties() {
        Map<String, String> config = defaultProperties(SMT_TOPIC, SMT_CONNECTOR);

        config.put(NAME, SMT_CONNECTOR);
        config.put(TOPICS_CONFIG, SMT_TOPIC);

        config.put(VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
        config.put("value.converter.schemas.enable", "false");

        config.put(TRANSFORMS_CONFIG, "extractField");
        config.put("transforms.extractField.type", "org.apache.kafka.connect.transforms.ExtractField$Value");
        config.put("transforms.extractField.field", "message");

        return config;
    }

    @Test
    void testIfSmtRetuningNullsIngestDataCorrectly() {
        Stream.iterate(0, UnaryOperator.identity())
                .limit(10)
                .flatMap(v -> Stream.of("{}", "{\"message\":\"value\"}"))
                .forEach(message -> connectCluster.kafka().produce(SMT_TOPIC, message));

        await()
                .untilAsserted(
                        () -> {
                            List<SinkRecord> records = SnowflakeFakeSinkTask.getRecords();
                            assertThat(records)
                                    .hasSize(20)
                                    .allMatch(r -> r.originalTopic().equals(SMT_TOPIC));
                        });

    }

}
