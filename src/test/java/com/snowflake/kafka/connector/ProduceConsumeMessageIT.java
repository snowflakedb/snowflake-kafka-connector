package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.fake.SnowflakeFakeSinkTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class ProduceConsumeMessageIT extends ConnectClusterBaseIT {

    private static final String TEST_TOPIC = "kafka-int-test";
    private static final String TEST_CONNECTOR_NAME = "test-connector";

    @BeforeEach
    void createConnector() {
        connectCluster.kafka().createTopic(TEST_TOPIC);
        connectCluster.configureConnector(TEST_CONNECTOR_NAME, defaultProperties(TEST_TOPIC, TEST_CONNECTOR_NAME));
        await().timeout(CONNECTOR_MAX_STARTUP_TIME).until(isConnectorRunning(TEST_CONNECTOR_NAME));
    }

    @AfterEach
    void deleteConnector() {
        connectCluster.deleteConnector(TEST_CONNECTOR_NAME);
        connectCluster.kafka().deleteTopic(TEST_TOPIC);
    }

    @Test
    public void connectorShouldConsumeMessagesFromTopic() {
        connectCluster.kafka().produce(TEST_TOPIC, "test1");
        connectCluster.kafka().produce(TEST_TOPIC, "test2");

        await()
                .untilAsserted(
                        () -> {
                            List<SinkRecord> records = SnowflakeFakeSinkTask.getRecords();
                            assertThat(records).hasSize(2);
                            assertThat(records.stream().map(SinkRecord::value)).containsExactly("test1", "test2");
                        });
    }
}
