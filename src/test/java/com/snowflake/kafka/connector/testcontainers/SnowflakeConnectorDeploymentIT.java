package com.snowflake.kafka.connector.testcontainers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.SnowflakeSinkConnector;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.WaitingConsumer;

import java.io.File;
import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;


@ExtendWith(KafkaTestcontainersExtension.class)
class SnowflakeConnectorDeploymentIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnowflakeConnectorDeploymentIT.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String PROFILE_PATH = "profile.json";

    @InjectKafkaEnvironment
    private KafkaTestEnvironment kafkaEcosystem;

    private String connectorName;
    private String topicName;
    private Map<String, String> connectorConfig;
    private KafkaConnectApiClient kafkaConnectClient;
    private WaitingConsumer waitingConsumer;

    @BeforeEach
    void setup() throws IOException {
        connectorName = "snowflake-test-connector-" + System.currentTimeMillis();
        topicName = "test-topic-" + System.currentTimeMillis();
        connectorConfig = buildConnectorConfig(topicName);
        kafkaConnectClient = new KafkaConnectApiClient(kafkaEcosystem.getConnectUrl(), HttpClient.newHttpClient());
        waitingConsumer = kafkaEcosystem.getKafkaConnectWaitingConsumer();

    }

    @AfterEach
    void cleanup() {
        kafkaConnectClient.deleteConnector(connectorName);
        connectorName = null;
    }

    @Test
    void test_connector_plugin_is_available_in_kafka_connect() throws Exception {
        final KafkaConnectPlugin[] plugins = kafkaConnectClient.getConnectorPlugins();
        assertThat(plugins).anyMatch(plugin -> plugin.getClazz().equals(SnowflakeSinkConnector.class.getName()));
    }

    @Test
    void test_connector_starts_successfully_with_use_user_defined_database_objects_option() throws Exception {
        connectorConfig.put(SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_USE_USER_DEFINED_DATABASE_OBJECTS, "true");
        kafkaConnectClient.createConnector(connectorName, connectorConfig);
        // when
        produceMessage(Map.of("id", "1", "name", "Zenon Żelazko")); // connector needs a message to trigger startup
        // then
        waitingConsumer.waitUntil(frame -> frame.getUtf8String().contains("SnowflakeSinkTask task open success for topic " + topicName), 30, TimeUnit.SECONDS);
    }

    private void produceMessage(Map<String, String> message) throws Exception {
        try (KafkaProducer<String, String> producer = kafkaEcosystem.createKafkaProducer()) {
            final String jsonPayload = OBJECT_MAPPER.writeValueAsString(message);
            final ProducerRecord<String, String> record = new ProducerRecord<>(topicName, "key", jsonPayload);
            producer.send(record).get();
            LOGGER.info("Sent {} to topic {}: ", jsonPayload, topicName);
        }
    }

    private Map<String, String> buildConnectorConfig(final String topicName) throws IOException {
        final Map<String, String> config = loadStreamingConfigFromProfile();
        config.put("connector.class", SnowflakeSinkConnector.class.getName());
        config.put("topics", topicName);
        config.put("tasks.max", "1");
        config.put(SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT, IngestionMethodConfig.SNOWPIPE_STREAMING.name());
        config.put(SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_CLIENT_LAG, "1");
        config.put("key.converter", "org.apache.kafka.connect.storage.StringConverter");
        config.put("value.converter", "org.apache.kafka.connect.json.JsonConverter");
        config.put("value.converter.schemas.enable", "false");
        return config;
    }

    private Map<String, String> loadStreamingConfigFromProfile() throws IOException {
        final File profileFile = new File(PROFILE_PATH);
        if (!profileFile.exists()) {
            fail("profile.json not found. Please create it with Snowflake credentials.");
        }

        final JsonNode profile = OBJECT_MAPPER.readTree(profileFile);
        final Map<String, String> config = new HashMap<>();

        // Required properties
        config.put(Utils.SF_URL, getRequiredProperty(profile, "host"));
        config.put(Utils.SF_USER, getRequiredProperty(profile, "user"));
        config.put(Utils.SF_DATABASE, getRequiredProperty(profile, "database"));
        config.put(Utils.SF_SCHEMA, getRequiredProperty(profile, "schema"));
        config.put(Utils.SF_WAREHOUSE, getRequiredProperty(profile, "warehouse"));
        config.put(Utils.SF_ROLE, getRequiredProperty(profile, "role"));

        // Authentication - either private key or other authenticator
        if (profile.has("private_key")) {
            config.put(Utils.SF_PRIVATE_KEY, profile.get("private_key").asText());
        }
        return config;
    }

    private String getRequiredProperty(final JsonNode profile, final String propertyName) {
        if (!profile.has(propertyName)) {
            fail("Required property '" + propertyName + "' not found in profile.json");
        }
        return profile.get(propertyName).asText();
    }

}

