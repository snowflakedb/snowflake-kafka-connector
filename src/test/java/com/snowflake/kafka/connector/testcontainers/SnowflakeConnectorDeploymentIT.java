package com.snowflake.kafka.connector.testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.snowflake.kafka.connector.SnowflakeSinkConnector;
import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.WaitingConsumer;

/**
 * Integration test that verifies the Snowflake Kafka Connector JAR can be deployed to Kafka
 * Connect and starts successfully with streaming ingestion configuration.
 *
 * <p>This test:
 *
 * <ul>
 *   <li>Verifies the plugin is available via REST API
 *   <li>Creates a connector instance with streaming configuration from profile.json
 *   <li>Verifies the connector reaches RUNNING state
 * </ul>
 *
 * <p>Note: The connector JAR is automatically copied to Kafka Connect during container startup by
 * {@link KafkaTestcontainersExtension}, so no manual copying or restart is needed.
 */
@ExtendWith(KafkaTestcontainersExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SnowflakeConnectorDeploymentIT {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(SnowflakeConnectorDeploymentIT.class);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String PROFILE_PATH = "profile.json";
  private static final Duration CONNECTOR_STARTUP_TIMEOUT = Duration.ofSeconds(60);
  private static final Duration POLL_INTERVAL = Duration.ofSeconds(2);

  @InjectKafkaEnvironment private KafkaTestEnvironment kafkaEnv;

  private String connectorName;
  private HttpClient httpClient;

  @BeforeAll
  void setupConnector() {
    httpClient = HttpClient.newHttpClient();
    LOGGER.info("Starting Snowflake Connector deployment test");
  }

  @AfterEach
  void cleanup() {
    if (connectorName != null) {
      try {
        deleteConnector(connectorName);
        LOGGER.info("Deleted connector: {}", connectorName);
      } catch (Exception e) {
        LOGGER.warn("Failed to delete connector {}: {}", connectorName, e.getMessage());
      }
      connectorName = null;
    }
  }

  @Test
  void testConnectorPluginIsAvailable() throws Exception {
    // Given
    final String pluginsEndpoint = kafkaEnv.getConnectUrl() + "/connector-plugins";
    final HttpRequest request =
        HttpRequest.newBuilder().uri(URI.create(pluginsEndpoint)).GET().build();

    // When
    final HttpResponse<String> response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    // Then
    assertEquals(200, response.statusCode(), "Connector plugins endpoint should return 200");

    final JsonNode plugins = OBJECT_MAPPER.readTree(response.body());
    assertTrue(plugins.isArray(), "Response should be an array");

    boolean foundSnowflakeConnector = false;
    for (JsonNode plugin : plugins) {
      final String pluginClass = plugin.get("class").asText();
      if (pluginClass.equals(SnowflakeSinkConnector.class.getName())) {
        foundSnowflakeConnector = true;
        LOGGER.info("Found Snowflake connector plugin: {}", pluginClass);
        break;
      }
    }

    assertTrue(
        foundSnowflakeConnector,
        "Snowflake connector plugin should be available in Kafka Connect");
  }

  @Test
  void testConnectorStartsSuccessfully() throws Exception {
    // Given
    connectorName = "snowflake-test-connector-" + System.currentTimeMillis();
    final String topicName = "test-topic-" + System.currentTimeMillis();

    final Map<String, String> connectorConfig = buildConnectorConfig(connectorName, topicName);
    connectorConfig.put("name", connectorName);
    connectorConfig.put(SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_USE_USER_DEFINED_DATABASE_OBJECTS, "true");
    final ObjectNode configJson = OBJECT_MAPPER.createObjectNode();
    configJson.put("name", connectorName);


    final ObjectNode config = OBJECT_MAPPER.createObjectNode();
    connectorConfig.forEach(config::put);
    configJson.set("config", config);

    // When - Create connector
    final String connectorsEndpoint = kafkaEnv.getConnectUrl() + "/connectors";
    final HttpRequest createRequest =
        HttpRequest.newBuilder()
            .uri(URI.create(connectorsEndpoint))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(OBJECT_MAPPER.writeValueAsString(configJson)))
            .build();

    final HttpResponse<String> createResponse =
        httpClient.send(createRequest, HttpResponse.BodyHandlers.ofString());

    // Then - Verify connector was created
    assertEquals(
        201,
        createResponse.statusCode(),
        "Connector creation should return 201. Response: " + createResponse.body());

    LOGGER.info("Connector created: {}", connectorName);

    // Wait for connector to reach RUNNING state
    waitForConnectorRunning(connectorName);
      // When - Send multiple JSON messages to the Kafka topic
      try (KafkaProducer<String, String> producer = kafkaEnv.createKafkaProducer()) {
          for (int i = 1; i <= 5; i++) {
              final ObjectNode testMessage = OBJECT_MAPPER.createObjectNode();
              testMessage.put("id", i);
              testMessage.put("name", "test-record-" + i);
              testMessage.put("timestamp", System.currentTimeMillis());

              final ProducerRecord<String, String> record =
                  new ProducerRecord<>(
                      topicName, "key" + i, OBJECT_MAPPER.writeValueAsString(testMessage));
              producer.send(record).get();
              LOGGER.info("Sent test message {} to topic {}: {}", i, topicName, testMessage);
          }
          producer.flush();
      }

    LOGGER.info("Connector {} reached RUNNING state", connectorName);
      // Then - Assert on Kafka Connect logs
      final String connectLogs = kafkaEnv.getKafkaConnectLogs();
      WaitingConsumer waitingConsumer = kafkaEnv.getKafkaConnectWaitingConsumer();
      waitingConsumer.waitUntil( frame -> frame.getUtf8String().contains("Error Code: 5029"), 30, TimeUnit.SECONDS);


      LOGGER.info("Checking Kafka Connect logs for expected content");
      assertTrue(
          connectLogs.contains(SnowflakeSinkConnector.class.getSimpleName()),
          "Logs should mention SnowflakeSinkConnector");
  }



  private Map<String, String> buildConnectorConfig(
      final String connectorName, final String topicName) throws IOException {
    final Map<String, String> config = new HashMap<>();

    // Load configuration from profile.json
    final Map<String, String> profileConfig = loadStreamingConfigFromProfile();

    // Set connector-specific properties
    config.put("connector.class", SnowflakeSinkConnector.class.getName());
    config.put("topics", topicName);
    config.put("tasks.max", "1");

    // Add Snowflake connection properties
    config.putAll(profileConfig);

    // Add streaming-specific configuration
    config.put(
        SnowflakeSinkConnectorConfig.INGESTION_METHOD_OPT,
        IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(SnowflakeSinkConnectorConfig.SNOWPIPE_STREAMING_MAX_CLIENT_LAG, "1");

    // Add converters
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

    if (profile.has("authenticator")) {
      config.put(Utils.SF_AUTHENTICATOR, profile.get("authenticator").asText());
    }

    // OAuth properties (if present)
    if (profile.has("oauth_client_id")) {
      config.put(Utils.SF_OAUTH_CLIENT_ID, profile.get("oauth_client_id").asText());
    }

    if (profile.has("oauth_client_secret")) {
      config.put(Utils.SF_OAUTH_CLIENT_SECRET, profile.get("oauth_client_secret").asText());
    }

    if (profile.has("oauth_refresh_token")) {
      config.put(Utils.SF_OAUTH_REFRESH_TOKEN, profile.get("oauth_refresh_token").asText());
    }

    config.put(Utils.NAME, "test-connector");
    config.put(Utils.TASK_ID, "0");

    return config;
  }

  private String getRequiredProperty(final JsonNode profile, final String propertyName) {
    if (!profile.has(propertyName)) {
      fail("Required property '" + propertyName + "' not found in profile.json");
    }
    return profile.get(propertyName).asText();
  }

  private void waitForConnectorRunning(final String connectorName) throws Exception {
    final String statusEndpoint =
        kafkaEnv.getConnectUrl() + "/connectors/" + connectorName + "/status";
    final long startTime = System.currentTimeMillis();
    final long timeoutMillis = CONNECTOR_STARTUP_TIMEOUT.toMillis();

    while (System.currentTimeMillis() - startTime < timeoutMillis) {
      final HttpRequest request =
          HttpRequest.newBuilder().uri(URI.create(statusEndpoint)).GET().build();

      final HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() == 200) {
        final JsonNode status = OBJECT_MAPPER.readTree(response.body());
        final String connectorState = status.get("connector").get("state").asText();

        LOGGER.info("Connector state: {}", connectorState);

        if ("RUNNING".equals(connectorState)) {
          // Also check task state
          final JsonNode tasks = status.get("tasks");
          if (tasks.isArray() && tasks.size() > 0) {
            final String taskState = tasks.get(0).get("state").asText();
            LOGGER.info("Task state: {}", taskState);

            if ("RUNNING".equals(taskState)) {
              return;
            }
          }
        } else if ("FAILED".equals(connectorState)) {
          final String trace = status.get("connector").get("trace").asText();
          fail("Connector failed to start: " + trace);
        }
      }

      Thread.sleep(POLL_INTERVAL.toMillis());
    }

    fail(
        "Connector did not reach RUNNING state within "
            + CONNECTOR_STARTUP_TIMEOUT.getSeconds()
            + " seconds");
  }

  private void deleteConnector(final String connectorName) throws Exception {
    final String deleteEndpoint = kafkaEnv.getConnectUrl() + "/connectors/" + connectorName;
    final HttpRequest request =
        HttpRequest.newBuilder().uri(URI.create(deleteEndpoint)).DELETE().build();

    final HttpResponse<String> response =
        httpClient.send(request, HttpResponse.BodyHandlers.ofString());

    if (response.statusCode() != 204 && response.statusCode() != 404) {
      LOGGER.warn(
          "Failed to delete connector. Status: {}, Response: {}",
          response.statusCode(),
          response.body());
    }
  }
}

