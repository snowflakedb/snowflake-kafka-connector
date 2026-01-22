package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceFactory;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.FakeIngestClientSupplier;
import com.snowflake.kafka.connector.internal.streaming.FakeSnowflakeStreamingIngestClient;
import com.snowflake.kafka.connector.internal.streaming.v2.StreamingClientManager;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.storage.StringConverter;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;

/** Base class for integration tests using an embedded Kafka Connect cluster. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class ConnectClusterBaseIT {

  static final String MOCK_SCHEMA_REGISTRY_URL = "mock://test-schema-registry";
  static final String SCHEMA_REGISTRY_SCOPE = "test-schema-registry";
  static final int PARTITION_COUNT = 1;
  static final int RECORD_COUNT = 100;
  static final int TOPIC_COUNT = 2;
  static final Integer TASK_NUMBER = 1;

  protected final FakeIngestClientSupplier fakeClientSupplier = new FakeIngestClientSupplier();
  protected final ObjectMapper objectMapper = new ObjectMapper();

  protected String tableName;
  protected String connectorName;
  protected String topic0;
  protected String topic1;
  protected EmbeddedConnectCluster connectCluster;
  protected SnowflakeConnectionService snowflake;

  @BeforeAll
  public void beforeAll() {
    Map<String, String> workerConfig = new HashMap<>();
    workerConfig.put("plugin.discovery", "hybrid_warn");
    // this parameter decides how often preCommit is called on the task
    workerConfig.put("offset.flush.interval.ms", "5000");

    connectCluster =
        new EmbeddedConnectCluster.Builder()
            .name("kafka-push-connector-connect-cluster")
            .numWorkers(1)
            .workerProps(workerConfig)
            .build();
    connectCluster.start();
  }

  @AfterAll
  public void afterAll() {
    if (connectCluster != null) {
      connectCluster.stop();
      connectCluster = null;
    }
  }

  @BeforeEach
  void before() {

    tableName = TestUtils.randomTableName();
    connectorName = String.format("%s_connector", tableName);
    topic0 = tableName + "0";
    topic1 = tableName + "1";
    connectCluster.kafka().createTopic(topic0, PARTITION_COUNT);
    connectCluster.kafka().createTopic(topic1, PARTITION_COUNT);
    snowflake =
        SnowflakeConnectionServiceFactory.builder()
            .setProperties(TestUtils.transformProfileFileToConnectorConfiguration(false))
            .noCaching()
            .build();

    StreamingClientManager.resetIngestClientSupplier();
  }

  @AfterEach
  void after() {
    connectCluster.kafka().deleteTopic(topic0);
    connectCluster.kafka().deleteTopic(topic1);
    connectCluster.deleteConnector(connectorName);
    StreamingClientManager.resetIngestClientSupplier();
    TestUtils.dropTable(tableName);
    MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
  }

  protected FakeSnowflakeStreamingIngestClient getOpenedFakeIngestClient(String connectorName) {
    await("channelsCreated")
        .atMost(Duration.ofSeconds(60))
        .ignoreExceptions()
        .until(
            () ->
                !getFakeSnowflakeStreamingIngestClient(connectorName)
                    .getOpenedChannels()
                    .isEmpty());

    return getFakeSnowflakeStreamingIngestClient(connectorName);
  }

  protected void waitForOpenedFakeIngestClient(String connectorName) {
    getOpenedFakeIngestClient(connectorName);
  }

  protected final Map<String, String> defaultProperties(String topicName, String connectorName) {
    Map<String, String> config = TestUtils.transformProfileFileToConnectorConfiguration(false);

    config.put(SinkConnector.TOPICS_CONFIG, topicName);
    config.put(
        ConnectorConfig.CONNECTOR_CLASS_CONFIG, SnowflakeStreamingSinkConnector.class.getName());
    config.put(ConnectorConfig.TASKS_MAX_CONFIG, TASK_NUMBER.toString());
    config.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    config.put(ConnectorConfig.VALUE_CONVERTER_CLASS_CONFIG, JsonConverter.class.getName());
    config.put(KafkaConnectorConfigParams.NAME, connectorName);
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_STREAMING_MAX_CLIENT_LAG, "1");
    config.put(KafkaConnectorConfigParams.VALUE_CONVERTER_SCHEMAS_ENABLE, "false");

    return config;
  }

  protected final void waitForConnectorRunning(String connectorName) {
    try {
      connectCluster
          .assertions()
          .assertConnectorAndAtLeastNumTasksAreRunning(
              connectorName, 1, "The connector did not start.");
    } catch (InterruptedException e) {
      throw new IllegalStateException("The connector is not running");
    }
  }

  protected final void waitForConnectorDoesNotExist(String connectorName) {
    try {
      connectCluster
          .assertions()
          .assertConnectorDoesNotExist(connectorName, "Failed to stop the connector");
    } catch (InterruptedException e) {
      throw new IllegalStateException("The connector is not running");
    }
  }

  protected final void waitForConnectorStopped(String connectorName) {
    try {
      connectCluster
          .assertions()
          .assertConnectorIsStopped(connectorName, "Connector should be stopped");
    } catch (InterruptedException e) {
      throw new IllegalStateException("The connector is not running");
    }
  }

  protected KafkaProducer<String, Object> createAvroProducer() {
    final Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, connectCluster.kafka().bootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
    props.put("schema.registry.url", MOCK_SCHEMA_REGISTRY_URL);
    return new KafkaProducer<>(props, new StringSerializer(), createAvroSerializer());
  }

  protected KafkaAvroSerializer createAvroSerializer() {
    final SchemaRegistryClient schemaRegistryClient =
        MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE);
    final KafkaAvroSerializer serializer = new KafkaAvroSerializer(schemaRegistryClient);
    serializer.configure(Map.of("schema.registry.url", MOCK_SCHEMA_REGISTRY_URL), false);
    return serializer;
  }

  protected Map<String, String> createConnectorConfig() {
    final String topics = topic0 + "," + topic1;
    final String topicsToTableMap = topic0 + ":" + tableName + "," + topic1 + ":" + tableName;

    final Map<String, String> config = defaultProperties(topics, connectorName);
    config.put(SNOWFLAKE_TOPICS2TABLE_MAP, topicsToTableMap);
    config.put(ConnectorConfig.KEY_CONVERTER_CLASS_CONFIG, StringConverter.class.getName());
    config.put("value.converter.schemas.enable", "false");
    config.put("errors.tolerance", "none");
    config.put("errors.log.enable", "true");
    config.put("errors.deadletterqueue.topic.name", "DLQ_TOPIC");
    config.put("errors.deadletterqueue.topic.replication.factor", "1");
    config.put("jmx", "true");
    return config;
  }

  void sendTombstoneRecords(final String topic) {
    // Send null tombstone
    connectCluster.kafka().produce(topic, null);
  }

  private FakeSnowflakeStreamingIngestClient getFakeSnowflakeStreamingIngestClient(
      String connectorName) {
    return fakeClientSupplier.getFakeIngestClients().stream()
        .filter((client) -> client.getConnectorName().equals(connectorName))
        .findFirst()
        .orElseThrow();
  }
}
