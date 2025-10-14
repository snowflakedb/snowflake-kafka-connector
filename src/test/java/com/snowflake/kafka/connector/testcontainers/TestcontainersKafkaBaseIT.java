package com.snowflake.kafka.connector.testcontainers;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/**
 * Base class for integration tests using Testcontainers to provide Kafka broker, Schema Registry,
 * and Kafka Connect. This class provides the simplest possible configuration with no authentication
 * required.
 *
 * <p>Containers are started once for all tests in the class (PER_CLASS lifecycle) and automatically
 * cleaned up on shutdown.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class TestcontainersKafkaBaseIT {

  private static final Logger LOGGER = LoggerFactory.getLogger(TestcontainersKafkaBaseIT.class);

  private static final String CONFLUENT_PLATFORM_VERSION = "7.9.2";
  private static final int SCHEMA_REGISTRY_PORT = 8081;
  private static final int KAFKA_CONNECT_PORT = 8083;

  private Network network;
  private KafkaContainer kafkaContainer;
  private GenericContainer<?> schemaRegistryContainer;
  private GenericContainer<?> kafkaConnectContainer;

  @BeforeAll
  @SuppressWarnings("resource") // Containers are closed in @AfterAll
  public void startContainers() {
    LOGGER.info("Starting Testcontainers: Kafka, Schema Registry, and Kafka Connect");

    // Create a shared network for all containers
    network = Network.newNetwork();

    // Start Kafka container
    kafkaContainer =
        new KafkaContainer(
                DockerImageName.parse("confluentinc/cp-kafka:" + CONFLUENT_PLATFORM_VERSION))
            .withNetwork(network)
            .withNetworkAliases("kafka")
            .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
            .withStartupTimeout(Duration.ofMinutes(2));

    kafkaContainer.start();
    LOGGER.info("Kafka container started. Bootstrap servers: {}", getKafkaBootstrapServers());

    // Start Schema Registry container
    schemaRegistryContainer =
        new GenericContainer<>(
                DockerImageName.parse(
                    "confluentinc/cp-schema-registry:" + CONFLUENT_PLATFORM_VERSION))
            .withNetwork(network)
            .withNetworkAliases("schema-registry")
            .withExposedPorts(SCHEMA_REGISTRY_PORT)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
            .withEnv(
                "SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS",
                "PLAINTEXT://kafka:9092")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT)
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(2))
            .dependsOn(kafkaContainer);

    schemaRegistryContainer.start();
    LOGGER.info("Schema Registry container started. URL: {}", getSchemaRegistryUrl());

    // Start Kafka Connect container
    Map<String, String> connectEnv = new HashMap<>();
    connectEnv.put("CONNECT_BOOTSTRAP_SERVERS", "kafka:9092");
    connectEnv.put("CONNECT_REST_PORT", String.valueOf(KAFKA_CONNECT_PORT));
    connectEnv.put("CONNECT_GROUP_ID", "connect-cluster");
    connectEnv.put("CONNECT_CONFIG_STORAGE_TOPIC", "connect-configs");
    connectEnv.put("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1");
    connectEnv.put("CONNECT_OFFSET_STORAGE_TOPIC", "connect-offsets");
    connectEnv.put("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1");
    connectEnv.put("CONNECT_STATUS_STORAGE_TOPIC", "connect-status");
    connectEnv.put("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1");
    connectEnv.put("CONNECT_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
    connectEnv.put("CONNECT_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter");
    connectEnv.put("CONNECT_KEY_CONVERTER_SCHEMAS_ENABLE", "false");
    connectEnv.put("CONNECT_VALUE_CONVERTER_SCHEMAS_ENABLE", "false");
    connectEnv.put("CONNECT_REST_ADVERTISED_HOST_NAME", "connect");
    connectEnv.put("CONNECT_PLUGIN_PATH", "/usr/share/java,/usr/share/confluent-hub-components");
    connectEnv.put(
        "CONNECT_SCHEMA_REGISTRY_URL",
        "http://schema-registry:" + SCHEMA_REGISTRY_PORT);

    kafkaConnectContainer =
        new GenericContainer<>(
                DockerImageName.parse(
                    "confluentinc/cp-kafka-connect:" + CONFLUENT_PLATFORM_VERSION))
            .withNetwork(network)
            .withNetworkAliases("connect")
            .withExposedPorts(KAFKA_CONNECT_PORT)
            .withEnv(connectEnv)
            .waitingFor(Wait.forHttp("/connectors").forStatusCode(200))
            .withStartupTimeout(Duration.ofMinutes(3))
            .dependsOn(kafkaContainer, schemaRegistryContainer);

    kafkaConnectContainer.start();
    LOGGER.info("Kafka Connect container started. URL: {}", getConnectUrl());

    LOGGER.info("All Testcontainers started successfully");
  }

  @AfterAll
  public void stopContainers() {
    LOGGER.info("Stopping Testcontainers");

    if (kafkaConnectContainer != null) {
      kafkaConnectContainer.stop();
      LOGGER.info("Kafka Connect container stopped");
    }

    if (schemaRegistryContainer != null) {
      schemaRegistryContainer.stop();
      LOGGER.info("Schema Registry container stopped");
    }

    if (kafkaContainer != null) {
      kafkaContainer.stop();
      LOGGER.info("Kafka container stopped");
    }

    if (network != null) {
      network.close();
      LOGGER.info("Network closed");
    }

    LOGGER.info("All Testcontainers stopped");
  }

  /**
   * Returns the Kafka bootstrap servers URL for connecting from the host machine.
   *
   * @return Kafka bootstrap servers URL
   */
  protected final String getKafkaBootstrapServers() {
    return kafkaContainer.getBootstrapServers();
  }

  /**
   * Returns the Schema Registry URL for connecting from the host machine.
   *
   * @return Schema Registry URL
   */
  protected final String getSchemaRegistryUrl() {
    return String.format(
        "http://%s:%d",
        schemaRegistryContainer.getHost(),
        schemaRegistryContainer.getMappedPort(SCHEMA_REGISTRY_PORT));
  }

  /**
   * Returns the Kafka Connect REST API URL for connecting from the host machine.
   *
   * @return Kafka Connect REST API URL
   */
  protected final String getConnectUrl() {
    return String.format(
        "http://%s:%d",
        kafkaConnectContainer.getHost(),
        kafkaConnectContainer.getMappedPort(KAFKA_CONNECT_PORT));
  }

  /**
   * Creates a Kafka producer with String key and value serializers.
   *
   * @return configured KafkaProducer
   */
  protected final KafkaProducer<String, String> createKafkaProducer() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBootstrapServers());
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 0);
    return new KafkaProducer<>(props);
  }

  /**
   * Creates a Kafka consumer with String key and value deserializers.
   *
   * @param groupId the consumer group ID
   * @return configured KafkaConsumer
   */
  protected final KafkaConsumer<String, String> createKafkaConsumer(final String groupId) {
    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
    return new KafkaConsumer<>(props);
  }

  /**
   * Returns the Kafka container instance for advanced usage.
   *
   * @return KafkaContainer instance
   */
  protected final KafkaContainer getKafkaContainer() {
    return kafkaContainer;
  }

  /**
   * Returns the Schema Registry container instance for advanced usage.
   *
   * @return Schema Registry container instance
   */
  protected final GenericContainer<?> getSchemaRegistryContainer() {
    return schemaRegistryContainer;
  }

  /**
   * Returns the Kafka Connect container instance for advanced usage.
   *
   * @return Kafka Connect container instance
   */
  protected final GenericContainer<?> getKafkaConnectContainer() {
    return kafkaConnectContainer;
  }
}

