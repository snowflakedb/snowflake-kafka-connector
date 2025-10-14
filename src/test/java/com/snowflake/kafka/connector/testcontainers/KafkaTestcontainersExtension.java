package com.snowflake.kafka.connector.testcontainers;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestInstancePostProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.output.ToStringConsumer;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

/**
 * JUnit 5 extension that manages Kafka, Schema Registry, and Kafka Connect containers for
 * integration tests.
 *
 * <p>Containers are started once per test class and automatically cleaned up after all tests
 * complete. Use with {@link InjectKafkaEnvironment} annotation to inject a {@link
 * KafkaTestEnvironment} into test fields.
 *
 * <p>Example usage:
 *
 * <pre>
 * &#64;ExtendWith(KafkaTestcontainersExtension.class)
 * class MyIntegrationTest {
 *
 *   &#64;InjectKafkaEnvironment
 *   private KafkaTestEnvironment kafkaEnv;
 *
 *   &#64;Test
 *   void testProduceMessage() throws Exception {
 *     try (KafkaProducer&lt;String, String&gt; producer = kafkaEnv.createKafkaProducer()) {
 *       ProducerRecord&lt;String, String&gt; record =
 *           new ProducerRecord&lt;&gt;("test-topic", "key", "value");
 *       producer.send(record).get();
 *     }
 *   }
 * }
 * </pre>
 */
public final class KafkaTestcontainersExtension
    implements BeforeAllCallback, AfterAllCallback, TestInstancePostProcessor {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(KafkaTestcontainersExtension.class);

  private static final String CONFLUENT_PLATFORM_VERSION = "7.9.2";
  private static final int SCHEMA_REGISTRY_PORT = 8081;
  private static final int KAFKA_CONNECT_PORT = 8083;
  private static final String CONNECTOR_PLUGIN_PATH = "/usr/share/java";

  @Override
  public void beforeAll(final ExtensionContext context) {
    ensureEnvironmentInitialized(context);
  }

  @Override
  public void afterAll(final ExtensionContext context) {
    // Cleanup is handled automatically by ExtensionContext.Store when ContainerManager
    // is removed (it implements CloseableResource)
  }

  @Override
  public void postProcessTestInstance(final Object testInstance, final ExtensionContext context) {
    final KafkaTestEnvironment environment = ensureEnvironmentInitialized(context);

    // Find all fields annotated with @InjectKafkaEnvironment and inject the environment
    final Class<?> testClass = testInstance.getClass();
    for (Field field : testClass.getDeclaredFields()) {
      if (field.isAnnotationPresent(InjectKafkaEnvironment.class)) {
        if (!field.getType().equals(KafkaTestEnvironment.class)) {
          throw new IllegalStateException(
              String.format(
                  "Field %s annotated with @InjectKafkaEnvironment must be of type KafkaTestEnvironment, but was %s",
                  field.getName(), field.getType().getName()));
        }

        field.setAccessible(true);
        try {
          field.set(testInstance, environment);
        } catch (IllegalAccessException e) {
          throw new RuntimeException("Failed to inject KafkaTestEnvironment into field: " + field.getName(), e);
        }
      }
    }
  }

  private ExtensionContext.Namespace getNamespace(final ExtensionContext context) {
    return ExtensionContext.Namespace.create(
        KafkaTestcontainersExtension.class, context.getRequiredTestClass());
  }

  /**
   * Ensures the KafkaTestEnvironment is initialized, creating it if necessary.
   * This method is idempotent and thread-safe due to getOrComputeIfAbsent semantics.
   *
   * @param context the extension context
   * @return the initialized KafkaTestEnvironment
   */
  private KafkaTestEnvironment ensureEnvironmentInitialized(final ExtensionContext context) {
    final ContainerManager manager =
        context
            .getStore(getNamespace(context))
            .getOrComputeIfAbsent(
                ContainerManager.class,
                key -> {
                  final ContainerManager newManager = new ContainerManager();
                  newManager.start();
                  return newManager;
                },
                ContainerManager.class);

    // Store the environment in the context for injection
    final KafkaTestEnvironment environment = manager.getEnvironment();
    context
        .getStore(getNamespace(context))
        .put(KafkaTestEnvironment.class, environment);
    return environment;
  }

  /**
   * Manages the lifecycle of Kafka, Schema Registry, and Kafka Connect containers. Implements
   * {@link ExtensionContext.Store.CloseableResource} to ensure proper cleanup.
   */
  private static final class ContainerManager
      implements ExtensionContext.Store.CloseableResource {

    private Network network;
    private KafkaContainer kafkaContainer;
    private GenericContainer<?> schemaRegistryContainer;
    private GenericContainer<?> kafkaConnectContainer;
    private ToStringConsumer connectLogConsumer;
    private WaitingConsumer connectWaitingConsumer;
    private KafkaTestEnvironment environment;

    @SuppressWarnings("resource") // Containers are closed in close() method
    void start() {
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
      kafkaContainer.followOutput(new Slf4jLogConsumer(LoggerFactory.getLogger("testcontainers.kafka")));
      LOGGER.info("Kafka container started. Bootstrap servers: {}", kafkaContainer.getBootstrapServers());

      // Start Schema Registry container
      schemaRegistryContainer =
          new GenericContainer<>(
                  DockerImageName.parse(
                      "confluentinc/cp-schema-registry:" + CONFLUENT_PLATFORM_VERSION))
              .withNetwork(network)
              .withNetworkAliases("schema-registry")
              .withExposedPorts(SCHEMA_REGISTRY_PORT)
              .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
              .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
              .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:" + SCHEMA_REGISTRY_PORT)
              .waitingFor(Wait.forHttp("/subjects").forStatusCode(200))
              .withStartupTimeout(Duration.ofMinutes(2))
              .dependsOn(kafkaContainer);

      schemaRegistryContainer.start();
      schemaRegistryContainer.followOutput(new Slf4jLogConsumer(LoggerFactory.getLogger("testcontainers.schema-registry")));
      LOGGER.info(
          "Schema Registry container started. URL: http://{}:{}",
          schemaRegistryContainer.getHost(),
          schemaRegistryContainer.getMappedPort(SCHEMA_REGISTRY_PORT));

      // Start Kafka Connect container
      final Map<String, String> connectEnv = new HashMap<>();
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
          "CONNECT_SCHEMA_REGISTRY_URL", "http://schema-registry:" + SCHEMA_REGISTRY_PORT);

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

      // Copy connector JAR to container before starting it
      copyConnectorJarIfExists(kafkaConnectContainer);

      // Initialize log consumers to capture Kafka Connect logs
      connectLogConsumer = new ToStringConsumer();
      connectWaitingConsumer = new WaitingConsumer();

      kafkaConnectContainer.start();
      // Chain all log consumers: WaitingConsumer -> ToStringConsumer -> SLF4J Logger
      kafkaConnectContainer.followOutput(
          connectWaitingConsumer
              .andThen(connectLogConsumer)
              .andThen(new Slf4jLogConsumer(LoggerFactory.getLogger("testcontainers.kafka-connect"))));
      LOGGER.info(
          "Kafka Connect container started. URL: http://{}:{}",
          kafkaConnectContainer.getHost(),
          kafkaConnectContainer.getMappedPort(KAFKA_CONNECT_PORT));

      LOGGER.info("All Testcontainers started successfully");

      // Create the environment
      environment =
          new KafkaTestEnvironment(
              kafkaContainer,
              schemaRegistryContainer,
              kafkaConnectContainer,
              connectLogConsumer,
              connectWaitingConsumer);
    }

    @Override
    public void close() {
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

    KafkaTestEnvironment getEnvironment() {
      return environment;
    }

    /**
     * Copies the Snowflake Kafka Connector JAR from the target directory to the Kafka Connect
     * container before it starts. This allows Kafka Connect to discover the plugin during its
     * initial startup without requiring a restart.
     *
     * @param container the Kafka Connect container to copy the JAR to
     */
    private void copyConnectorJarIfExists(final GenericContainer<?> container) {
      try {
        final Optional<File> jarFile = findConnectorJar();
        if (jarFile.isPresent()) {
          final File jar = jarFile.get();
          final String containerPath = CONNECTOR_PLUGIN_PATH + "/" + jar.getName();
          LOGGER.info(
              "Copying Snowflake Connector JAR {} to container path: {}",
              jar.getName(),
              containerPath);
          container.withCopyFileToContainer(
              MountableFile.forHostPath(jar.toPath()), containerPath);
          LOGGER.info("Snowflake Connector JAR copied to container");
        } else {
          throw new RuntimeException(
              "Snowflake Connector JAR not found in target directory. "
                  + "Build the project first with: mvn package -Dgpg.skip=truef -DskipTests");
        }
      } catch (IOException e) {
        LOGGER.warn("Failed to copy connector JAR: {}", e.getMessage());
      }
    }

    /**
     * Finds the Snowflake Kafka Connector JAR in the target directory.
     *
     * @return Optional containing the connector JAR file if found
     * @throws IOException if an I/O error occurs while searching
     */
    private Optional<File> findConnectorJar() throws IOException {
      final Path targetDir = Paths.get("target");
      if (!Files.exists(targetDir)) {
        LOGGER.debug("Target directory does not exist");
        return Optional.empty();
      }

      try (Stream<Path> files = Files.list(targetDir)) {
        return files
            .filter(
                path ->
                    path.getFileName().toString().matches("snowflake-kafka-connector-.*\\.jar")
                        && !path.getFileName().toString().contains("javadoc")
                        && !path.getFileName().toString().contains("sources"))
            .findFirst()
            .map(Path::toFile);
      }
    }
  }
}

