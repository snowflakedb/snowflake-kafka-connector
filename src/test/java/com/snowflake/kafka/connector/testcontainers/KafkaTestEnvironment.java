package com.snowflake.kafka.connector.testcontainers;

import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;

/**
 * Test utility class providing access to Kafka, Schema Registry, and Kafka Connect containers along
 * with helper methods for creating producers and consumers.
 *
 * <p>This class is injected into test fields annotated with {@link InjectKafkaEnvironment} when
 * using {@link KafkaTestcontainersExtension}.
 */
final class KafkaTestEnvironment {

  private static final int SCHEMA_REGISTRY_PORT = 8081;
  private static final int KAFKA_CONNECT_PORT = 8083;

  private final KafkaContainer kafkaContainer;
  private final GenericContainer<?> schemaRegistryContainer;
  private final GenericContainer<?> kafkaConnectContainer;

  KafkaTestEnvironment(
      final KafkaContainer kafkaContainer,
      final GenericContainer<?> schemaRegistryContainer,
      final GenericContainer<?> kafkaConnectContainer) {
    this.kafkaContainer = kafkaContainer;
    this.schemaRegistryContainer = schemaRegistryContainer;
    this.kafkaConnectContainer = kafkaConnectContainer;
  }

  /**
   * Returns the Kafka bootstrap servers URL for connecting from the host machine.
   *
   * @return Kafka bootstrap servers URL
   */
  public String getKafkaBootstrapServers() {
    return kafkaContainer.getBootstrapServers();
  }

  /**
   * Returns the Schema Registry URL for connecting from the host machine.
   *
   * @return Schema Registry URL
   */
  public String getSchemaRegistryUrl() {
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
  public String getConnectUrl() {
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
  public KafkaProducer<String, String> createKafkaProducer() {
    final Properties props = new Properties();
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
  public KafkaConsumer<String, String> createKafkaConsumer(final String groupId) {
    final Properties props = new Properties();
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
  public KafkaContainer getKafkaContainer() {
    return kafkaContainer;
  }

  /**
   * Returns the Schema Registry container instance for advanced usage.
   *
   * @return Schema Registry container instance
   */
  public GenericContainer<?> getSchemaRegistryContainer() {
    return schemaRegistryContainer;
  }

  /**
   * Returns the Kafka Connect container instance for advanced usage.
   *
   * @return Kafka Connect container instance
   */
  public GenericContainer<?> getKafkaConnectContainer() {
    return kafkaConnectContainer;
  }
}

