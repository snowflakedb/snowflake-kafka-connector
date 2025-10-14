package com.snowflake.kafka.connector.testcontainers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Example integration test demonstrating the usage of KafkaTestcontainersExtension. This test
 * verifies that Kafka broker, Schema Registry, and Kafka Connect are properly configured and
 * accessible.
 */
@ExtendWith(KafkaTestcontainersExtension.class)
class TestcontainersKafkaBaseITTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @InjectKafkaEnvironment private KafkaTestEnvironment kafkaEnv;

  @Test
  void testKafkaBrokerIsAccessible() throws ExecutionException, InterruptedException {
    // Given
    final String topicName = "test-topic";
    final String testKey = "test-key";
    final String testValue = "test-value";

    // When - produce a message
    try (KafkaProducer<String, String> producer = kafkaEnv.createKafkaProducer()) {
      ProducerRecord<String, String> record =
          new ProducerRecord<>(topicName, testKey, testValue);
      RecordMetadata metadata = producer.send(record).get();

      // Then
      assertNotNull(metadata);
      assertEquals(topicName, metadata.topic());
      assertTrue(metadata.offset() >= 0);
    }
  }

  @Test
  void testKafkaProducerAndConsumer() throws Exception {
    // Given
    final String topicName = "test-producer-consumer-topic";
    final String testKey = "key-1";
    final String testValue = "value-1";

    // When - produce a message
    try (KafkaProducer<String, String> producer = kafkaEnv.createKafkaProducer()) {
      ProducerRecord<String, String> record =
          new ProducerRecord<>(topicName, testKey, testValue);
      producer.send(record).get();
      producer.flush();
    }

    // Then - consume the message
    try (KafkaConsumer<String, String> consumer = kafkaEnv.createKafkaConsumer("test-group-1")) {
      consumer.subscribe(Collections.singletonList(topicName));

      ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
      assertEquals(1, records.count(), "Expected exactly one record");

      ConsumerRecord<String, String> record = records.iterator().next();
      assertEquals(testKey, record.key());
      assertEquals(testValue, record.value());
    }
  }

  @Test
  void testSchemaRegistryIsAccessible() throws Exception {
    // Given
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(URI.create(kafkaEnv.getSchemaRegistryUrl() + "/subjects"))
            .GET()
            .build();

    // When
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    // Then
    assertEquals(200, response.statusCode(), "Schema Registry should respond with 200");
    JsonNode jsonResponse = OBJECT_MAPPER.readTree(response.body());
    assertTrue(jsonResponse.isArray(), "Response should be a JSON array");
  }

  @Test
  void testKafkaConnectIsAccessible() throws Exception {
    // Given
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request =
        HttpRequest.newBuilder().uri(URI.create(kafkaEnv.getConnectUrl() + "/connectors")).GET().build();

    // When
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    // Then
    assertEquals(200, response.statusCode(), "Kafka Connect should respond with 200");
    JsonNode jsonResponse = OBJECT_MAPPER.readTree(response.body());
    assertTrue(jsonResponse.isArray(), "Response should be a JSON array of connectors");
  }

  @Test
  void testKafkaConnectVersion() throws Exception {
    // Given
    HttpClient client = HttpClient.newHttpClient();
    HttpRequest request =
        HttpRequest.newBuilder().uri(URI.create(kafkaEnv.getConnectUrl() + "/")).GET().build();

    // When
    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

    // Then
    assertEquals(200, response.statusCode(), "Kafka Connect root endpoint should respond with 200");
    JsonNode jsonResponse = OBJECT_MAPPER.readTree(response.body());
    assertTrue(jsonResponse.has("version"), "Response should contain version information");
    assertTrue(jsonResponse.has("commit"), "Response should contain commit information");
    assertNotNull(jsonResponse.get("version").asText());
  }

  @Test
  void testMultipleMessagesProducerConsumer() throws Exception {
    // Given
    final String topicName = "test-multiple-messages-topic";
    final int messageCount = 10;

    // When - produce multiple messages
    try (KafkaProducer<String, String> producer = kafkaEnv.createKafkaProducer()) {
      for (int i = 0; i < messageCount; i++) {
        ProducerRecord<String, String> record =
            new ProducerRecord<>(topicName, "key-" + i, "value-" + i);
        producer.send(record).get();
      }
      producer.flush();
    }

    // Then - consume all messages
    try (KafkaConsumer<String, String> consumer = kafkaEnv.createKafkaConsumer("test-group-2")) {
      consumer.subscribe(Collections.singletonList(topicName));

      int totalRecordsRead = 0;
      long startTime = System.currentTimeMillis();
      while (totalRecordsRead < messageCount
          && (System.currentTimeMillis() - startTime) < 10000) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        totalRecordsRead += records.count();
      }

      assertEquals(messageCount, totalRecordsRead, "Should consume all produced messages");
    }
  }

  @Test
  void testContainerUrlsAreValid() {
    // Given & When
    final String kafkaBootstrapServers = kafkaEnv.getKafkaBootstrapServers();
    final String schemaRegistryUrl = kafkaEnv.getSchemaRegistryUrl();
    final String connectUrl = kafkaEnv.getConnectUrl();

    // Then
    assertNotNull(kafkaBootstrapServers);
    assertTrue(kafkaBootstrapServers.contains(":"), "Kafka bootstrap servers should contain port");

    assertNotNull(schemaRegistryUrl);
    assertTrue(schemaRegistryUrl.startsWith("http://"), "Schema Registry URL should start with http://");

    assertNotNull(connectUrl);
    assertTrue(connectUrl.startsWith("http://"), "Kafka Connect URL should start with http://");
  }
}

