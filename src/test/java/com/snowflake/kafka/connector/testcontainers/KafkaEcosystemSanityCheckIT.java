package com.snowflake.kafka.connector.testcontainers;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(KafkaEcosystemExtention.class)
class KafkaEcosystemSanityCheckIT {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @InjectKafkaEnvironment
    private KafkaTestEnvironment kafkaEnv;

    @BeforeEach
    void setup() {
    }

    @Test
    void test_kafka_broker_is_accessible() throws ExecutionException, InterruptedException {
        // Given
        final String topicName = "test-topic";
        final String testKey = "test-key";
        final String testValue = "test-value";

        // when
        try (KafkaProducer<String, String> producer = kafkaEnv.createKafkaProducer()) {
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName, testKey, testValue);
            producer.send(record).get();
            producer.flush();
        }

        // then
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
    void test_schema_registry_is_accessible() throws Exception {
        // Given
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder().uri(URI.create(kafkaEnv.getSchemaRegistryUrl() + "/subjects")).GET().build();

        // When
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        // Then
        assertEquals(200, response.statusCode(), "Schema Registry should respond with 200");
        JsonNode jsonResponse = OBJECT_MAPPER.readTree(response.body());
        assertTrue(jsonResponse.isArray(), "Response should be a JSON array");
    }
}

