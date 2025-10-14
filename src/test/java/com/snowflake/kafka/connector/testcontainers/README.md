# Testcontainers Kafka Integration Tests

This package provides a JUnit 5 extension for integration tests using [Testcontainers](https://www.testcontainers.org/) to automatically start and manage Kafka broker, Schema Registry, and Kafka Connect containers.

## Overview

The `KafkaTestcontainersExtension` provides a simple, zero-configuration approach to integration testing with Kafka components using annotation-based field injection. All containers use PLAINTEXT security (no authentication) for simplicity.

## Features

- **Automatic Container Management**: Containers are automatically started before tests and stopped after
- **No Authentication Required**: All services configured with PLAINTEXT for easy testing
- **Complete Kafka Stack**: Includes Kafka broker, Schema Registry, and Kafka Connect
- **Helper Methods**: Pre-configured producer and consumer creation
- **Shared Containers**: Containers are shared across all tests in a class for faster execution
- **Annotation-Based Injection**: Clean field injection using `@InjectKafkaEnvironment`

## Usage

### Basic Example

```java
package com.snowflake.kafka.connector.mypackage;

import com.snowflake.kafka.connector.testcontainers.KafkaTestcontainersExtension;
import com.snowflake.kafka.connector.testcontainers.InjectKafkaEnvironment;
import com.snowflake.kafka.connector.testcontainers.KafkaTestEnvironment;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(KafkaTestcontainersExtension.class)
class MyIntegrationTest {

    @InjectKafkaEnvironment
    private KafkaTestEnvironment kafkaEnv;

    @Test
    void testProduceMessage() throws Exception {
        // Use helper method to create a producer
        try (KafkaProducer<String, String> producer = kafkaEnv.createKafkaProducer()) {
            ProducerRecord<String, String> record = 
                new ProducerRecord<>("test-topic", "key", "value");
            producer.send(record).get();
        }
    }

    @Test
    void testWithSchemaRegistry() {
        // Access Schema Registry URL
        String schemaRegistryUrl = kafkaEnv.getSchemaRegistryUrl();
        // Use in your test...
    }
}
```

### Available Helper Methods

#### Connection URLs

- `kafkaEnv.getKafkaBootstrapServers()` - Returns Kafka bootstrap servers URL
- `kafkaEnv.getSchemaRegistryUrl()` - Returns Schema Registry HTTP URL
- `kafkaEnv.getConnectUrl()` - Returns Kafka Connect REST API URL

#### Client Creation

- `kafkaEnv.createKafkaProducer()` - Creates a String producer with sensible defaults
- `kafkaEnv.createKafkaConsumer(String groupId)` - Creates a String consumer with sensible defaults

#### Advanced Access

- `kafkaEnv.getKafkaContainer()` - Access the underlying Kafka container
- `kafkaEnv.getSchemaRegistryContainer()` - Access the Schema Registry container
- `kafkaEnv.getKafkaConnectContainer()` - Access the Kafka Connect container

## Configuration

The extension uses the following defaults:

### Kafka Broker
- Image: `confluentinc/cp-kafka:7.9.2`
- Security: PLAINTEXT (no authentication)
- Auto-create topics: Enabled
- Network alias: `kafka`

### Schema Registry
- Image: `confluentinc/cp-schema-registry:7.9.2`
- Port: 8081
- Security: None
- Network alias: `schema-registry`

### Kafka Connect
- Image: `confluentinc/cp-kafka-connect:7.9.2`
- Port: 8083
- Key/Value converters: JsonConverter
- Replication factor: 1 (for all internal topics)
- Network alias: `connect`

## Running Tests

To run integration tests that use this extension:

```bash
# Run all integration tests
mvn verify -Dgpg.skip=true

# Run specific test class
mvn test -Dtest=TestcontainersKafkaBaseITTest -Dgpg.skip=true
```

Note: Tests require Docker to be running on the host machine.

## Example Test

See `TestcontainersKafkaBaseITTest` in this package for a complete example demonstrating:
- Producing and consuming messages
- Verifying Schema Registry availability
- Verifying Kafka Connect availability
- Multiple message handling

## Requirements

- Docker installed and running
- Java 11+
- Maven 3.6+

## Dependencies

The following dependencies are automatically included (see `pom.xml`):

```xml
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>testcontainers</artifactId>
    <version>1.19.0</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>junit-jupiter</artifactId>
    <version>1.19.0</version>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.testcontainers</groupId>
    <artifactId>kafka</artifactId>
    <version>1.19.0</version>
    <scope>test</scope>
</dependency>
```

## Troubleshooting

### Tests are slow to start
- Containers are downloaded on first run. Subsequent runs will be faster.
- Consider using Testcontainers cloud or local registry caching.

### "Could not find a valid Docker environment"
- Ensure Docker is installed and running
- Check Docker daemon is accessible (try `docker ps`)

### Connection refused errors
- Containers may need more time to start
- Check container logs: `docker logs <container-id>`
- Increase startup timeout if needed

## Best Practices

1. **Containers are shared**: The extension manages per-class container lifecycle automatically
2. **Clean up resources**: Close producers/consumers in try-with-resources or @AfterEach
3. **Use unique topic names**: Avoid conflicts between tests
4. **Keep tests focused**: Each test should verify a specific behavior
5. **Field injection**: Annotate fields with `@InjectKafkaEnvironment` for automatic injection

## Alternative Approaches

For different testing needs, consider:

- `ConnectClusterBaseIT` - Uses Kafka's embedded test cluster (faster but less realistic)
- End-to-end tests in `test/` directory - Full integration with real Snowflake

## Support

For issues or questions:
- Check [Testcontainers documentation](https://www.testcontainers.org/)
- Review example test: `TestcontainersKafkaBaseITTest`
- See project's main README for general testing guidance

