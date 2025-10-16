package com.snowflake.kafka.connector.testcontainers;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark fields that should be injected with a {@link KafkaTestEnvironment} instance
 * when using {@link KafkaEcosystemExtention}.
 *
 * <p>Example usage:
 *
 * <pre>
 * &#64;ExtendWith(KafkaEcosystemExtention.class)
 * class MyIntegrationTest {
 *
 *   &#64;InjectKafkaEnvironment
 *   private KafkaTestEnvironment kafkaEnv;
 *
 *   &#64;Test
 *   void testSomething() {
 *     String bootstrapServers = kafkaEnv.getKafkaBootstrapServers();
 *     // ... test logic
 *   }
 * }
 * </pre>
 */
@Target({ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
public @interface InjectKafkaEnvironment {}

