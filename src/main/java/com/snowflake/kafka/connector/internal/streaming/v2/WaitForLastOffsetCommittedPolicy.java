package com.snowflake.kafka.connector.internal.streaming.v2;

import static com.snowflake.kafka.connector.internal.SnowflakeErrors.ERROR_5027;

import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import dev.failsafe.Failsafe;
import dev.failsafe.Fallback;
import dev.failsafe.RetryPolicy;
import dev.failsafe.function.CheckedSupplier;
import java.time.Duration;

class WaitForLastOffsetCommittedPolicy {

  private static final KCLogger LOGGER =
      new KCLogger(WaitForLastOffsetCommittedPolicy.class.getName());

  static void getPolicy(CheckedSupplier<Object> action) {
    Fallback<Object> fallback =
        Fallback.ofException(
            e -> {
              LOGGER.error("Wait for the last offset to be commited - max retry attempts", e);
              throw ERROR_5027.getException();
            });

    RetryPolicy<Object> retryPolicy =
        RetryPolicy.builder()
            .handle(SnowflakeKafkaConnectorException.class)
            .withDelay(Duration.ofSeconds(1))
            .withBackoff(Duration.ofSeconds(1), Duration.ofSeconds(30), 1.5)
            .withJitter(Duration.ofMillis(100))
            .withMaxAttempts(10) // for some reason it has to be set as well
            .onRetry(
                event ->
                    LOGGER.info(
                        "Wait for the last offset to be commited retry no:{}, message:{}",
                        event.getAttemptCount(),
                        event.getLastException().getMessage()))
            .build();

    Failsafe.with(fallback).compose(retryPolicy).get(action);
  }
}
