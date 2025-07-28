package com.snowflake.kafka.connector.internal.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import dev.failsafe.function.CheckedSupplier;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.SFException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class OpenChannelRetryPolicyTest {

  @Mock private SnowflakeStreamingIngestChannel mockChannel;

  private Map<String, String> connectorConfig;
  private final String channelName = "test_channel";

  @BeforeEach
  void setUp() {
    MockitoAnnotations.initMocks(this);
    connectorConfig = new HashMap<>();
  }

  @Test
  void shouldReturnChannelOnFirstAttemptSuccess() {
    // Given
    CheckedSupplier<SnowflakeStreamingIngestChannel> supplier = () -> mockChannel;

    // When
    SnowflakeStreamingIngestChannel result =
        OpenChannelRetryPolicy.executeWithRetry(supplier, channelName, connectorConfig);

    // Then
    assertSame(mockChannel, result);
  }

  @Test
  void shouldRetryOnSFExceptionAndEventuallySucceed() {
    // Given
    AtomicInteger attemptCount = new AtomicInteger(0);
    SFException sfException = new SFException(ErrorCode.INTERNAL_ERROR, "Temporary failure");

    CheckedSupplier<SnowflakeStreamingIngestChannel> supplier =
        () -> {
          int attempt = attemptCount.incrementAndGet();
          if (attempt <= 2) {
            throw sfException; // Fail first 2 attempts
          }
          return mockChannel; // Succeed on 3rd attempt
        };

    // When
    SnowflakeStreamingIngestChannel result =
        OpenChannelRetryPolicy.executeWithRetry(supplier, channelName, connectorConfig);

    // Then
    assertSame(mockChannel, result);
    assertEquals(3, attemptCount.get()); // Verify it took 3 attempts
  }

  @Test
  void shouldNotRetryOnNonSFException() {
    // Given
    IllegalArgumentException nonRetryableException = new IllegalArgumentException("Non-retryable");
    CheckedSupplier<SnowflakeStreamingIngestChannel> supplier =
        () -> {
          throw nonRetryableException;
        };

    // When/Then
    IllegalArgumentException thrownException =
        assertThrows(
            IllegalArgumentException.class,
            () -> OpenChannelRetryPolicy.executeWithRetry(supplier, channelName, connectorConfig));

    assertSame(nonRetryableException, thrownException);
  }

  @Test
  void shouldEventuallyFailAfterMaxRetries() {
    // Given
    SFException persistentException =
        new SFException(ErrorCode.INTERNAL_ERROR, "Persistent failure");
    AtomicInteger attemptCount = new AtomicInteger(0);

    CheckedSupplier<SnowflakeStreamingIngestChannel> supplier =
        () -> {
          attemptCount.incrementAndGet();
          throw persistentException; // Always fail
        };

    // When/Then
    RuntimeException thrownException =
        assertThrows(
            RuntimeException.class,
            () -> OpenChannelRetryPolicy.executeWithRetry(supplier, channelName, connectorConfig));

    // Verify that origina lexception is thrown
    assertSame(persistentException, thrownException);

    // Verify multiple attempts were made
    assertTrue(attemptCount.get() > 1, "Should have made multiple retry attempts");
  }
}
