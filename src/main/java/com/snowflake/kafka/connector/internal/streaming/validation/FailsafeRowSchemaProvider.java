package com.snowflake.kafka.connector.internal.streaming.validation;

import static com.snowflake.kafka.connector.internal.SnowflakeErrors.ERROR_1007;

import com.snowflake.kafka.connector.internal.KCLogger;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeExecutor;
import dev.failsafe.Fallback;
import dev.failsafe.RetryPolicy;
import java.time.Duration;
import java.util.Map;

/** Wrapper to provide retry ability for RowSchemaProvider. */
public class FailsafeRowSchemaProvider implements RowSchemaProvider {

  private static final KCLogger LOGGER = new KCLogger(FailsafeRowSchemaProvider.class.getName());
  private static final Duration RETRY_DELAY = Duration.ofSeconds(1);
  private static final int MAX_ATTEMPTS = 3;

  private final RowSchemaProvider provider;
  private final FailsafeExecutor<RowSchema> failsafeExecutor;

  public FailsafeRowSchemaProvider(RowSchemaProvider provider) {
    this.provider = provider;
    this.failsafeExecutor = createExecutor();
  }

  @Override
  public RowSchema getRowSchema(String tableName, Map<String, String> connectorConfig) {
    return failsafeExecutor.get(() -> provider.getRowSchema(tableName, connectorConfig));
  }

  private static FailsafeExecutor<RowSchema> createExecutor() {
    RetryPolicy<RowSchema> retry =
        RetryPolicy.<RowSchema>builder()
            .handle(RowsetApiException.class)
            .withDelay(RETRY_DELAY)
            .withMaxAttempts(MAX_ATTEMPTS)
            .onRetry(
                event ->
                    LOGGER.warn(
                        "Failed to get row schema, retrying. Exception {}",
                        event.getLastException()))
            .build();

    Fallback<RowSchema> fallback =
        Fallback.ofException(e -> ERROR_1007.getException("Failed to get row schema"));
    return Failsafe.with(fallback, retry);
  }
}
