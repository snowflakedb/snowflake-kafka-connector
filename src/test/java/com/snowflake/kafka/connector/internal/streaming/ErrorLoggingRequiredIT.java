package com.snowflake.kafka.connector.internal.streaming;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.snowflake.kafka.connector.StaticTopicToTableResolver;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.config.SinkTaskConfigTestBuilder;
import com.snowflake.kafka.connector.config.SnowflakeValidation;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import com.snowflake.kafka.connector.internal.streaming.v2.service.BatchOffsetFetcher;
import com.snowflake.kafka.connector.internal.streaming.v2.service.PartitionChannelManager;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Live-account coverage for {@code snowflake.validation.require.error.table}. Creates a real table
 * without ERROR_LOGGING and asserts the connector fails closed by default, then succeeds after
 * {@code ALTER TABLE ... SET ERROR_LOGGING = TRUE}.
 */
public class ErrorLoggingRequiredIT {

  private final SnowflakeConnectionService conn = TestUtils.getConnectionService();
  private String table;

  @BeforeEach
  public void setup() {
    table = TestUtils.randomTableName();
    TestUtils.createTableWithMetadataColumn(table, true, false);
  }

  @AfterEach
  public void teardown() {
    TestUtils.dropTable(table);
  }

  @Test
  public void existingTableWithoutErrorLogging_constructorFailsWithError0036() {
    assertFalse(conn.hasErrorLoggingEnabled(table));

    assertThatThrownBy(() -> newService(requireErrorTableConfig(true)))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining("0036")
        .hasMessageContaining(table)
        .hasMessageContaining("ERROR_LOGGING");
  }

  @Test
  public void existingTableWithoutErrorLogging_createTableIfNotExistsFailsWithError0036() {
    assertFalse(conn.hasErrorLoggingEnabled(table));
    SnowflakeSinkServiceV2 service = newService(unmappedServerSideConfig(true));

    assertThatThrownBy(() -> service.createTableIfNotExists(table))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining("0036")
        .hasMessageContaining(table);
  }

  @Test
  public void optOut_allowsExistingTableWithoutErrorLogging() {
    assertFalse(conn.hasErrorLoggingEnabled(table));

    SnowflakeSinkServiceV2 service = newService(requireErrorTableConfig(false));
    service.createTableIfNotExists(table);
  }

  @Test
  public void existingTableWithErrorLogging_starts() {
    conn.executeQueryWithParameters("alter table identifier(?) set error_logging = true", table);
    assertTrue(conn.hasErrorLoggingEnabled(table));

    SnowflakeSinkServiceV2 service = newService(requireErrorTableConfig(true));
    service.createTableIfNotExists(table);
  }

  private SinkTaskConfig requireErrorTableConfig(boolean require) {
    return SinkTaskConfigTestBuilder.builder()
        .connectorName(TestUtils.TEST_CONNECTOR_NAME)
        .taskId("0")
        .validation(SnowflakeValidation.SERVER_SIDE)
        .requireErrorTable(require)
        .topicToTableResolver(new StaticTopicToTableResolver(Map.of("topic1", table)))
        .build();
  }

  private SinkTaskConfig unmappedServerSideConfig(boolean require) {
    return SinkTaskConfigTestBuilder.builder()
        .connectorName(TestUtils.TEST_CONNECTOR_NAME)
        .taskId("0")
        .validation(SnowflakeValidation.SERVER_SIDE)
        .requireErrorTable(require)
        .build();
  }

  private SnowflakeSinkServiceV2 newService(SinkTaskConfig config) {
    return new SnowflakeSinkServiceV2(
        conn,
        config,
        mock(SinkTaskContext.class),
        Optional.empty(),
        () -> mock(BatchOffsetFetcher.class),
        () -> mock(PartitionChannelManager.class),
        TaskMetrics.noop());
  }
}
