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
 * Live-account coverage for {@code snowflake.validation.require.error.table}. Existing tables
 * without ERROR_LOGGING fail startup; tables the connector creates already have ERROR_LOGGING.
 */
public class ErrorLoggingRequiredIT {

  private final SnowflakeConnectionService conn = TestUtils.getConnectionService();
  private String table;

  @BeforeEach
  public void setup() {
    table = TestUtils.randomTableName();
  }

  @AfterEach
  public void teardown() {
    TestUtils.dropTable(table);
  }

  @Test
  public void existingTableWithoutErrorLogging_constructorFailsWithError0036() {
    TestUtils.createTableWithMetadataColumn(table, true, false);
    assertFalse(conn.hasErrorLoggingEnabled(table));

    assertThatThrownBy(() -> newService(mappedServerSideConfig(true)))
        .isInstanceOf(SnowflakeKafkaConnectorException.class)
        .hasMessageContaining("0036")
        .hasMessageContaining(table)
        .hasMessageContaining("ERROR_LOGGING");
  }

  @Test
  public void missingTable_isCreatedWithErrorLogging() {
    assertFalse(conn.tableExist(table));

    SnowflakeSinkServiceV2 service = newService(unmappedServerSideConfig(true));
    service.createTableIfNotExists(table);

    assertTrue(conn.tableExist(table));
    assertTrue(conn.hasErrorLoggingEnabled(table));
  }

  @Test
  public void existingTableWithoutErrorLogging_startsWhenCheckDisabled() {
    TestUtils.createTableWithMetadataColumn(table, true, false);
    assertFalse(conn.hasErrorLoggingEnabled(table));

    SnowflakeSinkServiceV2 service = newService(mappedServerSideConfig(false));
    service.createTableIfNotExists(table);
  }

  @Test
  public void existingTableWithErrorLogging_starts() {
    TestUtils.createTableWithMetadataColumn(table, true, true);
    assertTrue(conn.hasErrorLoggingEnabled(table));

    SnowflakeSinkServiceV2 service = newService(mappedServerSideConfig(true));
    service.createTableIfNotExists(table);
  }

  private SinkTaskConfig mappedServerSideConfig(boolean require) {
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
