package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.SchemaEvolutionService;
import com.snowflake.kafka.connector.internal.streaming.schemaevolution.snowflake.SnowflakeSchemaEvolutionService;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class StreamingSinkServiceBuilder {

  private final SnowflakeConnectionService conn;
  private final Map<String, String> connectorConfig;

  private KafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();
  private SinkTaskContext sinkTaskContext = new InMemorySinkTaskContext(Collections.emptySet());
  private boolean enableCustomJMXMonitoring = false;
  private Map<String, String> topicToTableMap = new HashMap<>();
  private SchemaEvolutionService schemaEvolutionService;

  public static StreamingSinkServiceBuilder builder(
      SnowflakeConnectionService conn, Map<String, String> connectorConfig) {
    return new StreamingSinkServiceBuilder(conn, connectorConfig);
  }

  public SnowflakeSinkServiceV2 build() {
    return new SnowflakeSinkServiceV2(
        conn,
        connectorConfig,
        errorReporter,
        sinkTaskContext,
        enableCustomJMXMonitoring,
        topicToTableMap,
        schemaEvolutionService == null
            ? new SnowflakeSchemaEvolutionService(conn)
            : schemaEvolutionService);
  }

  private StreamingSinkServiceBuilder(
      SnowflakeConnectionService conn, Map<String, String> connectorConfig) {
    this.conn = conn;
    this.connectorConfig = connectorConfig;
  }

  public StreamingSinkServiceBuilder withErrorReporter(
      InMemoryKafkaRecordErrorReporter errorReporter) {
    this.errorReporter = errorReporter;
    return this;
  }

  public StreamingSinkServiceBuilder withSinkTaskContext(SinkTaskContext sinkTaskContext) {
    this.sinkTaskContext = sinkTaskContext;
    return this;
  }

  public StreamingSinkServiceBuilder withEnableCustomJMXMetrics(boolean enableCustomJMXMetrics) {
    this.enableCustomJMXMonitoring = enableCustomJMXMetrics;
    return this;
  }

  public StreamingSinkServiceBuilder withTopicToTableMap(Map<String, String> topic2TableMap) {
    topicToTableMap = topic2TableMap;
    return this;
  }

  public StreamingSinkServiceBuilder withSchemaEvolutionService(
      SchemaEvolutionService schemaEvolutionService) {
    this.schemaEvolutionService = schemaEvolutionService;
    return this;
  }
}
