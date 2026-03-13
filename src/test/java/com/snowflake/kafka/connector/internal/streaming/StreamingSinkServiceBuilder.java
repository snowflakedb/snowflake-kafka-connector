package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.dlq.KafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import com.snowflake.kafka.connector.internal.metrics.MetricsJmxReporter;
import com.snowflake.kafka.connector.internal.metrics.TaskMetrics;
import java.util.Collections;
import java.util.Optional;
import org.apache.kafka.connect.sink.SinkTaskContext;

public class StreamingSinkServiceBuilder {

  private final SnowflakeConnectionService conn;
  private SinkTaskConfig config;

  private KafkaRecordErrorReporter errorReporter = new InMemoryKafkaRecordErrorReporter();
  private SinkTaskContext sinkTaskContext = new InMemorySinkTaskContext(Collections.emptySet());
  private Optional<MetricsJmxReporter> metricsJmxReporter = Optional.empty();
  private TaskMetrics taskMetrics = TaskMetrics.noop();

  public static StreamingSinkServiceBuilder builder(
      SnowflakeConnectionService conn, SinkTaskConfig config) {
    return new StreamingSinkServiceBuilder(conn, config);
  }

  public SnowflakeSinkServiceV2 build() {
    return new SnowflakeSinkServiceV2(
        conn, config, errorReporter, sinkTaskContext, metricsJmxReporter, taskMetrics);
  }

  private StreamingSinkServiceBuilder(SnowflakeConnectionService conn, SinkTaskConfig config) {
    this.conn = conn;
    this.config = config;
  }

  public StreamingSinkServiceBuilder withErrorReporter(KafkaRecordErrorReporter errorReporter) {
    this.errorReporter = errorReporter;
    return this;
  }

  public StreamingSinkServiceBuilder withSinkTaskContext(SinkTaskContext sinkTaskContext) {
    this.sinkTaskContext = sinkTaskContext;
    return this;
  }

  public StreamingSinkServiceBuilder withMetricsJmxReporter(MetricsJmxReporter reporter) {
    this.metricsJmxReporter = Optional.of(reporter);
    return this;
  }
}
