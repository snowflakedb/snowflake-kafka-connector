package com.snowflake.kafka.connector.internal.streaming;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;

/* In memory implementation of SinkTaskContext used for testing */
public class InMemorySinkTaskContext implements SinkTaskContext {
  private final Map<TopicPartition, Long> offsets = new HashMap();
  private long timeoutMs = -1L;
  private Set<TopicPartition> assignment;

  public InMemorySinkTaskContext(Set<TopicPartition> assignment) {
    this.assignment = assignment;
  }

  public Map<String, String> configs() {
    throw new UnsupportedOperationException();
  }

  public void offset(Map<TopicPartition, Long> offsets) {
    this.offsets.putAll(offsets);
  }

  public void offset(TopicPartition tp, long offset) {
    this.offsets.put(tp, offset);
  }

  public Map<TopicPartition, Long> offsets() {
    return this.offsets;
  }

  public void timeout(long timeoutMs) {
    this.timeoutMs = timeoutMs;
  }

  public long timeout() {
    return this.timeoutMs;
  }

  public Set<TopicPartition> assignment() {
    return this.assignment;
  }

  public void setAssignment(Set<TopicPartition> nextAssignment) {
    this.assignment = nextAssignment;
  }

  public void pause(TopicPartition... partitions) {}

  public void resume(TopicPartition... partitions) {}

  public void requestCommit() {}

  public ErrantRecordReporter errantRecordReporter() {
    return new ErrantRecordReporter() {
      @Override
      public Future<Void> report(SinkRecord record, Throwable error) {
        return Executors.newCachedThreadPool().submit(() -> null);
      }
    };
  }
}
