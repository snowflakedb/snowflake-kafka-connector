package com.snowflake.kafka.connector.internal.streaming;

import com.snowflake.kafka.connector.internal.TestUtils;
import org.apache.kafka.common.TopicPartition;

public abstract class SnowflakeSinkServiceV2BaseIT {

  protected final String table = TestUtils.randomTableName();

  protected final int partition = 0;
  protected final int partition2 = 1;

  // Topic name should be same as table name. (Only for testing, not necessarily in real deployment)
  protected String topic = table;
  protected TopicPartition topicPartition = new TopicPartition(topic, partition);
}
