package com.snowflake.kafka.connector.internal.streaming;

import static com.snowflake.kafka.connector.internal.streaming.SnowflakeSinkServiceV2.makeChannelName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class SnowflakeSinkServiceV2ChannelNameTest {

  @Test
  void testMakeChannelName_WithinLimit() {
    // Normal case - should work fine
    String channelName = makeChannelName("myConnector", "myTopic", 0);
    assertEquals("myConnector_myTopic_0", channelName);
    assertTrue(channelName.length() <= 128);
  }

  @Test
  void testMakeChannelName_ExceedsLimit() {
    // Create very long names that exceed 128 characters
    String longConnector =
        "very-long-connector-name-that-exceeds-normal-limits-for-testing-purposes-123456789";
    String longTopic =
        "very.long.topic.name.that.also.exceeds.normal.limits.for.testing.purposes.v1.json";
    int partition = 999;

    // Should throw an exception
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> makeChannelName(longConnector, longTopic, partition));

    // Verify exception message contains useful information
    assertTrue(exception.getMessage().contains("exceeds maximum length"));
    assertTrue(exception.getMessage().contains("128 characters"));
    assertTrue(exception.getMessage().contains("shorter connector name"));
  }

  @Test
  void testMakeChannelName_ExactLimit() {
    // Create names that are exactly at the limit
    // 128 - 2 (separators) - 1 (partition "0") = 125 characters for connector + topic
    String connector = "a".repeat(62); // 62 chars
    String topic = "b".repeat(63); // 63 chars
    // Total: 62 + 1 + 63 + 1 + 1 = 128

    String channelName = makeChannelName(connector, topic, 0);
    assertEquals(128, channelName.length());
    assertTrue(channelName.startsWith("a"));
    assertTrue(channelName.contains("_b"));
    assertTrue(channelName.endsWith("_0"));
  }

  @Test
  void testMakeChannelName_OneCharacterOverLimit() {
    // Create names that are exactly 1 character over the limit
    // 129 total: should throw exception
    String connector = "a".repeat(62);
    String topic = "b".repeat(64); // One extra char
    int partition = 0;

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> makeChannelName(connector, topic, partition));

    assertTrue(exception.getMessage().contains("actual: 129"));
  }

  @Test
  void testMakeChannelName_LargePartitionNumber() {
    // Test with a large partition number
    String connector = "connector";
    String topic = "topic";
    int partition = 99999;

    String channelName = makeChannelName(connector, topic, partition);
    assertEquals("connector_topic_99999", channelName);
    assertTrue(channelName.length() <= 128);
  }

  @Test
  void testMakeChannelName_VeryLongNamesThrowException() {
    // Very long names should throw exception
    String veryLongConnector = "c".repeat(200);
    String veryLongTopic = "t".repeat(200);
    int partition = 12345;

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> makeChannelName(veryLongConnector, veryLongTopic, partition));

    assertTrue(exception.getMessage().contains("exceeds maximum length"));
    assertTrue(exception.getMessage().contains("200 chars")); // connector length
  }

  @Test
  void testMakeChannelName_EdgeCaseEmptyStrings() {
    // Test with empty strings (should still work)
    String channelName = makeChannelName("", "", 0);
    assertEquals("__0", channelName);
    assertTrue(channelName.length() <= 128);
  }

  @Test
  void testMakeChannelName_RealWorldExample() {
    // Real-world example with typical lengths
    String connector = "kafka-snowflake-streaming-prod-east-1";
    String topic = "com.company.analytics.user.events.v2";
    int partition = 42;

    String channelName = makeChannelName(connector, topic, partition);
    assertEquals(
        "kafka-snowflake-streaming-prod-east-1_com.company.analytics.user.events.v2_42",
        channelName);
    assertTrue(channelName.length() <= 128);
  }
}
