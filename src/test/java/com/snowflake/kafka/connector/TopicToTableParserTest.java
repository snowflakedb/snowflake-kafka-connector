package com.snowflake.kafka.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class TopicToTableParserTest {

  @Test
  public void testParseEmptyInput() {
    assertTrue(TopicToTableParser.parse("").isEmpty());
    assertTrue(TopicToTableParser.parse("   ").isEmpty());
  }

  @Test
  public void testParseMultipleEntries() {
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("topic_a", "TABLE_A");
    expected.put("topic_b", "TABLE_B");

    assertEquals(expected, TopicToTableParser.parse("topic_a:table_a, topic_b:table_b"));
  }

  @Test
  public void testParseQuotedEntries() {
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("topic:one", "table,one");
    expected.put("topic two", "table two");

    assertEquals(
        expected,
        TopicToTableParser.parse("\"topic:one\":\"table,one\", \"topic two\":\"table two\""));
  }

  @Test
  public void testParseEntriesPreservesOrder() {
    List<TopicToTableParser.Entry> entries =
        new TopicToTableParser("first:one, second:two").parseEntries();

    assertEquals(2, entries.size());
    assertEquals("first", entries.get(0).getTopic());
    assertEquals("ONE", entries.get(0).getTable());
    assertEquals("second", entries.get(1).getTopic());
    assertEquals("TWO", entries.get(1).getTable());
  }

  @Test
  public void testParseUppercasesOnlyUnquotedTableTokens() {
    Map<String, String> expected = new LinkedHashMap<>();
    expected.put("topic", "E");
    expected.put("other_topic", "e");

    assertEquals(expected, TopicToTableParser.parse("topic:e, other_topic:\"e\""));
  }

  @Test
  public void testParseRejectsDuplicateTopics() {
    IllegalArgumentException error = assertParseError("topic:one, topic:two");
    assertEquals("Duplicate topic: topic", error.getMessage());
  }

  @Test
  public void testParseRejectsOverlappingRegexes() {
    IllegalArgumentException error = assertParseError(".*:table_a, .*foo:table_b");
    assertTrue(error.getMessage().contains("Topic regexes cannot overlap"));
    assertTrue(error.getMessage().contains(".*"));
    assertTrue(error.getMessage().contains(".*foo"));
  }

  @Test
  public void testParseRejectsUnterminatedQuotedToken() {
    IllegalArgumentException error = assertParseError("\"topic:table");
    assertTrue(error.getMessage().contains("Unterminated quoted token"));
  }

  @Test
  public void testParseRejectsEmptyQuotedToken() {
    IllegalArgumentException error = assertParseError("\"\":table");
    assertTrue(error.getMessage().contains("Empty quoted token"));
  }

  @Test
  public void testParseRejectsMissingColon() {
    IllegalArgumentException error = assertParseError("topic table");
    assertTrue(error.getMessage().contains("Expected ':'"));
  }

  private static IllegalArgumentException assertParseError(String input) {
    try {
      TopicToTableParser.parse(input);
      fail("Expected IllegalArgumentException");
      return null;
    } catch (IllegalArgumentException error) {
      return error;
    }
  }
}
