package com.snowflake.kafka.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import org.junit.Test;

public class TopicToTableParserTest {

  private static TopicToTableResolver staticResolverFrom(String input) {
    List<TopicToTableParser.Entry> entries = TopicToTableParser.parseAndValidate(input);
    return StaticTopicToTableResolver.from(entries);
  }

  @Test
  public void testParseEmptyInput() {
    assertNull(staticResolverFrom("").resolve("anything"));
    assertNull(staticResolverFrom("   ").resolve("anything"));
  }

  @Test
  public void testParseMultipleEntries() {
    TopicToTableResolver resolver = staticResolverFrom("topic_a:table_a, topic_b:table_b");

    assertEquals("TABLE_A", resolver.resolve("topic_a"));
    assertEquals("TABLE_B", resolver.resolve("topic_b"));
    assertNull(resolver.resolve("topic_c"));
  }

  @Test
  public void testParseQuotedEntries() {
    TopicToTableResolver resolver =
        staticResolverFrom("\"topic:one\":\"table,one\", \"topic two\":\"table two\"");

    assertEquals("table,one", resolver.resolve("topic:one"));
    assertEquals("table two", resolver.resolve("topic two"));
  }

  @Test
  public void testParseEntriesPreservesOrder() {
    List<TopicToTableParser.Entry> entries =
        new TopicToTableParser("first:one, second:two").parseEntries();

    assertEquals(2, entries.size());
    assertEquals("first", entries.get(0).getTopic());
    assertEquals("one", entries.get(0).getTable());
    assertTrue(entries.get(0).shouldUppercase());
    assertEquals("second", entries.get(1).getTopic());
    assertEquals("two", entries.get(1).getTable());
    assertTrue(entries.get(1).shouldUppercase());
  }

  @Test
  public void testParseEntriesTracksQuotedStatus() {
    List<TopicToTableParser.Entry> entries =
        new TopicToTableParser("topic:unquoted, other:\"Quoted\"").parseEntries();

    assertEquals(2, entries.size());
    assertTrue(entries.get(0).shouldUppercase());
    assertEquals("unquoted", entries.get(0).getTable());
    assertFalse(entries.get(1).shouldUppercase());
    assertEquals("Quoted", entries.get(1).getTable());
  }

  @Test
  public void testParseUppercasesOnlyUnquotedTableTokens() {
    TopicToTableResolver resolver = staticResolverFrom("topic:e, other_topic:\"e\"");

    assertEquals("E", resolver.resolve("topic"));
    assertEquals("e", resolver.resolve("other_topic"));
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
      TopicToTableParser.parseAndValidate(input);
      fail("Expected IllegalArgumentException");
      return null;
    } catch (IllegalArgumentException error) {
      return error;
    }
  }
}
