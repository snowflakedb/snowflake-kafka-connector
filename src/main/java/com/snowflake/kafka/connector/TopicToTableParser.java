package com.snowflake.kafka.connector;

import java.util.ArrayList;
import java.util.List;

public class TopicToTableParser {
  private final String input;
  private int index;

  TopicToTableParser(String input) {
    this.input = input;
  }

  /**
   * Parse the topic2table config string into validated entries. Resolvers are responsible for
   * turning these entries into a concrete {@link TopicToTableResolver}.
   */
  public static List<Entry> parseAndValidate(String input) {
    List<Entry> entries = new TopicToTableParser(input).parseEntries();
    List<String> seenTopics = new ArrayList<>();
    for (Entry entry : entries) {
      String newTopic = entry.getTopic();
      if (seenTopics.contains(newTopic)) {
        throw new IllegalArgumentException("Duplicate topic: " + newTopic);
      }

      // Check that regexes don't overlap.
      for (String topic : seenTopics) {
        if (topic.matches(newTopic) || newTopic.matches(topic)) {
          throw new IllegalArgumentException(
              "Topic regexes cannot overlap. Overlapping regexes: " + topic + ", " + newTopic);
        }
      }

      seenTopics.add(newTopic);
    }
    return entries;
  }

  public List<Entry> parseEntries() {
    List<Entry> entries = new ArrayList<>();

    while (true) {
      skipWhitespace();
      if (isAtEnd()) {
        return entries;
      }

      Token topic = parseToken();
      skipWhitespace();
      expect(':');
      skipWhitespace();
      Token table = parseToken();
      // Only the table token drives uppercasing: unquoted tables are uppercased.
      // Quotes around topics are discarded.
      entries.add(new Entry(topic.text, table.text, !table.quoted));

      skipWhitespace();
      if (isAtEnd()) {
        return entries;
      }
      expect(',');
    }
  }

  private Token parseToken() {
    if (isAtEnd()) {
      throw error("Expected token, found end of input");
    }

    if (input.charAt(index) == '"') {
      return new Token(parseQuotedToken(), true);
    }
    return new Token(parseUnquotedToken(), false);
  }

  private String parseQuotedToken() {
    index++; // opening quote
    int textStart = index;

    while (!isAtEnd() && input.charAt(index) != '"') {
      index++;
    }

    if (isAtEnd()) {
      throw error("Unterminated quoted token");
    }
    if (index == textStart) {
      throw error("Empty quoted token");
    }

    String text = input.substring(textStart, index);
    index++; // closing quote
    return text;
  }

  private String parseUnquotedToken() {
    int start = index;
    while (!isAtEnd()) {
      char character = input.charAt(index);
      if (Character.isWhitespace(character)
          || character == ':'
          || character == ','
          || character == '"') {
        break;
      }
      index++;
    }

    if (index == start) {
      throw error("Expected token");
    }

    return input.substring(start, index);
  }

  private void skipWhitespace() {
    while (!isAtEnd() && Character.isWhitespace(input.charAt(index))) {
      index++;
    }
  }

  private void expect(char expectedCharacter) {
    if (isAtEnd() || input.charAt(index) != expectedCharacter) {
      throw error("Expected '" + expectedCharacter + "'");
    }
    index++;
  }

  private boolean isAtEnd() {
    return index >= input.length();
  }

  private IllegalArgumentException error(String message) {
    StringBuilder sb = new StringBuilder();
    sb.append(message);
    sb.append(" at position ");
    sb.append(index);
    sb.append(": \"");
    sb.append(input);
    sb.append("\". Format: <topic-1>:<table-1>,<topic-2>:\"<table-2>\",...");
    return new IllegalArgumentException(sb.toString());
  }

  /** A single parsed token plus whether it was written as a quoted ("...") token. */
  private static final class Token {
    final String text;
    final boolean quoted;

    Token(String text, boolean quoted) {
      this.text = text;
      this.quoted = quoted;
    }
  }

  public static final class Entry {
    private final String topic;
    private final String table;
    private final boolean shouldUppercase;

    private Entry(String topic, String table, boolean shouldUppercase) {
      this.topic = topic;
      this.table = table;
      this.shouldUppercase = shouldUppercase;
    }

    public String getTopic() {
      return topic;
    }

    /** Returns the raw table token as written in the config (never uppercased by the parser). */
    public String getTable() {
      return table;
    }

    /** Whether the resolved table name should be uppercased (true for unquoted table tokens). */
    public boolean shouldUppercase() {
      return shouldUppercase;
    }
  }
}
