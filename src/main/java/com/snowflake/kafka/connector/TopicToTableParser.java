package com.snowflake.kafka.connector;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class TopicToTableParser {
  private final String input;
  private int index;

  TopicToTableParser(String input) {
    this.input = input;
  }

  public static Map<String, String> parse(String input) {
    List<Entry> entries = new TopicToTableParser(input).parseEntries();
    Map<String, String> result = new LinkedHashMap<>();
    for (Entry entry : entries) {
      String newTopic = entry.getTopic();
      if (result.containsKey(newTopic)) {
        throw new IllegalArgumentException("Duplicate topic: " + newTopic);
      }

      // Check that regexes don't overlap.
      for (String topic : result.keySet()) {
        if (topic.matches(newTopic) || newTopic.matches(topic)) {
          throw new IllegalArgumentException(
              "Topic regexes cannot overlap. Overlapping regexes: " + topic + ", " + newTopic);
        }
      }

      result.put(newTopic, entry.getTable());
    }
    return result;
  }

  public List<Entry> parseEntries() {
    List<Entry> entries = new ArrayList<>();

    while (true) {
      skipWhitespace();
      if (isAtEnd()) {
        return entries;
      }

      String topic = parseToken(false);
      skipWhitespace();
      expect(':');
      skipWhitespace();
      String table = parseToken(true);
      entries.add(new Entry(topic, table));

      skipWhitespace();
      if (isAtEnd()) {
        return entries;
      }
      expect(',');
    }
  }

  private String parseToken(boolean uppercaseIfUnquoted) {
    if (isAtEnd()) {
      throw error("Expected token, found end of input");
    }

    if (input.charAt(index) == '"') {
      return parseQuotedToken();
    }
    if (uppercaseIfUnquoted) {
      return parseUnquotedToken().toUpperCase(Locale.ROOT);
    } else {
      return parseUnquotedToken();
    }
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

  public static final class Entry {
    private final String topic;
    private final String table;

    private Entry(String topic, String table) {
      this.topic = topic;
      this.table = table;
    }

    public String getTopic() {
      return topic;
    }

    public String getTable() {
      return table;
    }
  }
}
