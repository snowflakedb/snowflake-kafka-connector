package com.snowflake.kafka.connector.config;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

/**
 * Validated table options for an auto-created Snowflake-managed Iceberg table.
 *
 * <p>The accepted grammar is a whitespace-separated sequence of {@code KEY = value} properties from
 * a fixed allowlist, plus at most one {@code CLUSTER BY (key, ...)} clause whose keys are column
 * paths with an optional {@code ::TYPE} cast. Every value is checked against the type of its key.
 * Statement delimiters, comment markers, backslashes, unknown options, duplicates and the
 * properties the connector always sets itself are rejected. {@link #toSql()} re-serializes the
 * parsed options from their typed form, so the input text never reaches the DDL verbatim.
 */
public final class IcebergCreateTableOptions {

  private static final String CONFIG_NAME =
      KafkaConnectorConfigParams.SNOWFLAKE_ICEBERG_CREATE_TABLE_OPTIONS;

  private enum ValueType {
    STRING,
    INTEGER,
    BOOLEAN,
    STORAGE_SERIALIZATION_POLICY
  }

  private static final Map<String, ValueType> SUPPORTED_OPTIONS = new LinkedHashMap<>();

  static {
    SUPPORTED_OPTIONS.put("EXTERNAL_VOLUME", ValueType.STRING);
    SUPPORTED_OPTIONS.put("BASE_LOCATION", ValueType.STRING);
    SUPPORTED_OPTIONS.put("ICEBERG_VERSION", ValueType.INTEGER);
    SUPPORTED_OPTIONS.put("TARGET_FILE_SIZE", ValueType.STRING);
    SUPPORTED_OPTIONS.put("STORAGE_SERIALIZATION_POLICY", ValueType.STORAGE_SERIALIZATION_POLICY);
    SUPPORTED_OPTIONS.put("CATALOG_SYNC", ValueType.STRING);
    SUPPORTED_OPTIONS.put("DATA_RETENTION_TIME_IN_DAYS", ValueType.INTEGER);
    SUPPORTED_OPTIONS.put("MAX_DATA_EXTENSION_TIME_IN_DAYS", ValueType.INTEGER);
    SUPPORTED_OPTIONS.put("CHANGE_TRACKING", ValueType.BOOLEAN);
    SUPPORTED_OPTIONS.put("ENABLE_DATA_COMPACTION", ValueType.BOOLEAN);
    SUPPORTED_OPTIONS.put("ICEBERG_MERGE_ON_READ_BEHAVIOR", ValueType.STRING);
    SUPPORTED_OPTIONS.put("COMMENT", ValueType.STRING);
  }

  /** Properties appended to every managed-Iceberg CREATE by the connector. */
  private static final Set<String> CONNECTOR_OWNED_OPTIONS =
      Collections.unmodifiableSet(
          new TreeSet<>(Arrays.asList("CATALOG", "ENABLE_SCHEMA_EVOLUTION", "ERROR_LOGGING")));

  private static final Set<String> STORAGE_SERIALIZATION_POLICIES =
      Collections.unmodifiableSet(new TreeSet<>(Arrays.asList("COMPATIBLE", "OPTIMIZED")));

  private static final List<String> FORBIDDEN_COMMENT_MARKERS =
      Arrays.asList("--", "/*", "*/", "//");

  private static final IcebergCreateTableOptions EMPTY =
      new IcebergCreateTableOptions(Collections.emptyList(), new LinkedHashMap<>());

  /** Each element is one canonical cluster key, e.g. {@code RECORD_METADATA:"offset"::NUMBER}. */
  private final List<String> clusterBy;

  /** Option name (upper case) to its canonical SQL value, in input order. */
  private final Map<String, String> properties;

  private IcebergCreateTableOptions(List<String> clusterBy, Map<String, String> properties) {
    this.clusterBy = Collections.unmodifiableList(clusterBy);
    this.properties = Collections.unmodifiableMap(properties);
  }

  /**
   * Parses and validates the raw option string. A null or blank input yields empty options.
   *
   * @throws IllegalArgumentException if the input does not match the supported grammar
   */
  public static IcebergCreateTableOptions parse(String raw) {
    if (raw == null || raw.trim().isEmpty()) {
      return EMPTY;
    }
    if (raw.indexOf(';') >= 0) {
      throw invalid("statement delimiter ';' is not allowed");
    }
    for (String marker : FORBIDDEN_COMMENT_MARKERS) {
      if (raw.contains(marker)) {
        throw invalid("comment marker '" + marker + "' is not allowed");
      }
    }
    return new Parser(new Lexer(raw).tokenize()).parse();
  }

  /** The requested Iceberg format version, if {@code ICEBERG_VERSION} is set. */
  public Optional<Integer> icebergVersion() {
    return Optional.ofNullable(properties.get("ICEBERG_VERSION")).map(Integer::parseInt);
  }

  /**
   * Canonical SQL for the options: {@code CLUSTER BY (...)} first, then each property as {@code KEY
   * = value} in input order. Empty when no options are set.
   */
  public String toSql() {
    List<String> parts = new ArrayList<>();
    if (!clusterBy.isEmpty()) {
      parts.add("CLUSTER BY (" + String.join(", ", clusterBy) + ")");
    }
    properties.forEach((key, value) -> parts.add(key + " = " + value));
    return String.join(" ", parts);
  }

  @Override
  public String toString() {
    return toSql();
  }

  private static IllegalArgumentException invalid(String reason) {
    return new IllegalArgumentException("Invalid " + CONFIG_NAME + ": " + reason + ".");
  }

  private static String quoteString(String value) {
    return "'" + value.replace("'", "''") + "'";
  }

  private static String quoteIdentifier(String value) {
    return "\"" + value.replace("\"", "\"\"") + "\"";
  }

  private enum TokenType {
    WORD,
    QUOTED_IDENTIFIER,
    STRING,
    INTEGER,
    EQUALS,
    LEFT_PAREN,
    RIGHT_PAREN,
    COMMA,
    COLON,
    DOUBLE_COLON,
    DOT
  }

  private static final class Token {
    final TokenType type;

    /** Unescaped content for quoted tokens, the source text otherwise. */
    final String text;

    Token(TokenType type, String text) {
      this.type = type;
      this.text = text;
    }

    String describe() {
      switch (type) {
        case STRING:
          return "string literal " + quoteString(text);
        case QUOTED_IDENTIFIER:
          return "quoted identifier " + quoteIdentifier(text);
        default:
          return "'" + text + "'";
      }
    }
  }

  private static final class Lexer {
    private final String input;
    private int pos = 0;

    Lexer(String input) {
      this.input = input;
    }

    List<Token> tokenize() {
      List<Token> tokens = new ArrayList<>();
      while (true) {
        while (pos < input.length() && isAsciiWhitespace(input.charAt(pos))) {
          pos++;
        }
        if (pos >= input.length()) {
          return tokens;
        }
        tokens.add(next());
      }
    }

    private Token next() {
      char c = input.charAt(pos);
      if (isWordStart(c)) {
        int start = pos;
        while (pos < input.length() && isWordPart(input.charAt(pos))) {
          pos++;
        }
        return new Token(TokenType.WORD, input.substring(start, pos));
      }
      if (c >= '0' && c <= '9') {
        int start = pos;
        while (pos < input.length() && input.charAt(pos) >= '0' && input.charAt(pos) <= '9') {
          pos++;
        }
        return new Token(TokenType.INTEGER, input.substring(start, pos));
      }
      switch (c) {
        case '\'':
          return new Token(TokenType.STRING, readQuoted('\''));
        case '"':
          return new Token(TokenType.QUOTED_IDENTIFIER, readQuoted('"'));
        case '=':
          return single(TokenType.EQUALS);
        case '(':
          return single(TokenType.LEFT_PAREN);
        case ')':
          return single(TokenType.RIGHT_PAREN);
        case ',':
          return single(TokenType.COMMA);
        case '.':
          return single(TokenType.DOT);
        case ':':
          if (pos + 1 < input.length() && input.charAt(pos + 1) == ':') {
            pos += 2;
            return new Token(TokenType.DOUBLE_COLON, "::");
          }
          return single(TokenType.COLON);
        default:
          throw invalid("unexpected character '" + c + "' at position " + pos);
      }
    }

    private Token single(TokenType type) {
      return new Token(type, String.valueOf(input.charAt(pos++)));
    }

    /**
     * Reads a token enclosed in {@code quote}, where a doubled quote stands for one quote
     * character. Backslashes and control characters are rejected so that the token has exactly one
     * interpretation.
     */
    private String readQuoted(char quote) {
      int start = pos;
      pos++;
      StringBuilder value = new StringBuilder();
      while (pos < input.length()) {
        char c = input.charAt(pos);
        if (c == quote) {
          if (pos + 1 < input.length() && input.charAt(pos + 1) == quote) {
            value.append(quote);
            pos += 2;
            continue;
          }
          pos++;
          return value.toString();
        }
        if (c == '\\') {
          throw invalid("backslash is not allowed (at position " + pos + ")");
        }
        if (Character.isISOControl(c)) {
          throw invalid("control character is not allowed (at position " + pos + ")");
        }
        value.append(c);
        pos++;
      }
      throw invalid("unterminated " + quote + " quote starting at position " + start);
    }

    private static boolean isAsciiWhitespace(char c) {
      return c == ' ' || c == '\t' || c == '\n' || c == '\r';
    }

    private static boolean isWordStart(char c) {
      return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || c == '_';
    }

    private static boolean isWordPart(char c) {
      return isWordStart(c) || (c >= '0' && c <= '9') || c == '$';
    }
  }

  private static final class Parser {
    private final List<Token> tokens;
    private int pos = 0;
    private final List<String> clusterBy = new ArrayList<>();
    private final Map<String, String> properties = new LinkedHashMap<>();

    Parser(List<Token> tokens) {
      this.tokens = tokens;
    }

    IcebergCreateTableOptions parse() {
      while (pos < tokens.size()) {
        Token token = expect(TokenType.WORD, "an option name");
        String name = token.text.toUpperCase(Locale.ROOT);
        if (name.equals("CLUSTER")) {
          parseClusterBy();
        } else {
          parseProperty(name);
        }
      }
      return new IcebergCreateTableOptions(clusterBy, properties);
    }

    private void parseClusterBy() {
      if (!clusterBy.isEmpty()) {
        throw invalid("CLUSTER BY is specified more than once");
      }
      Token by = expect(TokenType.WORD, "BY after CLUSTER");
      if (!by.text.equalsIgnoreCase("BY")) {
        throw invalid("expected BY after CLUSTER but found " + by.describe());
      }
      expect(TokenType.LEFT_PAREN, "'(' after CLUSTER BY");
      do {
        clusterBy.add(parseClusterKey());
      } while (accept(TokenType.COMMA));
      expect(TokenType.RIGHT_PAREN, "',' or ')' in CLUSTER BY");
    }

    /** A column path such as {@code a.b:"c"} with an optional {@code ::TYPE[(n[, m])]} cast. */
    private String parseClusterKey() {
      StringBuilder key = new StringBuilder(parseIdentifier());
      while (peekIs(TokenType.COLON) || peekIs(TokenType.DOT)) {
        key.append(tokens.get(pos++).text).append(parseIdentifier());
      }
      if (accept(TokenType.DOUBLE_COLON)) {
        key.append("::")
            .append(expect(TokenType.WORD, "a type name after '::'").text.toUpperCase(Locale.ROOT));
        if (accept(TokenType.LEFT_PAREN)) {
          key.append('(').append(expect(TokenType.INTEGER, "a type argument").text);
          if (accept(TokenType.COMMA)) {
            key.append(", ").append(expect(TokenType.INTEGER, "a type argument").text);
          }
          expect(TokenType.RIGHT_PAREN, "')' after type arguments");
          key.append(')');
        }
      }
      return key.toString();
    }

    private String parseIdentifier() {
      Token token = next("a column name");
      if (token.type == TokenType.WORD) {
        return token.text;
      }
      if (token.type == TokenType.QUOTED_IDENTIFIER) {
        return quoteIdentifier(token.text);
      }
      throw invalid("expected a column name but found " + token.describe());
    }

    private void parseProperty(String name) {
      if (CONNECTOR_OWNED_OPTIONS.contains(name)) {
        throw invalid(name + " is set by the connector and must not be specified");
      }
      ValueType type = SUPPORTED_OPTIONS.get(name);
      if (type == null) {
        throw invalid(
            "unsupported option "
                + name
                + ". Supported options: CLUSTER BY, "
                + String.join(", ", SUPPORTED_OPTIONS.keySet()));
      }
      if (properties.containsKey(name)) {
        throw invalid(name + " is specified more than once");
      }
      expect(TokenType.EQUALS, "'=' after " + name);
      Token value = next("a value for " + name);
      properties.put(name, canonicalValue(name, type, value));
    }

    private static String canonicalValue(String name, ValueType type, Token value) {
      switch (type) {
        case STRING:
          if (value.type != TokenType.STRING) {
            throw invalid(name + " requires a single-quoted string but found " + value.describe());
          }
          return quoteString(value.text);
        case INTEGER:
          String digits = unquotedOrString(value);
          if (!digits.matches("[0-9]{1,9}")) {
            throw invalid(name + " requires a non-negative integer but found " + value.describe());
          }
          return String.valueOf(Integer.parseInt(digits));
        case BOOLEAN:
          String bool = unquotedOrString(value).toUpperCase(Locale.ROOT);
          if (!bool.equals("TRUE") && !bool.equals("FALSE")) {
            throw invalid(name + " requires TRUE or FALSE but found " + value.describe());
          }
          return bool;
        case STORAGE_SERIALIZATION_POLICY:
          String policy = unquotedOrString(value).toUpperCase(Locale.ROOT);
          if (!STORAGE_SERIALIZATION_POLICIES.contains(policy)) {
            throw invalid(
                name
                    + " requires one of "
                    + String.join(", ", STORAGE_SERIALIZATION_POLICIES)
                    + " but found "
                    + value.describe());
          }
          return policy;
        default:
          throw new IllegalStateException("unhandled value type: " + type);
      }
    }

    /** Keyword-like values may be written bare or as a string literal. */
    private static String unquotedOrString(Token value) {
      if (value.type == TokenType.WORD
          || value.type == TokenType.INTEGER
          || value.type == TokenType.STRING) {
        return value.text;
      }
      return "";
    }

    private Token next(String expected) {
      if (pos >= tokens.size()) {
        throw invalid("expected " + expected + " but reached the end of the input");
      }
      return tokens.get(pos++);
    }

    private Token expect(TokenType type, String expected) {
      Token token = next(expected);
      if (token.type != type) {
        throw invalid("expected " + expected + " but found " + token.describe());
      }
      return token;
    }

    private boolean peekIs(TokenType type) {
      return pos < tokens.size() && tokens.get(pos).type == type;
    }

    private boolean accept(TokenType type) {
      if (peekIs(type)) {
        pos++;
        return true;
      }
      return false;
    }
  }
}
