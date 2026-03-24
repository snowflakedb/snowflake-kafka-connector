package com.snowflake.kafka.connector.internal.validation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class SqlIdentifierNormalizerTest {

  @ParameterizedTest
  @CsvSource({
    // Unquoted → uppercased
    "city, CITY",
    "myCol, MYCOL",
    "ABC, ABC",
    "a_b_c, A_B_C",
    // Unquoted with escaped spaces
    "col\\ name, COL NAME",
  })
  public void testUnquotedIdentifiers(String input, String expected) {
    assertEquals(expected, SqlIdentifierNormalizer.normalizeSqlIdentifier(input));
  }

  @ParameterizedTest
  @CsvSource({
    // Quoted → strip quotes, preserve case
    "'\"city\"', city",
    "'\"MyCol\"', MyCol",
    "'\"ABC\"', ABC",
    "'\"col name\"', col name",
    // Quoted with escaped double-quotes
    "'\"col\"\"name\"', col\"name",
    "'\"\"\"city\"\"\"', \"city\"",
  })
  public void testQuotedIdentifiers(String input, String expected) {
    assertEquals(expected, SqlIdentifierNormalizer.normalizeSqlIdentifier(input));
  }

  @Test
  public void testEmptyString() {
    assertEquals("", SqlIdentifierNormalizer.normalizeSqlIdentifier(""));
  }

  @Test
  public void testSingleChar() {
    assertEquals("A", SqlIdentifierNormalizer.normalizeSqlIdentifier("a"));
  }

  @Test
  public void testSingleQuote() {
    // A single double-quote char is not a valid quoted identifier — treated as unquoted
    assertEquals("\"", SqlIdentifierNormalizer.normalizeSqlIdentifier("\""));
  }

  @Test
  public void testEmptyQuotedIdentifier() {
    assertEquals("", SqlIdentifierNormalizer.normalizeSqlIdentifier("\"\""));
  }

  @Test
  public void testCacheReturnsSameResult() {
    String first = SqlIdentifierNormalizer.normalizeSqlIdentifier("cached_test");
    String second = SqlIdentifierNormalizer.normalizeSqlIdentifier("cached_test");
    assertEquals(first, second);
    assertEquals("CACHED_TEST", first);
  }
}
