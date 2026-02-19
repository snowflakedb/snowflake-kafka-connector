package com.snowflake.kafka.connector;

import static java.util.Arrays.*;
import static java.util.Collections.*;
import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

public class UtilsTest {
  @Rule public final EnvironmentVariables environmentVariables = new EnvironmentVariables();

  @Test
  public void testGetTopicToTableMap() {
    // no map
    Map<String, String> config = new HashMap<>();
    Map<String, String> result = SnowflakeSinkTask.getTopicToTableMap(config);
    assert result.isEmpty();

    // has map
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP, "aaa:bbb," + "ccc:ddd");
    result = SnowflakeSinkTask.getTopicToTableMap(config);
    assert result.size() == 2;
    assert result.containsKey("aaa");
    assert result.get("aaa").equals("bbb");
    assert result.containsKey("ccc");
    assert result.get("ccc").equals("ddd");

    // has map, but invalid data
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP, "12321");
    result = SnowflakeSinkTask.getTopicToTableMap(config);
    assert result.isEmpty();
  }

  @Test
  public void testObjectIdentifier() {
    String name = "DATABASE.SCHEMA.TABLE";
    assert !Utils.isValidSnowflakeObjectIdentifier(name);
    String name1 = "table!@#$%^;()";
    assert !Utils.isValidSnowflakeObjectIdentifier(name1);
  }

  @Test
  public void testVersionChecker() {
    assert Utils.checkConnectorVersion();
  }

  @Test
  public void testParseTopicToTable() {
    TestUtils.assertError(SnowflakeErrors.ERROR_0021, () -> Utils.parseTopicToTableMap("adsadas"));

    TestUtils.assertError(
        SnowflakeErrors.ERROR_0021, () -> Utils.parseTopicToTableMap("abc:@123,bvd:adsa"));
  }

  @Test
  public void testParseTopicToTableRegex() {
    String catTable = "cat_table";
    String dogTable = "dog_table";
    String catTopicRegex = ".*_cat";
    String dogTopicRegex = ".*_dog";

    // test two different regexs
    Map<String, String> topic2table =
        Utils.parseTopicToTableMap(
            Utils.formatString("{}:{},{}:{}", catTopicRegex, catTable, dogTopicRegex, dogTable));
    assert topic2table.containsKey(catTopicRegex);
    assert topic2table.containsKey(dogTopicRegex);
    assert topic2table.containsValue(catTable);
    assert topic2table.containsValue(dogTable);
    assert topic2table.size() == 2;

    // error: overlapping regex, same table
    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0021,
        () ->
            Utils.parseTopicToTableMap(
                Utils.formatString(
                    "{}:{},{}:{}", catTopicRegex, catTable, "big_" + catTopicRegex, catTable)));

    // error: overlapping regex, different table
    assert TestUtils.assertError(
        SnowflakeErrors.ERROR_0021,
        () ->
            Utils.parseTopicToTableMap(
                Utils.formatString(
                    "{}:{},{}:{}",
                    catTopicRegex,
                    catTable,
                    dogTopicRegex + catTopicRegex,
                    dogTable)));
  }

  @Test
  public void testGetTableName() {
    Map<String, String> topic2table = Utils.parseTopicToTableMap("ab@cd:abcd, 1234:_1234");

    assert Utils.getTableName("ab@cd", topic2table, false).equals("abcd");
    assert Utils.getTableName("1234", topic2table, false).equals("_1234");

    TestUtils.assertError(SnowflakeErrors.ERROR_0020, () -> Utils.getTableName("", topic2table, false));
    TestUtils.assertError(SnowflakeErrors.ERROR_0020, () -> Utils.getTableName(null, topic2table, false));

    String topic = "bc*def";
    assert Utils.getTableName(topic, topic2table, false).equals("bc_def_" + Math.abs(topic.hashCode()));

    topic = "12345";
    assert Utils.getTableName(topic, topic2table, false).equals("_12345_" + Math.abs(topic.hashCode()));
  }

  @Test
  public void testGenerateTableName() {
    Map<String, String> topic2table = Utils.parseTopicToTableMap("ab@cd:abcd, 1234:_1234");

    String topic0 = "ab@cd";
    Utils.GeneratedName generatedTableName1 = Utils.generateTableName(topic0, topic2table, false);
    assertEquals("abcd", generatedTableName1.getName());
    assertTrue(generatedTableName1.isNameFromMap());

    String topic1 = "1234";
    Utils.GeneratedName generatedTableName2 = Utils.generateTableName(topic1, topic2table, false);
    assertEquals("_1234", generatedTableName2.getName());
    assertTrue(generatedTableName2.isNameFromMap());

    String topic2 = "bc*def";
    Utils.GeneratedName generatedTableName3 = Utils.generateTableName(topic2, topic2table, false);
    assertEquals("bc_def_" + Math.abs(topic2.hashCode()), generatedTableName3.getName());
    assertFalse(generatedTableName3.isNameFromMap());

    String topic3 = "12345";
    Utils.GeneratedName generatedTableName4 = Utils.generateTableName(topic3, topic2table, false);
    assertEquals("_12345_" + Math.abs(topic3.hashCode()), generatedTableName4.getName());
    assertFalse(generatedTableName4.isNameFromMap());

    TestUtils.assertError(
        SnowflakeErrors.ERROR_0020, () -> Utils.generateTableName("", topic2table, false));
    //noinspection DataFlowIssue
    TestUtils.assertError(
        SnowflakeErrors.ERROR_0020, () -> Utils.generateTableName(null, topic2table, false));
  }

  @Test
  public void testGetTableNameRegex() {
    String catTable = "cat_table";
    String dogTable = "dog_table";
    String catTopicRegex = ".*_cat";
    String dogTopicRegex = ".*_dog";

    // test two different regexs
    Map<String, String> topic2table =
        Utils.parseTopicToTableMap(
            Utils.formatString("{}:{},{}:{}", catTopicRegex, catTable, dogTopicRegex, dogTable));

    assert Utils.getTableName("calico_cat", topic2table, false).equals(catTable);
    assert Utils.getTableName("orange_cat", topic2table, false).equals(catTable);
    assert Utils.getTableName("_cat", topic2table, false).equals(catTable);
    assert Utils.getTableName("corgi_dog", topic2table, false).equals(dogTable);

    // test new topic should not have wildcard
    String topic = "bird.*";
    assert Utils.getTableName(topic, topic2table, false).equals("bird_" + Math.abs(topic.hashCode()));
  }

  @Test
  public void testTableFullName() {
    assert Utils.isValidSnowflakeTableName("_1342dfsaf$");
    assert Utils.isValidSnowflakeTableName("dad._1342dfsaf$");
    assert Utils.isValidSnowflakeTableName("adsa123._gdgsdf._1342dfsaf$");
    assert !Utils.isValidSnowflakeTableName("_13)42dfsaf$");
    assert !Utils.isValidSnowflakeTableName("_13.42dfsaf$");
    assert !Utils.isValidSnowflakeTableName("_1342.df.sa.f$");
  }

  @Test
  public void testConvertAppName() {
    HashMap<String, String> config = new HashMap<String, String>();

    config.put(KafkaConnectorConfigParams.NAME, "_aA1");
    Utils.convertAppName(config);
    assert config.get(KafkaConnectorConfigParams.NAME).equals("_aA1");

    config.put(KafkaConnectorConfigParams.NAME, "-_aA1");
    Utils.convertAppName(config);
    assert config.get(KafkaConnectorConfigParams.NAME).equals("___aA1_44483871");

    config.put(KafkaConnectorConfigParams.NAME, "_aA1-");
    Utils.convertAppName(config);
    assert config.get(KafkaConnectorConfigParams.NAME).equals("_aA1__90688251");

    config.put(KafkaConnectorConfigParams.NAME, "testApp.snowflake-connector");
    Utils.convertAppName(config);
    assert config
        .get(KafkaConnectorConfigParams.NAME)
        .equals("testApp_snowflake_connector_36242259");
  }

  @Test
  public void testIsValidSnowflakeApplicationName() {
    assert Utils.isValidSnowflakeApplicationName("-_aA1");
    assert Utils.isValidSnowflakeApplicationName("aA_1-");
    assert !Utils.isValidSnowflakeApplicationName("1aA_-");
    assert !Utils.isValidSnowflakeApplicationName("_1.a$");
    assert !Utils.isValidSnowflakeApplicationName("(1.f$-_");
  }

  @Test
  public void testLogMessageBasic() {
    // no variable
    String expected = Utils.SF_LOG_TAG + " test message";

    assert Utils.formatLogMessage("test message").equals(expected);

    // 1 variable
    expected = Utils.SF_LOG_TAG + " 1 test message";

    assert Utils.formatLogMessage("{} test message", 1).equals(expected);
  }

  @Test
  public void testLogMessageNulls() {
    // nulls
    String expected = Utils.SF_LOG_TAG + " null test message";
    assert Utils.formatLogMessage("{} test message", (String) null).equals(expected);

    expected = Utils.SF_LOG_TAG + " some string test null message null";
    assert Utils.formatLogMessage("{} test {} message {}", "some string", null, null)
        .equals(expected);
  }

  @Test
  public void testLogMessageMultiLines() {
    // 2 variables
    String expected = Utils.SF_LOG_TAG + " 1 test message\n" + "2 test message";

    System.out.println(Utils.formatLogMessage("{} test message\n{} test message", 1, 2));

    assert Utils.formatLogMessage("{} test message\n{} test message", 1, 2).equals(expected);

    // 3 variables
    expected = Utils.SF_LOG_TAG + " 1 test message\n" + "2 test message\n" + "3 test message";

    assert Utils.formatLogMessage("{} test message\n{} test message\n{} test " + "message", 1, 2, 3)
        .equals(expected);

    // 4 variables
    expected =
        Utils.SF_LOG_TAG
            + " 1 test message\n"
            + "2 test message\n"
            + "3 test message\n"
            + "4 test message";

    assert Utils.formatLogMessage(
            "{} test message\n{} test message\n{} test " + "message\n{} test message", 1, 2, 3, 4)
        .equals(expected);
  }

  @Test
  public void testGetExceptionMessage() throws Exception {
    String customMessage = "customMessage";
    String exceptionMessage = "exceptionMessage";
    Exception cause = new Exception("cause");
    StackTraceElement[] stackTrace = new StackTraceElement[0];

    Exception nullMessageEx = new Exception();
    assert Utils.getExceptionMessage(customMessage, nullMessageEx)
        .equals(
            Utils.formatString(
                Utils.GET_EXCEPTION_FORMAT,
                customMessage,
                Utils.GET_EXCEPTION_MISSING_MESSAGE,
                Utils.GET_EXCEPTION_MISSING_CAUSE));

    Exception nullCauseEx = new Exception(exceptionMessage);
    nullCauseEx.initCause(null);
    assert Utils.getExceptionMessage(customMessage, nullCauseEx)
        .equals(
            Utils.formatString(
                Utils.GET_EXCEPTION_FORMAT,
                customMessage,
                exceptionMessage,
                Utils.GET_EXCEPTION_MISSING_CAUSE));

    Exception stacktraceEx = new Exception(exceptionMessage, cause);
    stacktraceEx.getCause().setStackTrace(stackTrace);
    assert Utils.getExceptionMessage(customMessage, stacktraceEx)
        .equals(
            Utils.formatString(Utils.GET_EXCEPTION_FORMAT, customMessage, exceptionMessage, "[]"));
  }

  @Test
  public void testQuoteNameIfNeeded() {
    assertEquals("\"ABC\"", Utils.quoteNameIfNeeded("abc"));
    assertEquals("\"abc\"", Utils.quoteNameIfNeeded("\"abc\""));
    assertEquals("\"ABC\"", Utils.quoteNameIfNeeded("ABC"));
    assertEquals("\"AL%$\"", Utils.quoteNameIfNeeded("al%$"));
  }

  @Test
  public void testSemanticVersionParsing() {
    // Test standard version parsing
    SemanticVersion version311 = new SemanticVersion("3.1.1");
    assertEquals(3, version311.major());
    assertEquals(1, version311.minor());
    assertEquals(1, version311.patch());
    assertFalse(version311.isReleaseCandidate());
    assertEquals("3.1.1", version311.originalVersion());

    // Test version with RC suffix
    SemanticVersion version400rc = new SemanticVersion("4.0.0-rc");
    assertEquals(4, version400rc.major());
    assertEquals(0, version400rc.minor());
    assertEquals(0, version400rc.patch());
    assertTrue(version400rc.isReleaseCandidate());
    assertEquals("4.0.0-rc", version400rc.originalVersion());

    // Test version with RC1 suffix
    SemanticVersion version401rc1 = new SemanticVersion("4.0.1-RC1");
    assertEquals(4, version401rc1.major());
    assertEquals(0, version401rc1.minor());
    assertEquals(1, version401rc1.patch());
    assertTrue(version401rc1.isReleaseCandidate());
    assertEquals("4.0.1-RC1", version401rc1.originalVersion());
  }

  @Test
  public void testSemanticVersionComparison() {
    SemanticVersion v310 = new SemanticVersion("3.1.0");
    SemanticVersion v311 = new SemanticVersion("3.1.1");
    SemanticVersion v320 = new SemanticVersion("3.2.0");
    SemanticVersion v400 = new SemanticVersion("4.0.0");
    SemanticVersion v401 = new SemanticVersion("4.0.1");
    SemanticVersion v501 = new SemanticVersion("5.0.1");

    // Test less than
    assertTrue(v310.compareTo(v311) < 0);
    assertTrue(v311.compareTo(v320) < 0);
    assertTrue(v320.compareTo(v400) < 0);
    assertTrue(v400.compareTo(v401) < 0);
    assertTrue(v310.compareTo(v501) < 0);

    // Test greater than
    assertTrue(v311.compareTo(v310) > 0);
    assertTrue(v320.compareTo(v311) > 0);
    assertTrue(v400.compareTo(v320) > 0);
    assertTrue(v401.compareTo(v400) > 0);
    assertTrue(v501.compareTo(v401) > 0);

    // Test equals
    SemanticVersion v311_2 = new SemanticVersion("3.1.1");
    assertEquals(0, v311.compareTo(v311_2));
    assertEquals(v311, v311_2);

    // Test RC versions are treated same as non-RC for comparison (major.minor.patch only)
    SemanticVersion v400rc = new SemanticVersion("4.0.0-rc");
    assertEquals(0, v400.compareTo(v400rc));
  }

  @Test
  public void testSemanticVersionInvalidFormat() {
    try {
      new SemanticVersion("invalid");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid version format"));
    }

    try {
      new SemanticVersion("1.2");
      fail("Should have thrown IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Invalid version format"));
    }
  }

  @Test
  public void testFindRecommendedVersion() {
    //  v4.0.0 should recommend v5.0.0 (highest available)
    List<String> availableVersions = asList("3.3.1", "4.0.0", "4.0.1", "4.1.0", "5.0.0");

    SemanticVersion current = new SemanticVersion("4.0.0");
    String recommended = Utils.findRecommendedVersion(current, availableVersions);

    assertEquals("5.0.0", recommended);
  }

  @Test
  public void testFindRecommendedVersionFiltersRCVersions() {
    // Scenario 3: Should not recommend RC versions
    List<String> availableVersions = asList("3.1.1", "3.2.0-rc", "3.2.0-RC1", "4.0.0-rc");

    SemanticVersion current = new SemanticVersion("4.1.1");
    String recommended = Utils.findRecommendedVersion(current, availableVersions);

    assertNull(recommended); // No stable version available newer than 3.1.1
  }

  @Test
  public void testFindRecommendedVersionNoUpgradeAvailable() {
    //  Current is already latest
    List<String> availableVersions = asList("4.1.0", "4.2.0", "4.3.1");

    SemanticVersion current = new SemanticVersion("4.3.1");
    String recommended = Utils.findRecommendedVersion(current, availableVersions);

    assertNull(recommended);
  }

  @Test
  public void testFindRecommendedVersionWithEmptyList() {
    //  Empty version list should return null
    List<String> availableVersions = emptyList();

    SemanticVersion current = new SemanticVersion("3.1.1");
    String recommended = Utils.findRecommendedVersion(current, availableVersions);

    assertNull(recommended);
  }

  @Test
  public void testFindRecommendedVersionWithInvalidVersions() {
    //  Invalid versions should be skipped
    List<String> availableVersions = asList("3.1.1", "invalid", "3.2.0", "bad.version", "3.3.0");

    SemanticVersion current = new SemanticVersion("3.1.1");
    String recommended = Utils.findRecommendedVersion(current, availableVersions);

    assertEquals("3.3.0", recommended);
  }

  @Test
  public void testFindRecommendedVersionOnlyRCVersionsAvailable() {
    //  Only RC versions newer than current - should return null
    List<String> availableVersions = asList("3.1.0", "3.1.1", "3.2.0-RC", "3.3.0-rc1");

    SemanticVersion current = new SemanticVersion("3.1.1");
    String recommended = Utils.findRecommendedVersion(current, availableVersions);

    assertNull(recommended);
  }

  /**
   * Reproducer for SNOW-2909010: KC4 does not support case-sensitive table names.
   *
   * <p>Demonstrates three failure modes when users attempt to use quoted/case-sensitive Snowflake
   * table names with the Kafka connector:
   *
   * <ol>
   *   <li>Config validation rejects quoted table names in topic2table.map
   *   <li>Topic names with special chars (needing quoting) get sanitized instead of quoted
   *   <li>Mixed-case names pass through unquoted, so Snowflake uppercases them
   * </ol>
   */
  @Test
  public void testCaseSensitiveQuotedTableNameNotSupported() {
    // --- Failure 1: isValidSnowflakeTableName rejects quoted identifiers ---
    // Quoted names like "My-Table" are valid Snowflake identifiers, but the connector's
    // regex validation does not allow double-quote characters at all.
    assertFalse(Utils.isValidSnowflakeTableName("\"My-Table\""));
    assertFalse(Utils.isValidSnowflakeTableName("\"myMixedCase\""));
    assertFalse(Utils.isValidSnowflakeTableName("\"name-with-dashes\""));

    // --- Failure 2: parseTopicToTableMap throws ERROR_0021 on quoted table names ---
    // Users cannot configure topic2table.map with a quoted destination table name.
    // e.g. snowflake.topic2table.map=myTopic:"My-Table" will fail validation.
    TestUtils.assertError(
        SnowflakeErrors.ERROR_0021,
        () -> Utils.parseTopicToTableMap("myTopic:\"My-Table\""));
    TestUtils.assertError(
        SnowflakeErrors.ERROR_0021,
        () -> Utils.parseTopicToTableMap("myTopic:\"myMixedCase\""));

    // --- Failure 3: topic names with special chars get sanitized, not quoted ---
    // A topic like "my-sensitive-Topic" isn't a valid unquoted identifier (has dashes),
    // so generateValidName sanitizes it: dashes replaced with _, hash appended.
    // It should instead preserve the name by quoting it.
    Map<String, String> emptyMap = new HashMap<>();
    String dashTopic = "my-sensitive-Topic";
    String result = Utils.getTableName(dashTopic, emptyMap, false);
    assertTrue(
        result.startsWith("my_sensitive_Topic_"),
        "Expected sanitized name starting with my_sensitive_Topic_, got: " + result);
    assertFalse(result.contains("-"), "Dashes were stripped from the table name");

    // --- Failure 4: mixed-case names pass through WITHOUT quoting ---
    // "myMixedCase" is a valid unquoted Snowflake identifier, so it passes through as-is.
    // But Snowflake will uppercase unquoted identifiers to MYMIXEDCASE.
    // The connector should wrap it in quotes to preserve case, but it doesn't.
    String mixedCase = "myMixedCase";
    String mixedResult = Utils.getTableName(mixedCase, emptyMap, false);
    assertEquals(mixedCase, mixedResult, "Name passes through unchanged");
    assertFalse(
        mixedResult.startsWith("\""),
        "Name is NOT quoted — Snowflake will uppercase it to MYMIXEDCASE");
  }
}
