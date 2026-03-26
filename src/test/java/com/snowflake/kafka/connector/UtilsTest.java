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
  public void testGetTableName() {
    Map<String, String> topic2table = TopicToTableParser.parse("ab@cd:abcd, 1234:_1234");

    assert Utils.getTableName("ab@cd", topic2table, true).equals("ABCD");
    assert Utils.getTableName("1234", topic2table, true).equals("_1234");

    TestUtils.assertError(
        SnowflakeErrors.ERROR_0020, () -> Utils.getTableName("", topic2table, true));
    TestUtils.assertError(
        SnowflakeErrors.ERROR_0020, () -> Utils.getTableName(null, topic2table, true));

    String topic = "bc*def";
    assert Utils.getTableName(topic, topic2table, true)
        .equals("BC_DEF_" + Math.abs(topic.hashCode()));

    topic = "12345";
    assert Utils.getTableName(topic, topic2table, true)
        .equals("_12345_" + Math.abs(topic.hashCode()));
  }

  @Test
  public void testGetTableNameRegex() {
    String catTable = "cat_table";
    String dogTable = "dog_table";
    String catTopicRegex = ".*_cat";
    String dogTopicRegex = ".*_dog";

    // test two different regexs
    Map<String, String> topic2table =
        TopicToTableParser.parse(
            Utils.formatString("{}:{},{}:{}", catTopicRegex, catTable, dogTopicRegex, dogTable));

    assert Utils.getTableName("calico_cat", topic2table, true).equals("CAT_TABLE");
    assert Utils.getTableName("orange_cat", topic2table, true).equals("CAT_TABLE");
    assert Utils.getTableName("_cat", topic2table, true).equals("CAT_TABLE");
    assert Utils.getTableName("corgi_dog", topic2table, true).equals("DOG_TABLE");

    // test new topic should not have wildcard
    String topic = "bird.*";
    assert Utils.getTableName(topic, topic2table, true)
        .equals("BIRD_" + Math.abs(topic.hashCode()));
  }

  @Test
  public void testConvertAppName() {
    HashMap<String, String> config = new HashMap<String, String>();

    config.put(KafkaConnectorConfigParams.NAME, "_aA1");
    Utils.convertAppName(config);
    assert config.get(KafkaConnectorConfigParams.NAME).equals("_AA1");

    config.put(KafkaConnectorConfigParams.NAME, "-_aA1");
    Utils.convertAppName(config);
    assert config.get(KafkaConnectorConfigParams.NAME).equals("___AA1_44483871");

    config.put(KafkaConnectorConfigParams.NAME, "_aA1-");
    Utils.convertAppName(config);
    assert config.get(KafkaConnectorConfigParams.NAME).equals("_AA1__90688251");

    config.put(KafkaConnectorConfigParams.NAME, "testApp.snowflake-connector");
    Utils.convertAppName(config);
    assert config
        .get(KafkaConnectorConfigParams.NAME)
        .equals("TESTAPP_SNOWFLAKE_CONNECTOR_36242259");
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

  @Test
  public void testSanitizationToggle() {
    Map<String, String> emptyMap = new HashMap<>();

    // Sanitization enabled (v3 compatible)
    String uppercased = Utils.getTableName("MyTopic", emptyMap, true);
    assertEquals("MYTOPIC", uppercased, "Valid identifier should be uppercased");

    String sanitized = Utils.getTableName("my-topic", emptyMap, true);
    assertTrue(
        sanitized.startsWith("MY_TOPIC_"), "Invalid identifier should be sanitized+uppercased");
    assertTrue(sanitized.matches("^[A-Z_0-9]+$"), "Should be fully uppercased");

    // Sanitization disabled (pass through)
    String passedThrough = Utils.getTableName("MyTopic", emptyMap, false);
    assertEquals("MyTopic", passedThrough, "Should pass through unchanged");

    String invalid = Utils.getTableName("my-topic", emptyMap, false);
    assertEquals("my-topic", invalid, "Invalid identifier should pass through");
  }

  @Test
  public void testMapEntriesBypassSanitization() {
    Map<String, String> map = TopicToTableParser.parse("myTopic:\"My-Table\",otherTopic:MixedCase");

    // Quoted table names preserve case; unquoted are uppercased at parse time
    assertEquals("My-Table", Utils.getTableName("myTopic", map, true));
    assertEquals("My-Table", Utils.getTableName("myTopic", map, false));
    assertEquals("MIXEDCASE", Utils.getTableName("otherTopic", map, true));
    assertEquals("MIXEDCASE", Utils.getTableName("otherTopic", map, false));
  }
}
