package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeURL;
import com.snowflake.kafka.connector.internal.TestUtils;
import java.util.HashMap;
import java.util.Map;
import org.junit.Assert;
import org.junit.Ignore;
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
    config.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP, "aaa:bbb," + "ccc:ddd");
    result = SnowflakeSinkTask.getTopicToTableMap(config);
    assert result.size() == 2;
    assert result.containsKey("aaa");
    assert result.get("aaa").equals("bbb");
    assert result.containsKey("ccc");
    assert result.get("ccc").equals("ddd");

    // has map, but invalid data
    config.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP, "12321");
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
    assert topic2table.keySet().size() == 2;

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
  public void testTableName() {
    Map<String, String> topic2table = Utils.parseTopicToTableMap("ab@cd:abcd, 1234:_1234");

    assert Utils.tableName("ab@cd", topic2table).equals("abcd");
    assert Utils.tableName("1234", topic2table).equals("_1234");

    TestUtils.assertError(SnowflakeErrors.ERROR_0020, () -> Utils.tableName("", topic2table));
    TestUtils.assertError(SnowflakeErrors.ERROR_0020, () -> Utils.tableName(null, topic2table));

    String topic = "bc*def";
    assert Utils.tableName(topic, topic2table).equals("bc_def_" + Math.abs(topic.hashCode()));

    topic = "12345";
    assert Utils.tableName(topic, topic2table).equals("_12345_" + Math.abs(topic.hashCode()));
  }

  @Test
  public void testGenerateTableName() {
    Map<String, String> topic2table = Utils.parseTopicToTableMap("ab@cd:abcd, 1234:_1234");

    String topic0 = "ab@cd";
    Utils.GeneratedName generatedTableName1 = Utils.generateTableName(topic0, topic2table);
    Assert.assertEquals("abcd", generatedTableName1.getName());
    Assert.assertTrue(generatedTableName1.isNameFromMap());

    String topic1 = "1234";
    Utils.GeneratedName generatedTableName2 = Utils.generateTableName(topic1, topic2table);
    Assert.assertEquals("_1234", generatedTableName2.getName());
    Assert.assertTrue(generatedTableName2.isNameFromMap());

    String topic2 = "bc*def";
    Utils.GeneratedName generatedTableName3 = Utils.generateTableName(topic2, topic2table);
    Assert.assertEquals("bc_def_" + Math.abs(topic2.hashCode()), generatedTableName3.getName());
    Assert.assertFalse(generatedTableName3.isNameFromMap());

    String topic3 = "12345";
    Utils.GeneratedName generatedTableName4 = Utils.generateTableName(topic3, topic2table);
    Assert.assertEquals("_12345_" + Math.abs(topic3.hashCode()), generatedTableName4.getName());
    Assert.assertFalse(generatedTableName4.isNameFromMap());

    TestUtils.assertError(
        SnowflakeErrors.ERROR_0020, () -> Utils.generateTableName("", topic2table));
    //noinspection DataFlowIssue
    TestUtils.assertError(
        SnowflakeErrors.ERROR_0020, () -> Utils.generateTableName(null, topic2table));
  }

  @Test
  public void testTableNameRegex() {
    String catTable = "cat_table";
    String dogTable = "dog_table";
    String catTopicRegex = ".*_cat";
    String dogTopicRegex = ".*_dog";

    // test two different regexs
    Map<String, String> topic2table =
        Utils.parseTopicToTableMap(
            Utils.formatString("{}:{},{}:{}", catTopicRegex, catTable, dogTopicRegex, dogTable));

    assert Utils.tableName("calico_cat", topic2table).equals(catTable);
    assert Utils.tableName("orange_cat", topic2table).equals(catTable);
    assert Utils.tableName("_cat", topic2table).equals(catTable);
    assert Utils.tableName("corgi_dog", topic2table).equals(dogTable);

    // test new topic should not have wildcard
    String topic = "bird.*";
    assert Utils.tableName(topic, topic2table).equals("bird_" + Math.abs(topic.hashCode()));
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

    config.put(SnowflakeSinkConnectorConfig.NAME, "_aA1");
    Utils.convertAppName(config);
    assert config.get(SnowflakeSinkConnectorConfig.NAME).equals("_aA1");

    config.put(SnowflakeSinkConnectorConfig.NAME, "-_aA1");
    Utils.convertAppName(config);
    assert config.get(SnowflakeSinkConnectorConfig.NAME).equals("___aA1_44483871");

    config.put(SnowflakeSinkConnectorConfig.NAME, "_aA1-");
    Utils.convertAppName(config);
    assert config.get(SnowflakeSinkConnectorConfig.NAME).equals("_aA1__90688251");

    config.put(SnowflakeSinkConnectorConfig.NAME, "testApp.snowflake-connector");
    Utils.convertAppName(config);
    assert config
        .get(SnowflakeSinkConnectorConfig.NAME)
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
  public void testSetJDBCLoggingDir() {
    String defaultTmpDir = System.getProperty(Utils.JAVA_IO_TMPDIR);

    Utils.setJDBCLoggingDirectory();
    assert !System.getProperty(Utils.JAVA_IO_TMPDIR).isEmpty();

    environmentVariables.set(
        SnowflakeSinkConnectorConfig.SNOWFLAKE_JDBC_LOG_DIR, "/dummy_dir_not_exist");
    Utils.setJDBCLoggingDirectory();
    assert !System.getProperty(Utils.JAVA_IO_TMPDIR).equals("/dummy_dir_not_exist");

    environmentVariables.set(SnowflakeSinkConnectorConfig.SNOWFLAKE_JDBC_LOG_DIR, "/usr");
    Utils.setJDBCLoggingDirectory();
    assert System.getProperty(Utils.JAVA_IO_TMPDIR).equals("/usr");

    environmentVariables.set(SnowflakeSinkConnectorConfig.SNOWFLAKE_JDBC_LOG_DIR, defaultTmpDir);
    Utils.setJDBCLoggingDirectory();
    assert System.getProperty(Utils.JAVA_IO_TMPDIR).equals(defaultTmpDir);
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

    Exception stacktraceEx = new Exception(exceptionMessage);
    stacktraceEx.initCause(cause);
    stacktraceEx.getCause().setStackTrace(stackTrace);
    assert Utils.getExceptionMessage(customMessage, stacktraceEx)
        .equals(
            Utils.formatString(Utils.GET_EXCEPTION_FORMAT, customMessage, exceptionMessage, "[]"));
  }

  @Test
  @Ignore("OAuth tests are temporary disabled")
  public void testGetSnowflakeOAuthAccessToken() {
    Map<String, String> config = TestUtils.getConfForStreamingWithOAuth();
    if (config != null) {
      SnowflakeURL url = new SnowflakeURL(config.get(Utils.SF_URL));
      Utils.getSnowflakeOAuthAccessToken(
          url,
          config.get(Utils.SF_OAUTH_CLIENT_ID),
          config.get(Utils.SF_OAUTH_CLIENT_SECRET),
          config.get(Utils.SF_OAUTH_REFRESH_TOKEN));
      TestUtils.assertError(
          SnowflakeErrors.ERROR_1004,
          () -> Utils.getSnowflakeOAuthAccessToken(url, "INVALID", "INVALID", "INVALID"));
    }
  }

  @Test
  public void testQuoteNameIfNeeded() {
    Assert.assertEquals("\"ABC\"", Utils.quoteNameIfNeeded("abc"));
    Assert.assertEquals("\"abc\"", Utils.quoteNameIfNeeded("\"abc\""));
    Assert.assertEquals("\"ABC\"", Utils.quoteNameIfNeeded("ABC"));
    Assert.assertEquals("\"A\"", Utils.quoteNameIfNeeded("a"));
  }
}
