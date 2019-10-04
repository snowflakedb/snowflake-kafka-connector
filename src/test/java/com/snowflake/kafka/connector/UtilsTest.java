package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.TestUtils;
import org.junit.Test;

import java.util.Map;

import org.apache.kafka.common.config.ConfigException;

public class UtilsTest
{
  @Test
  public void testObjectIdentifier()
  {
    String name = "DATABASE.SCHEMA.TABLE";
    assert !Utils.isValidSnowflakeObjectIdentifier(name);
    String name1 = "table!@#$%^;()";
    assert !Utils.isValidSnowflakeObjectIdentifier(name1);
  }

  @Test
  public void testVersionChecker()
  {
    Utils.checkConnectorVersion();
  }

  @Test
  public void testParseTopicToTable()
  {
    String input = "adsadas";
    try
    {
      Utils.parseTopicToTableMap(input);
      assert false;
    }
    catch (ConfigException e) { }

    input = "abc:@123,bvd:adsa";
    try
    {
      Utils.parseTopicToTableMap(input);
      assert false;
    }
    catch (ConfigException e) { }
  }

  @Test
  public void testTableName()
  {
    Map<String, String> topic2table =
      Utils.parseTopicToTableMap("ab@cd:abcd, 1234:_1234");

    assert SnowflakeSinkTask.tableName("ab@cd", topic2table).equals("abcd");
    assert SnowflakeSinkTask.tableName("1234", topic2table).equals("_1234");

    TestUtils.assertError(SnowflakeErrors.ERROR_0020, ()-> SnowflakeSinkTask.tableName("", topic2table));
    TestUtils.assertError(SnowflakeErrors.ERROR_0020, ()-> SnowflakeSinkTask.tableName(null, topic2table));

    String topic = "bc*def";
    assert SnowflakeSinkTask.tableName(topic, topic2table).equals("bc_def_"+ Math.abs(topic.hashCode()));

    topic = "12345";
    assert SnowflakeSinkTask.tableName(topic, topic2table).equals("_12345_"+ Math.abs(topic.hashCode()));
  }

  @Test
  public void testTableFullName()
  {
    assert Utils.isValidSnowflakeTableName("_1342dfsaf$");
    assert Utils.isValidSnowflakeTableName("dad._1342dfsaf$");
    assert Utils.isValidSnowflakeTableName("adsa123._gdgsdf._1342dfsaf$");
    assert !Utils.isValidSnowflakeTableName("_13)42dfsaf$");
    assert !Utils.isValidSnowflakeTableName("_13.42dfsaf$");
    assert !Utils.isValidSnowflakeTableName("_1342.df.sa.f$");


  }

}
