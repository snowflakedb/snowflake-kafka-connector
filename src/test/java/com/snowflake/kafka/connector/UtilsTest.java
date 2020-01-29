package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.TestUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class UtilsTest
{
  @Test
  public void testGetTopicToTableMap()
  {
    //no map
    Map<String, String> config = new HashMap<>();
    Map<String, String> result = SnowflakeSinkTask.getTopicToTableMap(config);
    assert result.isEmpty();

    //has map
    config.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP, "aaa:bbb,ccc:ddd");
    result = SnowflakeSinkTask.getTopicToTableMap(config);
    assert result.size() == 2;
    assert result.containsKey("aaa");
    assert result.get("aaa").equals("bbb");
    assert result.containsKey("ccc");
    assert result.get("ccc").equals("ddd");

    //has map, but invalid data
    config.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP, "12321");
    result = SnowflakeSinkTask.getTopicToTableMap(config);
    assert result.isEmpty();
  }


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
    assert Utils.checkConnectorVersion();
  }

  @Test
  public void testParseTopicToTable()
  {
    String input = "adsadas";
    assert Utils.parseTopicToTableMap(input) == null;

    input = "abc:@123,bvd:adsa";
    assert Utils.parseTopicToTableMap(input) == null;
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
