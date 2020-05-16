package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.TestUtils;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigValue;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ConnectorTest
{
  final static String allPropertiesList[] =
    {
      SnowflakeSinkConnectorConfig.SNOWFLAKE_URL,
      SnowflakeSinkConnectorConfig.SNOWFLAKE_USER,
      SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA,
      SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE,
      SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS,
      SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES,
      SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC,
      SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_ALL,
      SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_TOPIC,
      SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_OFFSET_AND_PARTITION,
      SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_CREATETIME,
      SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP,
      SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY,
      SnowflakeSinkConnectorConfig.JVM_PROXY_PORT,
      SnowflakeSinkConnectorConfig.JVM_PROXY_HOST,
      SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
    };

  final static Set<String> allProperties = new HashSet<>(Arrays.asList(allPropertiesList));

  private static void assertPropHasError(final Map<String, ConfigValue> validateMap, final String[] propArray)
  {
    List<String> propList = Arrays.asList(propArray);
    for (String prop : allProperties)
    {
      if (propList.contains(prop))
      {
        assert !validateMap.get(prop).errorMessages().isEmpty();
      } else
      {
        assert validateMap.get(prop).errorMessages().isEmpty();
      }
    }
  }

  private static void assertPropNoError(final Map<String, ConfigValue> validateMap, final String[] propArray)
  {
    List<String> propList = Arrays.asList(propArray);
    for (String prop : allProperties)
    {
      if (propList.contains(prop))
      {
        assert validateMap.get(prop).errorMessages().isEmpty();
      } else
      {
        assert !validateMap.get(prop).errorMessages().isEmpty();
      }
    }
  }

  private static Map<String, ConfigValue> toValidateMap(final Map<String, String> config)
  {
    SnowflakeSinkConnector sinkConnector = new SnowflakeSinkConnector();
    Config result = sinkConnector.validate(config);
    return Utils.validateConfigToMap(result);
  }

  static Map<String, String> getErrorConfig()
  {
    Map<String, String> config = new HashMap<>();
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_URL, "");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_USER, "");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY, "");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, "");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA,"");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE, "");
    config.put(SnowflakeSinkConnectorConfig.BUFFER_COUNT_RECORDS, "0");
    config.put(SnowflakeSinkConnectorConfig.BUFFER_SIZE_BYTES, "10000000000");
    config.put(SnowflakeSinkConnectorConfig.BUFFER_FLUSH_TIME_SEC, "2");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_ALL, "falseee");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_TOPIC, "falseee");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_OFFSET_AND_PARTITION, "falseee");
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_METADATA_CREATETIME, "falseee");
    config.put(SnowflakeSinkConnectorConfig.TOPICS_TABLES_MAP, "jfsja,,");
    return config;
  }

  static Map<String, String> getEmptyConfig()
  {
    Map<String, String> config = new HashMap<>();
    return config;
  }

  static Map<String, String> getCorrectConfig()
  {
    Map<String, String> config = TestUtils.getConf();
    config.remove(Utils.SF_WAREHOUSE);
    config.remove(Utils.NAME);
    config.remove(Utils.TASK_ID);
    return config;
  }

  @Test
  public void testValidateErrorConfig()
  {
    Map<String, ConfigValue> validateMap = toValidateMap(getErrorConfig());
    // all single field validation has error
    assertPropNoError(validateMap, new String[]{
      SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY,
      SnowflakeSinkConnectorConfig.JVM_PROXY_PORT,
      SnowflakeSinkConnectorConfig.JVM_PROXY_HOST,
      SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
    });
  }

  @Test
  public void testValidateEmptyConfig()
  {
    Map<String, ConfigValue> validateMap = toValidateMap(getEmptyConfig());
    assertPropHasError(validateMap, new String[]{
      SnowflakeSinkConnectorConfig.SNOWFLAKE_USER,
      SnowflakeSinkConnectorConfig.SNOWFLAKE_URL,
      SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA,
      SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE,
    });
  }

  @Test
  public void testValidateCorrectConfig()
  {
    Map<String, ConfigValue> validateMap = toValidateMap(getCorrectConfig());
    assertPropHasError(validateMap, new String[]{});
  }

  @Test
  public void testValidateErrorURLFormatConfig()
  {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_URL, "https://google.com");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[]{
      SnowflakeSinkConnectorConfig.SNOWFLAKE_URL
    });
  }

  @Test
  public void testValidateErrorURLAccountConfig()
  {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_URL, "wronggAccountt.snowflakecomputing.com:443");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[]{
      SnowflakeSinkConnectorConfig.SNOWFLAKE_USER,
      SnowflakeSinkConnectorConfig.SNOWFLAKE_URL,
      SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY
    });
  }

  @Test
  public void testValidateErrorUserConfig()
  {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_USER, "wrongUser");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[]{
      SnowflakeSinkConnectorConfig.SNOWFLAKE_USER,
      SnowflakeSinkConnectorConfig.SNOWFLAKE_URL,
      SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY
    });
  }

  @Test
  public void testValidateErrorPasswordConfig()
  {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY, "wrongPassword");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[]{
      SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY
    });
  }

  @Test
  public void testValidateEmptyPasswordConfig()
  {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY, "");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[]{
      SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY
    });
  }

  @Test
  public void testValidateNullPasswordConfig()
  {
    Map<String, String> config = getCorrectConfig();
    config.remove(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY);
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[]{
      SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY
    });
  }

  @Test
  public void testValidateErrorPassphraseConfig()
  {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, "wrongPassphrase");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[]{
      SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY,
      SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE
    });
  }

  @Test
  public void testValidateErrorDatabaseConfig()
  {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE, "wrongDatabase");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[]{
      SnowflakeSinkConnectorConfig.SNOWFLAKE_DATABASE
    });
  }

  @Test
  public void testValidateErrorSchemaConfig()
  {
    Map<String, String> config = getCorrectConfig();
    config.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA, "wrongSchema");
    Map<String, ConfigValue> validateMap = toValidateMap(config);
    assertPropHasError(validateMap, new String[]{
      SnowflakeSinkConnectorConfig.SNOWFLAKE_SCHEMA
    });
  }
}
