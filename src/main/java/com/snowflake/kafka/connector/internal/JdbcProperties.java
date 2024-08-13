package com.snowflake.kafka.connector.internal;

import java.util.Properties;

/** Wrapper class for all snowflake jdbc properties */
public class JdbcProperties {

  /** All jdbc properties including proxyProperties */
  private final Properties properties;
  /** Proxy related properties */
  private final Properties proxyProperties;

  private JdbcProperties(Properties combinedProperties, Properties proxyProperties) {
    this.properties = combinedProperties;
    this.proxyProperties = proxyProperties;
  }

  public Properties getProperties() {
    return properties;
  }

  public String getProperty(String key) {
    return properties.getProperty(key);
  }

  public Object get(String key) {
    return properties.get(key);
  }

  public Properties getProxyProperties() {
    return proxyProperties;
  }

  /**
   * Combine all jdbc related properties. Throws error if jdbcPropertiesMap overrides any property
   * defined in connectionProperties or proxyProperties.
   *
   * @param connectionProperties snowflake.database.name, snowflake.schema,name,
   *     snowflake.private.key etc.
   * @param proxyProperties jvm.proxy.xxx
   * @param jdbcPropertiesMap snowflake.jdbc.map
   */
  static JdbcProperties create(
      Properties connectionProperties, Properties proxyProperties, Properties jdbcPropertiesMap) {
    InternalUtils.assertNotEmpty("connectionProperties", connectionProperties);
    proxyProperties = setEmptyIfNull(proxyProperties);
    jdbcPropertiesMap = setEmptyIfNull(jdbcPropertiesMap);

    Properties proxyAndConnection = mergeProperties(connectionProperties, proxyProperties);
    detectOverrides(proxyAndConnection, jdbcPropertiesMap);

    Properties combinedProperties = mergeProperties(proxyAndConnection, jdbcPropertiesMap);

    return new JdbcProperties(combinedProperties, proxyProperties);
  }

  /** Test method */
  static JdbcProperties create(Properties connectionProperties) {
    return create(connectionProperties, new Properties(), new Properties());
  }

  private static void detectOverrides(Properties proxyAndConnection, Properties jdbcPropertiesMap) {
    jdbcPropertiesMap.forEach(
        (k, v) -> {
          if (proxyAndConnection.containsKey(k)) {
            throw SnowflakeErrors.ERROR_0031.getException("Duplicated property: " + k);
          }
        });
  }

  private static Properties mergeProperties(
      Properties connectionProperties, Properties proxyProperties) {
    Properties mergedProperties = new Properties();
    mergedProperties.putAll(connectionProperties);
    mergedProperties.putAll(proxyProperties);
    return mergedProperties;
  }

  /** Parsing methods does not return null. However, It's better to be perfectly sure. */
  private static Properties setEmptyIfNull(Properties properties) {
    if (properties != null) {
      return properties;
    }
    return new Properties();
  }
}
