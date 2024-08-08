package com.snowflake.kafka.connector.internal;

import java.util.Properties;

// docs
public class JdbcProperties {

  private static final KCLogger LOGGER = new KCLogger(JdbcProperties.class.getName());

  private final Properties properties;

  // Placeholder for all proxy related properties set in the connector configuration
  private final Properties proxyProperties;

  private JdbcProperties(Properties combinedProperties, Properties proxyProperties) {
    this.properties = combinedProperties;
    this.proxyProperties = proxyProperties;
  }

  public Properties getProperties() {
    Properties copy = new Properties();
    copy.putAll(this.properties);
    return copy;
  }

  public String getProperty(String key) {
    return properties.getProperty(key);
  }

  public Object get(String key) {
    return properties.get(key);
  }

  public Properties getProxyProperties() {
    Properties copy = new Properties();
    copy.putAll(this.proxyProperties);
    return copy;
  }

  /**
   * @param connectionProperties
   * @param proxyProperties
   * @param jdbcPropertiesMap
   * @return
   */
  static JdbcProperties create(
      Properties connectionProperties, Properties proxyProperties, Properties jdbcPropertiesMap) {
    InternalUtils.assertNotEmpty("connectionProperties", connectionProperties);
    validateNull(proxyProperties);
    validateNull(jdbcPropertiesMap);

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
  private static Properties validateNull(Properties properties) {
    if (properties != null) {
      return properties;
    }
    return new Properties();
  }
}
