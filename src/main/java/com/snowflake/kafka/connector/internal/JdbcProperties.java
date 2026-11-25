package com.snowflake.kafka.connector.internal;

import java.util.HashSet;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

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

  public Properties getProxyProperties() {
    return proxyProperties;
  }

  /**
   * Combine all jdbc related properties. Throws error if jdbcPropertiesMap overrides any property
   * defined in connectionProperties or proxyProperties.
   *
   * @param connectionProperties snowflake.database.name, snowflake.schema,name,
   *     snowflake.private.key etc.
   * @param proxyProperties the snowflake.https.proxy.* settings
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

  /**
   * Keys are compared case-insensitively and ignoring surrounding whitespace: the driver resolves
   * its own properties case-insensitively and forwards unknown keys as session parameters, so a
   * case variant of a connector-owned key would still reach the session.
   */
  private static void detectOverrides(Properties proxyAndConnection, Properties jdbcPropertiesMap) {
    Set<String> reservedKeys = new HashSet<>();
    proxyAndConnection.keySet().forEach(k -> reservedKeys.add(normalizeKey(k)));
    Set<String> seenKeys = new HashSet<>();
    jdbcPropertiesMap.forEach(
        (k, v) -> {
          String key = normalizeKey(k);
          if (reservedKeys.contains(key)) {
            throw SnowflakeErrors.ERROR_0031.getException(
                "Duplicated property: " + k + " is set by the connector");
          }
          if (!seenKeys.add(key)) {
            throw SnowflakeErrors.ERROR_0031.getException(
                "Duplicated property: " + k + " is set more than once in snowflake.jdbc.map");
          }
        });
  }

  private static String normalizeKey(Object key) {
    return String.valueOf(key).trim().toUpperCase(Locale.ROOT);
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
