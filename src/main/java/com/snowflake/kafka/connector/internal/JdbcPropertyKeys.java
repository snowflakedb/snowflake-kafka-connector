package com.snowflake.kafka.connector.internal;

/**
 * Snowflake JDBC connection property key names. These match the official JDBC driver connection
 * parameters (see Snowflake JDBC documentation). Used instead of internal SFSessionProperty to
 * remain compatible with JDBC 4.x public API.
 */
public final class JdbcPropertyKeys {

  private JdbcPropertyKeys() {}

  public static final String AUTHENTICATOR = "authenticator";
  public static final String TOKEN = "token";
  public static final String ROLE = "role";
  public static final String ALLOW_UNDERSCORES_IN_HOST = "allowUnderscoresInHost";
  public static final String USE_PROXY = "useProxy";
  public static final String PROXY_HOST = "proxyHost";
  public static final String PROXY_PORT = "proxyPort";
  public static final String PROXY_USER = "proxyUser";
  public static final String PROXY_PASSWORD = "proxyPassword";
  public static final String NON_PROXY_HOSTS = "nonProxyHosts";
}
