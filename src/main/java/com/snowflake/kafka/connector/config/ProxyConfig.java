package com.snowflake.kafka.connector.config;

import com.google.auto.value.AutoValue;
import javax.annotation.Nullable;

/**
 * The connector's {@code jvm.proxy.*} settings. Any field may be null when unset. The bypass list
 * uses the JDK {@code http.nonProxyHosts} format.
 */
@AutoValue
public abstract class ProxyConfig {

  @Nullable
  public abstract String getHost();

  @Nullable
  public abstract String getPort();

  @Nullable
  public abstract String getNonProxyHosts();

  @Nullable
  public abstract String getUsername();

  @Nullable
  public abstract String getPassword();

  public static ProxyConfig create(
      @Nullable String host,
      @Nullable String port,
      @Nullable String nonProxyHosts,
      @Nullable String username,
      @Nullable String password) {
    return new AutoValue_ProxyConfig(host, port, nonProxyHosts, username, password);
  }

  /** No proxy configured, so clients follow the JVM's defaults. */
  public static ProxyConfig none() {
    return create(null, null, null, null, null);
  }

  /** True when both host and port are set. */
  public boolean isConfigured() {
    return !isBlank(getHost()) && !isBlank(getPort());
  }

  /** True when both user name and password are set. */
  public boolean hasCredentials() {
    return !isBlank(getUsername()) && !isBlank(getPassword());
  }

  @Override
  public final String toString() {
    return "ProxyConfig{host="
        + getHost()
        + ", port="
        + getPort()
        + ", nonProxyHosts="
        + getNonProxyHosts()
        + ", username="
        + getUsername()
        + ", password="
        + (getPassword() == null ? null : "****")
        + "}";
  }

  private static boolean isBlank(String value) {
    return value == null || value.trim().isEmpty();
  }
}
