package com.snowflake.kafka.connector.config;

import com.google.auto.value.AutoValue;
import com.google.common.collect.Range;
import com.google.common.net.HostAndPort;
import com.google.common.net.InetAddresses;
import com.google.common.net.InternetDomainName;
import com.google.common.primitives.Ints;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * The connector's {@code jvm.proxy.*} settings. Any field may be null when unset. The bypass list
 * uses the JDK {@code http.nonProxyHosts} format.
 *
 * <p>{@link #create} rejects an invalid combination instead of ignoring it, so a proxy the operator
 * asked for is never dropped silently. {@link #validate} applies the same rules and reports every
 * problem against its config key, for connector validation.
 */
@AutoValue
public abstract class ProxyConfig {

  private static final Range<Integer> VALID_PORTS = Range.closed(1, 65535);

  static final String HOST_AND_PORT_TOGETHER = "proxy host and port must be provided together";
  static final String USERNAME_AND_PASSWORD_TOGETHER =
      "proxy username and password must be provided together";

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

  /**
   * Validates and normalizes the settings. Host, port and bypass list are trimmed, and a blank
   * value counts as unset.
   *
   * @throws IllegalArgumentException if the settings are invalid; the message names every offending
   *     config key
   */
  public static ProxyConfig create(
      @Nullable String host,
      @Nullable String port,
      @Nullable String nonProxyHosts,
      @Nullable String username,
      @Nullable String password) {
    Map<String, String> errors = validate(host, port, nonProxyHosts, username, password);
    if (!errors.isEmpty()) {
      throw new IllegalArgumentException(
          "Invalid proxy settings: "
              + errors.entrySet().stream()
                  .map(e -> e.getKey() + ": " + e.getValue())
                  .collect(Collectors.joining("; ")));
    }
    return new AutoValue_ProxyConfig(
        trimToNull(host),
        trimToNull(port),
        trimToNull(nonProxyHosts),
        blankToNull(username),
        blankToNull(password));
  }

  /** No proxy configured, so clients follow the JVM's defaults. */
  public static ProxyConfig none() {
    return new AutoValue_ProxyConfig(null, null, null, null, null);
  }

  /**
   * Checks the settings and returns the problems keyed by config key. Empty when the settings are
   * valid, including when nothing is set.
   */
  public static Map<String, String> validate(
      @Nullable String host,
      @Nullable String port,
      @Nullable String nonProxyHosts,
      @Nullable String username,
      @Nullable String password) {
    Map<String, String> errors = new LinkedHashMap<>();
    String trimmedHost = trimToNull(host);
    String trimmedPort = trimToNull(port);

    if ((trimmedHost == null) != (trimmedPort == null)) {
      errors.put(KafkaConnectorConfigParams.JVM_PROXY_HOST, HOST_AND_PORT_TOGETHER);
      errors.put(KafkaConnectorConfigParams.JVM_PROXY_PORT, HOST_AND_PORT_TOGETHER);
    } else if (trimmedHost != null) {
      try {
        parsePort(trimmedPort);
      } catch (IllegalArgumentException e) {
        errors.put(KafkaConnectorConfigParams.JVM_PROXY_PORT, e.getMessage());
      }
      try {
        checkHost(trimmedHost);
      } catch (IllegalArgumentException e) {
        errors.put(KafkaConnectorConfigParams.JVM_PROXY_HOST, e.getMessage());
      }
    }

    if (blankToNull(username) == null != (blankToNull(password) == null)) {
      errors.put(KafkaConnectorConfigParams.JVM_PROXY_USERNAME, USERNAME_AND_PASSWORD_TOGETHER);
      errors.put(KafkaConnectorConfigParams.JVM_PROXY_PASSWORD, USERNAME_AND_PASSWORD_TOGETHER);
    }

    String invalidEntry = invalidNonProxyHostsEntry(nonProxyHosts);
    if (invalidEntry != null) {
      errors.put(
          KafkaConnectorConfigParams.JVM_NON_PROXY_HOSTS,
          invalidNonProxyHostsMessage(invalidEntry));
    }
    return errors;
  }

  /** True when both host and port are set. */
  public boolean isConfigured() {
    return getHost() != null && getPort() != null;
  }

  /** True when both user name and password are set. */
  public boolean hasCredentials() {
    return getUsername() != null && getPassword() != null;
  }

  /**
   * {@code http://host:port}, or null when host or port is blank. The host must be an IP address or
   * a host name (IPv6 may be bracketed); Guava's {@link HostAndPort} formats the authority.
   *
   * @throws IllegalArgumentException if the port is not an integer from 1 to 65535, or the host is
   *     not valid
   */
  @Nullable
  public static String toUrl(@Nullable String host, @Nullable String port) {
    String trimmedHost = trimToNull(host);
    String trimmedPort = trimToNull(port);
    if (trimmedHost == null || trimmedPort == null) {
      return null;
    }
    return buildUrl(trimmedHost, parsePort(trimmedPort));
  }

  /**
   * Returns the first entry of a {@code |}-separated bypass list that the SDK would reject, or null
   * when the list is valid or blank. An entry may contain only letters, digits and {@code . - _ : *
   * [ ]}, which matches what the streaming SDK accepts in {@code no_proxy}; for example a CIDR
   * block such as {@code 10.0.0.0/8} is not valid. Empty entries are ignored.
   */
  @Nullable
  public static String invalidNonProxyHostsEntry(@Nullable String nonProxyHosts) {
    if (nonProxyHosts == null) {
      return null;
    }
    for (String entry : nonProxyHosts.split("\\|", -1)) {
      String trimmed = entry.trim();
      if (!trimmed.isEmpty() && !trimmed.chars().allMatch(ProxyConfig::isNonProxyHostsChar)) {
        return trimmed;
      }
    }
    return null;
  }

  /** The message used when {@link #invalidNonProxyHostsEntry} finds a bad entry. */
  public static String invalidNonProxyHostsMessage(String entry) {
    return "invalid entry '"
        + entry
        + "': use the JDK http.nonProxyHosts format, entries separated by '|' that are host"
        + " names or IP addresses, where '*' matches any characters";
  }

  private static boolean isNonProxyHostsChar(int c) {
    return (c >= 'a' && c <= 'z')
        || (c >= 'A' && c <= 'Z')
        || (c >= '0' && c <= '9')
        || c == '.'
        || c == '-'
        || c == '_'
        || c == ':'
        || c == '*'
        || c == '['
        || c == ']';
  }

  private static int parsePort(String port) {
    Integer portNumber = Ints.tryParse(port);
    if (portNumber == null || !VALID_PORTS.contains(portNumber)) {
      throw new IllegalArgumentException(
          "proxy port must be an integer between 1 and 65535, got '" + port + "'");
    }
    return portNumber;
  }

  /** The host to use in a URL (IPv6 without brackets); throws if it is not an IP or host name. */
  private static String checkHost(String host) {
    boolean bracketedIpv6 =
        host.length() > 2 && host.startsWith("[") && host.endsWith("]") && host.indexOf(':') > 0;
    String bare = bracketedIpv6 ? host.substring(1, host.length() - 1) : host;
    if (!InetAddresses.isInetAddress(bare) && !InternetDomainName.isValid(bare)) {
      throw new IllegalArgumentException("proxy host is not valid: '" + host + "'");
    }
    return bare;
  }

  private static String buildUrl(String host, int port) {
    return "http://" + HostAndPort.fromParts(checkHost(host), port);
  }

  @Nullable
  private static String trimToNull(@Nullable String value) {
    if (value == null) {
      return null;
    }
    String trimmed = value.trim();
    return trimmed.isEmpty() ? null : trimmed;
  }

  /**
   * Null when blank, otherwise the value unchanged (used for credentials, which are not trimmed).
   */
  @Nullable
  private static String blankToNull(@Nullable String value) {
    return value == null || value.trim().isEmpty() ? null : value;
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
}
