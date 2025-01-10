package com.snowflake.kafka.connector.internal;

import static org.apache.commons.lang3.StringUtils.EMPTY;

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

public class OAuthURL implements URL {
  private static final KCLogger LOGGER = new KCLogger(OAuthURL.class.getName());

  private static final String OAUTH_SERVER_URL_REGEX_PATTERN =
      "^(https?://)?((([\\w\\d-]+)(\\.[\\w\\d-]+)*)(:(\\d+))?)((/[\\w\\d]*)*)/?$";

  private final String url;

  private final boolean ssl;

  private final int port;

  private final String path;

  private OAuthURL(String url, boolean ssl, int port, String path) {
    this.url = url;
    this.ssl = ssl;
    this.port = port;
    this.path = path;
  }

  public static OAuthURL from(final String urlStr) {
    Pattern pattern = Pattern.compile(OAUTH_SERVER_URL_REGEX_PATTERN);

    Matcher matcher = pattern.matcher(urlStr.trim().toLowerCase());

    if (!matcher.find()) {
      throw SnowflakeErrors.ERROR_0007.getException("input url: " + urlStr);
    }

    boolean ssl = !"http://".equals(matcher.group(1));

    String url = matcher.group(3);

    int port = 0;
    if (matcher.group(7) != null) {
      port = Integer.parseInt(matcher.group(7));
    } else if (ssl) {
      port = 443;
    } else {
      port = 80;
    }

    String path = StringUtils.defaultIfBlank(matcher.group(8), EMPTY);

    LOGGER.debug("parsed Snowflake URL: {}", urlStr);

    return new OAuthURL(url, ssl, port, path);
  }

  @Override
  public String hostWithPort() {
    return String.format("%s:%s", this.url, this.port);
  }

  @Override
  public String getScheme() {
    if (ssl) {
      return "https";
    } else {
      return "http";
    }
  }

  @Override
  public boolean sslEnabled() {
    return ssl;
  }

  @Override
  public String path() {
    return path;
  }
}
