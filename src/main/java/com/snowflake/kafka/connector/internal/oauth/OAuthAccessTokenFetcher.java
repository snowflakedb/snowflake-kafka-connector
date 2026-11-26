package com.snowflake.kafka.connector.internal.oauth;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.URL;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import net.snowflake.client.api.exception.SnowflakeSQLException;
import net.snowflake.client.internal.core.HttpClientSettingsKey;
import net.snowflake.client.internal.core.HttpUtil;
import net.snowflake.client.internal.core.SFException;
import net.snowflake.client.internal.core.SFSession;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.CloseableHttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPost;
import net.snowflake.client.jdbc.internal.apache.http.entity.ContentType;
import net.snowflake.client.jdbc.internal.apache.http.entity.StringEntity;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.apache.http.util.EntityUtils;
import org.apache.kafka.common.config.types.Password;

/**
 * Fetches an OAuth access token from an OAuth token endpoint. Used only for JDBC connections; the
 * streaming SDK handles its own OAuth token lifecycle.
 *
 * <p>Supports two grant types:
 *
 * <ul>
 *   <li><b>refresh_token</b>: when a refresh token is provided
 *   <li><b>client_credentials</b>: when no refresh token is provided
 * </ul>
 *
 * @see <a
 *     href="https://github.com/snowflakedb/snowflake/blob/4fdb96cd5849f266cda430c5d49a13c29e866af5/GlobalServices/src/main/java/com/snowflake/resources/ResourceConstants.java">ResourceConstants</a>
 */
public class OAuthAccessTokenFetcher {

  private static final KCLogger LOGGER = new KCLogger(OAuthAccessTokenFetcher.class.getName());
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String TOKEN_REQUEST_ENDPOINT = "/oauth/token-request";
  private static final String OAUTH_CONTENT_TYPE_HEADER = "application/x-www-form-urlencoded";
  private static final String BASIC_AUTH_HEADER_PREFIX = "Basic ";
  private static final String GRANT_TYPE_PARAM = "grant_type";
  private static final String GRANT_TYPE_REFRESH_TOKEN = "refresh_token";
  private static final String GRANT_TYPE_CLIENT_CREDENTIALS = "client_credentials";
  private static final String REFRESH_TOKEN = "refresh_token";
  private static final String ACCESS_TOKEN = "access_token";
  private static final String SCOPE = "scope";
  private static final String REDIRECT_URI = "redirect_uri";
  private static final String DEFAULT_REDIRECT_URI = "https://localhost.com/oauth";

  private static final int HTTP_TOO_MANY_REQUESTS = 429;

  private static final int MAX_RETRIES = 4;
  private static final Duration INITIAL_DELAY = Duration.ofSeconds(1);
  private static final Duration MAX_DELAY = Duration.ofSeconds(8);

  /**
   * Fetch an OAuth access token. Retries with exponential backoff on transient failures. The
   * request is sent with the HTTP client the Snowflake JDBC driver builds for {@code
   * jdbcProperties}, so it reaches the token endpoint exactly the way the JDBC connection reaches
   * Snowflake: the same proxy selection, proxy credentials, bypass list, TLS settings and
   * certificate revocation checks. The client is shared with the driver and stays open.
   *
   * @param url OAuth server URL (scheme, host, port, path)
   * @param clientId OAuth client ID
   * @param clientSecret OAuth client secret
   * @param refreshToken OAuth refresh token; empty selects client_credentials grant
   * @param scope explicit OAuth scope to request; empty sends no scope (KC v3 behavior)
   * @param jdbcProperties the properties that select the driver's HTTP client settings, i.e. the
   *     proxy and {@code snowflake.jdbc.map} properties passed to the JDBC connection
   * @return the access token string
   * @throws SnowflakeKafkaConnectorException if the driver rejects {@code jdbcProperties} or the
   *     token fetch fails after all retries
   */
  public static String fetchAccessToken(
      URL url,
      String clientId,
      Password clientSecret,
      Optional<Password> refreshToken,
      Optional<String> scope,
      Properties jdbcProperties) {
    CloseableHttpClient httpClient = HttpUtil.getHttpClient(jdbcHttpClientSettings(jdbcProperties));
    return fetchAccessToken(url, clientId, clientSecret, refreshToken, scope, httpClient);
  }

  /**
   * The driver's HTTP client settings for {@code jdbcProperties}, resolved by the driver's own
   * session logic rather than a copy of it, so the token request cannot drift from the JDBC
   * connection when the driver changes how it reads proxy settings.
   */
  static HttpClientSettingsKey jdbcHttpClientSettings(Properties jdbcProperties) {
    SFSession session = new SFSession();
    try {
      for (Map.Entry<Object, Object> property : jdbcProperties.entrySet()) {
        session.addSFSessionProperty(property.getKey().toString(), property.getValue());
      }
      return session.getHttpClientKey();
    } catch (SFException e) {
      throw SnowflakeErrors.ERROR_1004.getException(e.getMessage());
    } catch (SnowflakeSQLException e) {
      throw SnowflakeErrors.ERROR_1004.getException(e);
    }
  }

  static String fetchAccessToken(
      URL url,
      String clientId,
      Password clientSecret,
      Optional<Password> refreshToken,
      Optional<String> scope,
      CloseableHttpClient httpClient) {
    String basicAuth =
        BASIC_AUTH_HEADER_PREFIX
            + Base64.getEncoder()
                .encodeToString(
                    (clientId + ":" + clientSecret.value()).getBytes(StandardCharsets.UTF_8));

    Map<String, String> formParams = new LinkedHashMap<>();
    refreshToken
        .map(Password::value)
        .ifPresentOrElse(
            value -> {
              formParams.put(GRANT_TYPE_PARAM, GRANT_TYPE_REFRESH_TOKEN);
              formParams.put(REFRESH_TOKEN, value);
              formParams.put(REDIRECT_URI, DEFAULT_REDIRECT_URI);
            },
            () -> formParams.put(GRANT_TYPE_PARAM, GRANT_TYPE_CLIENT_CREDENTIALS));
    scope.ifPresent(value -> formParams.put(SCOPE, value));

    String formBody =
        formParams.entrySet().stream()
            .map(
                entry ->
                    URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8)
                        + "="
                        + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8))
            .collect(Collectors.joining("&"));

    URI tokenUri = buildTokenUri(url);

    RetryPolicy<String> retryPolicy =
        RetryPolicy.<String>builder()
            .handle(Exception.class)
            // A SnowflakeKafkaConnectorException here is terminal (bad credentials, malformed
            // request, or a response without an access_token) -- retrying cannot make it succeed,
            // so only genuinely transient failures (IOException/timeout) are retried.
            .abortOn(SnowflakeKafkaConnectorException.class)
            .withBackoff(INITIAL_DELAY, MAX_DELAY)
            .withMaxRetries(MAX_RETRIES)
            .onRetry(
                event ->
                    LOGGER.warn(
                        "OAuth token fetch retry attempt #{} due to: {}",
                        event.getAttemptCount(),
                        event.getLastException().getMessage()))
            .build();

    try {
      return Failsafe.with(retryPolicy)
          .get(
              () -> {
                HttpPost request = new HttpPost(tokenUri);
                request.setHeader("Authorization", basicAuth);
                request.setEntity(
                    new StringEntity(
                        formBody,
                        ContentType.create(OAUTH_CONTENT_TYPE_HEADER, StandardCharsets.UTF_8)));
                int statusCode;
                String body;
                try (CloseableHttpResponse response = httpClient.execute(request)) {
                  statusCode = response.getStatusLine().getStatusCode();
                  body =
                      response.getEntity() == null
                          ? ""
                          : EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                }

                if (statusCode < 200 || statusCode >= 300) {
                  String message = "HTTP " + statusCode + ": " + body;
                  // 5xx and 429 are transient -- rethrow as IOException so the retry policy handles
                  // them. Every other non-2xx status (e.g. 400/401/403) is a permanent auth/request
                  // error that retrying cannot fix, so surface it as the terminal exception, which
                  // the retry policy aborts on.
                  if (statusCode >= 500 || statusCode == HTTP_TOO_MANY_REQUESTS) {
                    throw new IOException(message);
                  }
                  throw SnowflakeErrors.ERROR_1004.getException(message);
                }

                JsonNode json = MAPPER.readTree(body);
                JsonNode accessTokenNode = json.get(ACCESS_TOKEN);
                if (accessTokenNode == null || accessTokenNode.isNull()) {
                  throw SnowflakeErrors.ERROR_1004.getException(
                      "Response does not contain access_token: " + body);
                }
                return accessTokenNode.asText();
              });
    } catch (SnowflakeKafkaConnectorException e) {
      throw e;
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_1004.getException(e);
    }
  }

  private static URI buildTokenUri(URL url) {
    try {
      String path = url.path();
      if (path.isEmpty()) {
        path = TOKEN_REQUEST_ENDPOINT;
      }
      return new URI(
          url.getScheme(),
          null,
          url.hostWithPort().split(":")[0],
          Integer.parseInt(url.hostWithPort().split(":")[1]),
          path,
          null,
          null);
    } catch (Exception e) {
      throw SnowflakeErrors.ERROR_0033.getException(
          "Failed to build OAuth token URI: " + e.getMessage());
    }
  }
}
