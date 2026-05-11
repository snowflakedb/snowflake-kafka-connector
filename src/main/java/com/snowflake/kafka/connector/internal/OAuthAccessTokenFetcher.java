package com.snowflake.kafka.connector.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

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
 */
public class OAuthAccessTokenFetcher {

  private static final KCLogger LOGGER = new KCLogger(OAuthAccessTokenFetcher.class.getName());
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Fetch an OAuth access token.
   *
   * @param url OAuth server URL (scheme, host, port, path)
   * @param clientId OAuth client ID
   * @param clientSecret OAuth client secret
   * @param refreshToken OAuth refresh token; empty/null selects client_credentials grant
   * @return the access token string
   * @throws SnowflakeKafkaConnectorException if the token fetch fails
   */
  public static String fetchAccessToken(
      URL url, String clientId, String clientSecret, String refreshToken) {

    String basicAuth =
        OAuthConstants.BASIC_AUTH_HEADER_PREFIX
            + Base64.getEncoder()
                .encodeToString((clientId + ":" + clientSecret).getBytes(StandardCharsets.UTF_8));

    Map<String, String> formParams = new LinkedHashMap<>();
    if (refreshToken != null && !refreshToken.isEmpty()) {
      formParams.put(OAuthConstants.GRANT_TYPE_PARAM, OAuthConstants.GRANT_TYPE_REFRESH_TOKEN);
      formParams.put(OAuthConstants.REFRESH_TOKEN, refreshToken);
      formParams.put(OAuthConstants.REDIRECT_URI, OAuthConstants.DEFAULT_REDIRECT_URI);
    } else {
      formParams.put(OAuthConstants.GRANT_TYPE_PARAM, OAuthConstants.GRANT_TYPE_CLIENT_CREDENTIALS);
    }

    String formBody =
        formParams.entrySet().stream()
            .map(
                entry ->
                    URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8)
                        + "="
                        + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8))
            .collect(Collectors.joining("&"));

    URI tokenUri = buildTokenUri(url);

    HttpRequest request =
        HttpRequest.newBuilder()
            .uri(tokenUri)
            .header("Content-Type", OAuthConstants.OAUTH_CONTENT_TYPE_HEADER)
            .header("Authorization", basicAuth)
            .POST(HttpRequest.BodyPublishers.ofString(formBody))
            .build();

    try {
      HttpClient client = HttpClient.newHttpClient();
      HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() < 200 || response.statusCode() >= 300) {
        throw SnowflakeErrors.ERROR_1004.getException(
            "HTTP " + response.statusCode() + ": " + response.body());
      }

      JsonNode json = MAPPER.readTree(response.body());
      JsonNode accessTokenNode = json.get(OAuthConstants.ACCESS_TOKEN);
      if (accessTokenNode == null || accessTokenNode.isNull()) {
        throw SnowflakeErrors.ERROR_1004.getException(
            "Response does not contain access_token: " + response.body());
      }
      return accessTokenNode.asText();
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
        path = OAuthConstants.TOKEN_REQUEST_ENDPOINT;
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
