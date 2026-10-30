package com.snowflake.kafka.connector.internal.streaming.v2.evolveschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.SnowflakeURL;
import com.snowflake.kafka.connector.internal.spcs.SpcsEnvironment;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import java.io.IOException;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.UUID;

/**
 * Calls the SSv2 {@code :evolve-schema} pipe endpoint over REST, authenticating the way the
 * streaming SDK does:
 *
 * <ol>
 *   <li>{@code GET /v2/streaming/hostname} on the account host returns the streaming host.
 *   <li>{@code POST /oauth/token} on the account host exchanges the user credential for a token
 *       scoped to that host.
 *   <li>The endpoint is called on the streaming host with the scoped token.
 * </ol>
 *
 * <p>The streaming host and scoped token are cached; the token is refreshed shortly before it
 * expires, or after a 401. Transient failures (I/O errors, 429, 5xx) are retried with backoff.
 */
public class RestEvolveSchemaClient implements EvolveSchemaClient {

  private static final KCLogger LOGGER = new KCLogger(RestEvolveSchemaClient.class.getName());
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static final String TOKEN_TYPE_HEADER = "X-Snowflake-Authorization-Token-Type";
  private static final String SCOPED_TOKEN_TYPE = "OAUTH";
  private static final int HTTP_UNAUTHORIZED = 401;
  private static final int HTTP_TOO_MANY_REQUESTS = 429;

  private static final Duration REQUEST_TIMEOUT = Duration.ofMinutes(2);
  private static final Duration TOKEN_REFRESH_MARGIN = Duration.ofMinutes(5);
  // Used when the scoped token's expiry can't be read.
  private static final Duration DEFAULT_TOKEN_LIFETIME = Duration.ofMinutes(10);

  private static final int MAX_RETRIES = 4;
  private static final Duration INITIAL_DELAY = Duration.ofSeconds(1);
  private static final Duration MAX_DELAY = Duration.ofSeconds(8);

  private final HttpClient httpClient;
  private final UserCredential credential;
  private final String accountBaseUrl;
  private final String scheme;
  private final int port;
  private final Clock clock;
  private final RetryPolicy<EvolveSchemaResponse> retryPolicy;

  private String streamingHost;
  private String scopedToken;
  private Instant scopedTokenRefreshAt = Instant.MIN;

  RestEvolveSchemaClient(
      HttpClient httpClient,
      UserCredential credential,
      String scheme,
      String accountHost,
      int port,
      Clock clock,
      Duration initialRetryDelay,
      Duration maxRetryDelay) {
    this.httpClient = httpClient;
    this.credential = credential;
    this.scheme = scheme;
    this.port = port;
    this.accountBaseUrl = scheme + "://" + accountHost + ":" + port;
    this.clock = clock;
    this.retryPolicy =
        RetryPolicy.<EvolveSchemaResponse>builder()
            .handle(IOException.class)
            .withBackoff(initialRetryDelay, maxRetryDelay)
            .withMaxRetries(MAX_RETRIES)
            .onRetry(
                event ->
                    LOGGER.warn(
                        "Evolve-schema retry attempt #{} due to: {}",
                        event.getAttemptCount(),
                        event.getLastException().getMessage()))
            .build();
  }

  /** Creates a client for the account and credential in {@code config}. */
  public static RestEvolveSchemaClient from(SinkTaskConfig config) {
    if (config.getSnowflakeUrl() == null || config.getSnowflakeUrl().isBlank()) {
      throw SnowflakeErrors.ERROR_0017.getException();
    }
    SnowflakeURL url = new SnowflakeURL(config.getSnowflakeUrl());
    String account =
        config.getAuthenticator().suppliesAmbientIdentity()
            ? SpcsEnvironment.account().orElseGet(url::getAccount)
            : url.getAccount();
    String hostWithPort = url.hostWithPort();
    int port = Integer.parseInt(hostWithPort.substring(hostWithPort.lastIndexOf(':') + 1));
    return new RestEvolveSchemaClient(
        HttpClient.newBuilder().proxy(ProxySelector.getDefault()).build(),
        UserCredential.from(config, url, account),
        url.getScheme(),
        url.getUrlWithoutPort(),
        port,
        Clock.systemUTC(),
        INITIAL_DELAY,
        MAX_DELAY);
  }

  @Override
  public synchronized EvolveSchemaResponse evolveSchema(
      String database, String schema, String pipe, EvolveSchemaRequest request) {
    String path =
        "/v2/streaming/databases/"
            + encodePathSegment(database)
            + "/schemas/"
            + encodePathSegment(schema)
            + "/pipes/"
            + encodePathSegment(pipe)
            + ":evolve-schema";
    String body;
    try {
      body = MAPPER.writeValueAsString(request);
    } catch (IOException e) {
      throw new EvolveSchemaException("Failed to serialize the evolve-schema request", e);
    }
    String requestId = UUID.randomUUID().toString();

    try {
      return Failsafe.with(retryPolicy)
          .get(ctx -> callEvolveSchema(path, body, requestId, ctx.getAttemptCount()));
    } catch (FailsafeException e) {
      Throwable cause = e.getCause() == null ? e : e.getCause();
      if (cause instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      if (cause instanceof TransientStatusException) {
        TransientStatusException transientStatus = (TransientStatusException) cause;
        throw toEvolveSchemaException(transientStatus.statusCode, transientStatus.body);
      }
      throw new EvolveSchemaException(
          "Evolve-schema request " + requestId + " failed: " + cause.getMessage(), cause);
    } catch (SnowflakeKafkaConnectorException e) {
      throw new EvolveSchemaException("Evolve-schema authentication failed: " + e.getMessage(), e);
    }
  }

  private EvolveSchemaResponse callEvolveSchema(
      String path, String body, String requestId, int retryCount)
      throws IOException, InterruptedException {
    HttpResponse<String> response = sendEvolveSchema(path, body, requestId, retryCount);
    if (response.statusCode() == HTTP_UNAUTHORIZED) {
      LOGGER.info("Evolve-schema returned 401; refreshing the scoped token");
      invalidateScopedToken();
      response = sendEvolveSchema(path, body, requestId, retryCount);
    }
    int status = response.statusCode();
    if (isTransient(status)) {
      throw new TransientStatusException(status, response.body());
    }
    if (status < 200 || status >= 300) {
      throw toEvolveSchemaException(status, response.body());
    }
    return MAPPER.readValue(response.body(), EvolveSchemaResponse.class);
  }

  private HttpResponse<String> sendEvolveSchema(
      String path, String body, String requestId, int retryCount)
      throws IOException, InterruptedException {
    String token = scopedToken();
    URI uri =
        URI.create(
            scheme
                + "://"
                + streamingHost
                + ":"
                + port
                + path
                + "?requestId="
                + requestId
                + "&retryCount="
                + retryCount);
    HttpRequest request =
        baseRequest(uri)
            .header("Authorization", "Bearer " + token)
            .header(TOKEN_TYPE_HEADER, SCOPED_TOKEN_TYPE)
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(body))
            .build();
    return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
  }

  /** Returns a scoped token valid for at least {@link #TOKEN_REFRESH_MARGIN}. */
  private String scopedToken() throws IOException, InterruptedException {
    if (scopedToken != null && clock.instant().isBefore(scopedTokenRefreshAt)) {
      return scopedToken;
    }
    String host = streamingHost();
    String userToken = credential.token();
    HttpRequest request =
        baseRequest(URI.create(accountBaseUrl + "/oauth/token"))
            .header("Authorization", "Bearer " + userToken)
            .header(TOKEN_TYPE_HEADER, credential.tokenType())
            .header("Content-Type", "application/x-www-form-urlencoded")
            .POST(HttpRequest.BodyPublishers.ofString(credential.scopedTokenRequestBody(host)))
            .build();
    String token =
        checkAccountResponse("scoped token", httpClient.send(request, ofString())).trim();
    scopedToken = token;
    scopedTokenRefreshAt = expiry(token).minus(TOKEN_REFRESH_MARGIN);
    return token;
  }

  private String streamingHost() throws IOException, InterruptedException {
    if (streamingHost != null) {
      return streamingHost;
    }
    HttpRequest request =
        baseRequest(URI.create(accountBaseUrl + "/v2/streaming/hostname"))
            .header("Authorization", "Bearer " + credential.token())
            .header(TOKEN_TYPE_HEADER, credential.tokenType())
            .GET()
            .build();
    String host = checkAccountResponse("streaming hostname", httpClient.send(request, ofString()));
    streamingHost = replaceUnderscoresInSubdomain(host.trim());
    LOGGER.info("Evolve-schema uses streaming host {}", streamingHost);
    return streamingHost;
  }

  private void invalidateScopedToken() {
    scopedToken = null;
    scopedTokenRefreshAt = Instant.MIN;
  }

  private static String checkAccountResponse(String what, HttpResponse<String> response)
      throws IOException {
    int status = response.statusCode();
    if (isTransient(status)) {
      throw new TransientStatusException(status, response.body());
    }
    if (status < 200 || status >= 300) {
      throw new EvolveSchemaException(
          status, "", "Fetching the " + what + " failed: HTTP " + status + ": " + response.body());
    }
    return response.body();
  }

  private HttpRequest.Builder baseRequest(URI uri) {
    return HttpRequest.newBuilder()
        .uri(uri)
        .timeout(REQUEST_TIMEOUT)
        .header("User-Agent", StreamingClientProperties.APPLICATION_NAME)
        .header("Accept", "application/json");
  }

  /** Reads {@code exp} (epoch seconds) from a JWT, or falls back to a short default lifetime. */
  private Instant expiry(String token) {
    String[] parts = token.split("\\.");
    if (parts.length == 3) {
      try {
        JsonNode exp = MAPPER.readTree(Base64.getUrlDecoder().decode(parts[1])).get("exp");
        if (exp != null && exp.canConvertToLong()) {
          return Instant.ofEpochSecond(exp.asLong());
        }
      } catch (IOException | IllegalArgumentException e) {
        LOGGER.warn("Could not read the scoped token's expiry: {}", e.getMessage());
      }
    }
    return clock.instant().plus(DEFAULT_TOKEN_LIFETIME).plus(TOKEN_REFRESH_MARGIN);
  }

  private static EvolveSchemaException toEvolveSchemaException(int status, String body) {
    String errorCode = "";
    String message = body;
    try {
      JsonNode json = MAPPER.readTree(body);
      if (json != null && json.isObject()) {
        errorCode = text(json, "error_code");
        if (errorCode.isEmpty()) {
          errorCode = text(json, "code");
        }
        String bodyMessage = text(json, "message");
        if (!bodyMessage.isEmpty()) {
          message = bodyMessage;
        }
      }
    } catch (IOException e) {
      // Not JSON; keep the raw body as the message.
    }
    return new EvolveSchemaException(
        status,
        errorCode,
        "Evolve-schema failed: HTTP " + status + " " + errorCode + ": " + message);
  }

  private static String text(JsonNode json, String field) {
    JsonNode node = json.get(field);
    return node == null || node.isNull() ? "" : node.asText();
  }

  private static boolean isTransient(int status) {
    return status >= 500 || status == HTTP_TOO_MANY_REQUESTS;
  }

  private static HttpResponse.BodyHandler<String> ofString() {
    return HttpResponse.BodyHandlers.ofString();
  }

  /** Percent-encodes one path segment; a space becomes {@code %20}. */
  static String encodePathSegment(String segment) {
    return URLEncoder.encode(segment, StandardCharsets.UTF_8).replace("+", "%20");
  }

  /**
   * Replaces underscores with hyphens in the account part of a {@code <account>.ingest...} host, as
   * the streaming SDK does, since underscores are not valid in host names.
   */
  static String replaceUnderscoresInSubdomain(String host) {
    int ingest = host.indexOf(".ingest");
    if (ingest < 0) {
      return host;
    }
    return host.substring(0, ingest).replace('_', '-') + host.substring(ingest);
  }

  /** A retryable non-2xx status: 429 or 5xx. */
  private static final class TransientStatusException extends IOException {
    private final int statusCode;
    private final String body;

    TransientStatusException(int statusCode, String body) {
      super("HTTP " + statusCode + ": " + body);
      this.statusCode = statusCode;
      this.body = body;
    }
  }
}
