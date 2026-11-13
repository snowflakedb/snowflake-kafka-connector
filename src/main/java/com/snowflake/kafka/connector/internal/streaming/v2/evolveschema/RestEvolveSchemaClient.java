package com.snowflake.kafka.connector.internal.streaming.v2.evolveschema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.snowflake.kafka.connector.internal.SnowflakeURL;
import com.snowflake.kafka.connector.internal.streaming.StreamingClientProperties;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import java.io.IOException;
import java.net.Authenticator;
import java.net.ProxySelector;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Set;
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
 * expires, or after a 401. Transient failures (I/O errors other than timeouts, 429, 500, 502, 503,
 * 504) are retried with backoff, for at most {@link #MAX_RETRY_DURATION}, so that a call returns
 * well within Kafka's default {@code max.poll.interval.ms} of 5 minutes.
 */
public class RestEvolveSchemaClient implements EvolveSchemaClient {

  private static final KCLogger LOGGER = new KCLogger(RestEvolveSchemaClient.class.getName());
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static final String TOKEN_TYPE_HEADER = "X-Snowflake-Authorization-Token-Type";
  private static final String SCOPED_TOKEN_TYPE = "OAUTH";
  private static final int HTTP_UNAUTHORIZED = 401;
  private static final Set<Integer> TRANSIENT_STATUSES = Set.of(429, 500, 502, 503, 504);

  // Snowflake waits up to 60 seconds for the pipe's owning node to run the DDL and restart the
  // pipe, so the evolve-schema request gets longer than the hostname and token requests.
  private static final Duration EVOLVE_SCHEMA_TIMEOUT = Duration.ofSeconds(75);
  private static final Duration ACCOUNT_REQUEST_TIMEOUT = Duration.ofSeconds(20);
  private static final Duration MAX_RETRY_DURATION = Duration.ofSeconds(90);
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
  private final RetryPolicy<String> retryPolicy;

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
        RetryPolicy.<String>builder()
            // A timed-out request may still be running on Snowflake; retrying it would only add
            // another full timeout.
            .handleIf(e -> e instanceof IOException && !(e instanceof HttpTimeoutException))
            .withBackoff(initialRetryDelay, maxRetryDelay)
            .withMaxRetries(MAX_RETRIES)
            .withMaxDuration(MAX_RETRY_DURATION)
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
    // The account only goes into a key-pair JWT, so the SPCS account lookup isn't needed here.
    return new RestEvolveSchemaClient(
        httpClient(Authenticator.getDefault()),
        UserCredential.from(config, url, url.getAccount()),
        url.getScheme(),
        url.getUrlWithoutPort(),
        url.getPort(),
        Clock.systemUTC(),
        INITIAL_DELAY,
        MAX_DELAY);
  }

  /**
   * An HTTP client that uses the JVM's proxy settings, and {@code authenticator} for proxy
   * credentials. {@code Utils.enableJVMProxy} installs those as the default {@link Authenticator},
   * which, unlike {@code HttpURLConnection}, {@link HttpClient} ignores unless it's set on the
   * builder.
   *
   * @param authenticator may be null
   */
  static HttpClient httpClient(Authenticator authenticator) {
    HttpClient.Builder builder = HttpClient.newBuilder().proxy(ProxySelector.getDefault());
    if (authenticator != null) {
      builder.authenticator(authenticator);
    }
    return builder.build();
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

    String responseBody;
    try {
      responseBody =
          Failsafe.with(retryPolicy)
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
    // Parsed outside the retry: the schema was applied, so an unreadable 2xx body must not resend
    // the request, which would restart the pipe again.
    try {
      return MAPPER.readValue(responseBody, EvolveSchemaResponse.class);
    } catch (IOException e) {
      throw new EvolveSchemaException(
          "Evolve-schema request " + requestId + " succeeded, but its response can't be read", e);
    }
  }

  /** Returns the body of a 2xx response. */
  private String callEvolveSchema(String path, String body, String requestId, int retryCount)
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
    return response.body();
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
        baseRequest(uri, EVOLVE_SCHEMA_TIMEOUT)
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
        baseRequest(URI.create(accountBaseUrl + "/oauth/token"), ACCOUNT_REQUEST_TIMEOUT)
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
        baseRequest(URI.create(accountBaseUrl + "/v2/streaming/hostname"), ACCOUNT_REQUEST_TIMEOUT)
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

  private HttpRequest.Builder baseRequest(URI uri, Duration timeout) {
    return HttpRequest.newBuilder()
        .uri(uri)
        .timeout(timeout)
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
    return TRANSIENT_STATUSES.contains(status);
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

  /** A retryable non-2xx status: one of {@link #TRANSIENT_STATUSES}. */
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
