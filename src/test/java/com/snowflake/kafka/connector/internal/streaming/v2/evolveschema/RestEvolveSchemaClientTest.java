package com.snowflake.kafka.connector.internal.streaming.v2.evolveschema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RestEvolveSchemaClientTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Instant NOW = Instant.ofEpochSecond(1_700_000_000L);
  private static final String HOSTNAME_PATH = "/v2/streaming/hostname";
  private static final String TOKEN_PATH = "/oauth/token";
  private static final String EVOLVE_PATH =
      "/v2/streaming/databases/DB/schemas/PUBLIC/pipes/%22MY%20PIPE%22:evolve-schema";
  private static final String OK_BODY =
      "{\"columns_added\":[\"A\"],\"columns_already_present\":[\"B\"],"
          + "\"columns_type_changed\":[\"C\"],\"columns_made_nullable\":[],"
          + "\"pipe_shutdown\":true,\"unknown\":1}";

  private FakeServer server;
  private MutableClock clock;

  @BeforeEach
  void setUp() throws Exception {
    server = new FakeServer();
    clock = new MutableClock(NOW);
    server.respond(HOSTNAME_PATH, 200, "my_acct.ingest.snowflakecomputing.com\n");
    server.respond(TOKEN_PATH, 200, scopedToken(NOW.plus(Duration.ofHours(1))));
  }

  @Test
  void evolvesSchemaThroughHostnameAndScopedToken() throws Exception {
    server.respond(EVOLVE_PATH, 200, OK_BODY);

    EvolveSchemaResponse response = evolve(client(oauthCredential("ANALYST ROLE")));

    assertThat(response.getColumnsAdded()).containsExactly("A");
    assertThat(response.getColumnsAlreadyPresent()).containsExactly("B");
    assertThat(response.getColumnsTypeChanged()).containsExactly("C");
    assertThat(response.getColumnsMadeNullable()).isEmpty();
    assertThat(response.isPipeShutdown()).isTrue();

    assertThat(server.requests).hasSize(3);
    Recorded hostname = server.requests.get(0);
    assertThat(hostname.request.uri().toString())
        .isEqualTo("https://myacct.snowflakecomputing.com:443" + HOSTNAME_PATH);
    assertThat(hostname.request.method()).isEqualTo("GET");
    assertThat(header(hostname, "Authorization")).isEqualTo("Bearer user-token");
    assertThat(header(hostname, RestEvolveSchemaClient.TOKEN_TYPE_HEADER)).isEqualTo("OAUTH");

    Recorded token = server.requests.get(1);
    assertThat(token.request.uri().toString())
        .isEqualTo("https://myacct.snowflakecomputing.com:443" + TOKEN_PATH);
    assertThat(header(token, "Content-Type")).isEqualTo("application/x-www-form-urlencoded");
    assertThat(token.body)
        .isEqualTo(
            "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange"
                + "&scope=my-acct.ingest.snowflakecomputing.com+session%3Arole%3AANALYST%2520ROLE"
                + "&subject_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Aaccess_token");

    Recorded evolve = server.requests.get(2);
    assertThat(evolve.request.method()).isEqualTo("POST");
    assertThat(evolve.request.uri().getScheme() + "://" + evolve.request.uri().getAuthority())
        .isEqualTo("https://my-acct.ingest.snowflakecomputing.com:443");
    assertThat(evolve.request.uri().getRawPath()).isEqualTo(EVOLVE_PATH);
    assertThat(evolve.request.uri().getQuery()).matches("requestId=[0-9a-f-]{36}&retryCount=0");
    assertThat(header(evolve, "Authorization")).isEqualTo("Bearer " + server.lastToken);
    assertThat(header(evolve, RestEvolveSchemaClient.TOKEN_TYPE_HEADER)).isEqualTo("OAUTH");
    assertThat(header(evolve, "Content-Type")).isEqualTo("application/json");
    assertThat(header(evolve, "Accept")).isEqualTo("application/json");
    assertThat(header(evolve, "User-Agent")).startsWith("SnowflakeKafkaConnector/");

    JsonNode body = MAPPER.readTree(evolve.body);
    assertThat(body.get("schema_type").asText()).isEqualTo("KAFKA_CONNECT");
    assertThat(body.get("schema").get("type").asText()).isEqualTo("struct");
    assertThat(body.get("channel_name").asText()).isEqualTo("CH");
    assertThat(body.get("offset_token").asText()).isEqualTo("42");
    assertThat(body.get("normalize_column_names").asBoolean()).isFalse();
  }

  @Test
  void keyPairCredentialUsesJwtBearerGrantWithoutRole() throws Exception {
    server.respond(EVOLVE_PATH, 200, OK_BODY);
    UserCredential jwt =
        new UserCredential(
            UserCredential.TOKEN_TYPE_KEYPAIR_JWT,
            UserCredential.GRANT_TYPE_JWT_BEARER,
            null,
            null,
            () -> "jwt");

    evolve(client(jwt));

    assertThat(header(server.requests.get(1), RestEvolveSchemaClient.TOKEN_TYPE_HEADER))
        .isEqualTo("KEYPAIR_JWT");
    assertThat(server.requests.get(1).body)
        .isEqualTo(
            "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Ajwt-bearer"
                + "&scope=my-acct.ingest.snowflakecomputing.com");
  }

  @Test
  void spcsCredentialRequestsNoRole() throws Exception {
    server.respond(EVOLVE_PATH, 200, OK_BODY);

    evolve(client(UserCredential.spcs(() -> "spcs-token")));

    assertThat(header(server.requests.get(0), "Authorization")).isEqualTo("Bearer spcs-token");
    assertThat(server.requests.get(1).body)
        .isEqualTo(
            "grant_type=urn%3Aietf%3Aparams%3Aoauth%3Agrant-type%3Atoken-exchange"
                + "&scope=my-acct.ingest.snowflakecomputing.com"
                + "&subject_token_type=urn%3Aietf%3Aparams%3Aoauth%3Atoken-type%3Aaccess_token");
  }

  @Test
  void reusesHostnameAndScopedTokenUntilNearExpiry() throws Exception {
    server.respond(EVOLVE_PATH, 200, OK_BODY);
    server.respond(EVOLVE_PATH, 200, OK_BODY);
    server.respond(EVOLVE_PATH, 200, OK_BODY);
    server.respond(TOKEN_PATH, 200, scopedToken(NOW.plus(Duration.ofHours(2))));
    RestEvolveSchemaClient client = client(oauthCredential(null));

    evolve(client);
    clock.now = NOW.plus(Duration.ofMinutes(54));
    evolve(client);
    clock.now = NOW.plus(Duration.ofMinutes(56));
    evolve(client);

    assertThat(server.paths())
        .containsExactly(
            HOSTNAME_PATH, TOKEN_PATH, EVOLVE_PATH, EVOLVE_PATH, TOKEN_PATH, EVOLVE_PATH);
  }

  @Test
  void retriesTransientStatusesWithIncreasingRetryCount() throws Exception {
    server.respond(EVOLVE_PATH, 503, "unavailable");
    server.respond(EVOLVE_PATH, 429, "slow down");
    server.respond(EVOLVE_PATH, 200, OK_BODY);

    evolve(client(oauthCredential(null)));

    List<Recorded> evolves = server.requestsTo(EVOLVE_PATH);
    assertThat(evolves).hasSize(3);
    String requestId = evolves.get(0).request.uri().getQuery().split("&")[0];
    for (int i = 0; i < evolves.size(); i++) {
      assertThat(evolves.get(i).request.uri().getQuery()).isEqualTo(requestId + "&retryCount=" + i);
    }
  }

  @Test
  void retriesExhaustedKeepsLastStatus() {
    for (int i = 0; i < 5; i++) {
      server.respond(EVOLVE_PATH, 503, "{\"code\":\"UNAVAILABLE\",\"message\":\"try later\"}");
    }

    assertThatExceptionOfType(EvolveSchemaException.class)
        .isThrownBy(() -> evolve(client(oauthCredential(null))))
        .satisfies(
            e -> {
              assertThat(e.getStatusCode()).isEqualTo(503);
              assertThat(e.getErrorCode()).isEqualTo("UNAVAILABLE");
            });
    assertThat(server.requestsTo(EVOLVE_PATH)).hasSize(5);
  }

  @Test
  void refreshesScopedTokenOn401() throws Exception {
    server.respond(EVOLVE_PATH, 401, "expired");
    server.respond(EVOLVE_PATH, 200, OK_BODY);
    server.respond(TOKEN_PATH, 200, scopedToken(NOW.plus(Duration.ofHours(2))));

    evolve(client(oauthCredential(null)));

    assertThat(server.paths())
        .containsExactly(HOSTNAME_PATH, TOKEN_PATH, EVOLVE_PATH, TOKEN_PATH, EVOLVE_PATH);
    assertThat(header(server.requests.get(4), "Authorization"))
        .isEqualTo("Bearer " + server.lastToken);
  }

  @Test
  void repeated401IsReported() {
    server.respond(EVOLVE_PATH, 401, "no");
    server.respond(EVOLVE_PATH, 401, "still no");

    assertThatExceptionOfType(EvolveSchemaException.class)
        .isThrownBy(() -> evolve(client(oauthCredential(null))))
        .satisfies(e -> assertThat(e.getStatusCode()).isEqualTo(401));
  }

  @Test
  void clientErrorIsParsedAndNotRetried() {
    server.respond(
        EVOLVE_PATH,
        400,
        "{\"code\":\"400\",\"error_code\":\"ERR_EVOLVE_SCHEMA_INCOMPATIBLE_COLUMN_TYPE\","
            + "\"message\":\"Column A: cannot change NUMBER(38,0) to VARCHAR\"}");

    assertThatExceptionOfType(EvolveSchemaException.class)
        .isThrownBy(() -> evolve(client(oauthCredential(null))))
        .satisfies(
            e -> {
              assertThat(e.getStatusCode()).isEqualTo(400);
              assertThat(e.getErrorCode()).isEqualTo("ERR_EVOLVE_SCHEMA_INCOMPATIBLE_COLUMN_TYPE");
              assertThat(e.getMessage()).contains("cannot change NUMBER(38,0) to VARCHAR");
            });
    assertThat(server.requestsTo(EVOLVE_PATH)).hasSize(1);
  }

  @Test
  void scopedTokenRejectionIsReportedWithStatus() {
    server.responses.get(TOKEN_PATH).clear();
    server.respond(TOKEN_PATH, 403, "forbidden");

    assertThatExceptionOfType(EvolveSchemaException.class)
        .isThrownBy(() -> evolve(client(oauthCredential(null))))
        .satisfies(e -> assertThat(e.getStatusCode()).isEqualTo(403));
    assertThat(server.requestsTo(EVOLVE_PATH)).isEmpty();
  }

  @Test
  void replacesUnderscoresOnlyBeforeIngest() {
    assertThat(RestEvolveSchemaClient.replaceUnderscoresInSubdomain("a_b.ingest.x_y.com"))
        .isEqualTo("a-b.ingest.x_y.com");
    assertThat(RestEvolveSchemaClient.replaceUnderscoresInSubdomain("a_b.example.com"))
        .isEqualTo("a_b.example.com");
  }

  private EvolveSchemaResponse evolve(RestEvolveSchemaClient client) throws Exception {
    JsonNode schema = MAPPER.readTree("{\"type\":\"struct\",\"fields\":[]}");
    return client.evolveSchema(
        "DB",
        "PUBLIC",
        "\"MY PIPE\"",
        new EvolveSchemaRequest(
            EvolveSchemaRequest.SCHEMA_TYPE_KAFKA_CONNECT, schema, "CH", "42", false));
  }

  private RestEvolveSchemaClient client(UserCredential credential) {
    return new RestEvolveSchemaClient(
        server.httpClient,
        credential,
        "https",
        "myacct.snowflakecomputing.com",
        443,
        clock,
        Duration.ofMillis(1),
        Duration.ofMillis(2));
  }

  private static UserCredential oauthCredential(String role) {
    return UserCredential.oauth(role, () -> "user-token");
  }

  private static String header(Recorded recorded, String name) {
    return recorded.request.headers().firstValue(name).orElse(null);
  }

  private static int tokenCounter = 0;

  private static String scopedToken(Instant expiry) {
    Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
    String payload = "{\"exp\":" + expiry.getEpochSecond() + ",\"n\":" + (tokenCounter++) + "}";
    return encoder.encodeToString("{\"alg\":\"none\"}".getBytes(StandardCharsets.UTF_8))
        + "."
        + encoder.encodeToString(payload.getBytes(StandardCharsets.UTF_8))
        + ".sig";
  }

  private static final class Recorded {
    final HttpRequest request;
    final String body;

    Recorded(HttpRequest request, String body) {
      this.request = request;
      this.body = body;
    }
  }

  /** Mock HttpClient that answers by request path from queued responses and records requests. */
  private static final class FakeServer {
    final HttpClient httpClient = mock(HttpClient.class);
    final Map<String, Deque<HttpResponse<String>>> responses = new HashMap<>();
    final Map<HttpResponse<String>, String> bodies = new HashMap<>();
    final List<Recorded> requests = new ArrayList<>();
    String lastToken;

    @SuppressWarnings("unchecked")
    FakeServer() throws Exception {
      when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
          .thenAnswer(
              invocation -> {
                HttpRequest request = invocation.getArgument(0);
                requests.add(new Recorded(request, readBody(request)));
                Deque<HttpResponse<String>> queue = responses.get(request.uri().getRawPath());
                if (queue == null || queue.isEmpty()) {
                  throw new AssertionError("Unexpected request to " + request.uri());
                }
                HttpResponse<String> response = queue.size() > 1 ? queue.poll() : queue.peek();
                if (request.uri().getRawPath().equals(TOKEN_PATH)) {
                  lastToken = bodies.get(response);
                }
                return response;
              });
    }

    /** Queues a response; the last queued response for a path is repeated. */
    @SuppressWarnings("unchecked")
    void respond(String path, int status, String body) {
      HttpResponse<String> response = mock(HttpResponse.class);
      when(response.statusCode()).thenReturn(status);
      when(response.body()).thenReturn(body);
      bodies.put(response, body);
      responses.computeIfAbsent(path, p -> new ArrayDeque<>()).add(response);
    }

    List<String> paths() {
      List<String> paths = new ArrayList<>();
      requests.forEach(r -> paths.add(r.request.uri().getRawPath()));
      return paths;
    }

    List<Recorded> requestsTo(String path) {
      List<Recorded> matching = new ArrayList<>();
      requests.stream()
          .filter(r -> r.request.uri().getRawPath().equals(path))
          .forEach(matching::add);
      return matching;
    }
  }

  private static String readBody(HttpRequest request) throws InterruptedException {
    if (request.bodyPublisher().isEmpty()) {
      return "";
    }
    StringBuilder body = new StringBuilder();
    CountDownLatch done = new CountDownLatch(1);
    request
        .bodyPublisher()
        .get()
        .subscribe(
            new Flow.Subscriber<>() {
              @Override
              public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
              }

              @Override
              public void onNext(ByteBuffer item) {
                body.append(StandardCharsets.UTF_8.decode(item));
              }

              @Override
              public void onError(Throwable throwable) {
                done.countDown();
              }

              @Override
              public void onComplete() {
                done.countDown();
              }
            });
    done.await(5, TimeUnit.SECONDS);
    return body.toString();
  }

  private static final class MutableClock extends Clock {
    Instant now;

    MutableClock(Instant now) {
      this.now = now;
    }

    @Override
    public ZoneOffset getZone() {
      return ZoneOffset.UTC;
    }

    @Override
    public Clock withZone(java.time.ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return now;
    }
  }
}
