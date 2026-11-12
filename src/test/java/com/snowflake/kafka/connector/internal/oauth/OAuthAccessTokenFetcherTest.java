package com.snowflake.kafka.connector.internal.oauth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.config.ProxyConfig;
import com.snowflake.kafka.connector.internal.EmbeddedProxyServer;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

class OAuthAccessTokenFetcherTest {

  private HttpClient httpClient;
  private HttpResponse<String> httpResponse;
  private OAuthURL url;

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() throws Exception {
    httpClient = mock(HttpClient.class);
    httpResponse = mock(HttpResponse.class);
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(httpResponse);
    url = OAuthURL.from("https://oauth.example.com:443/oauth/token-request");
  }

  @Test
  void fetchAccessToken_refreshTokenGrant_returnsToken() {
    when(httpResponse.statusCode()).thenReturn(200);
    when(httpResponse.body()).thenReturn("{\"access_token\": \"my-access-token\"}");

    String token =
        OAuthAccessTokenFetcher.fetchAccessToken(
            url,
            "client_id",
            new Password("client_secret"),
            Optional.of(new Password("my_refresh_token")),
            Optional.empty(),
            httpClient);

    assertThat(token).isEqualTo("my-access-token");
  }

  @Test
  void fetchAccessToken_clientCredentialsGrant_returnsToken() {
    when(httpResponse.statusCode()).thenReturn(200);
    when(httpResponse.body()).thenReturn("{\"access_token\": \"cc-token\"}");

    String token =
        OAuthAccessTokenFetcher.fetchAccessToken(
            url,
            "client_id",
            new Password("client_secret"),
            Optional.empty(),
            Optional.empty(),
            httpClient);

    assertThat(token).isEqualTo("cc-token");
  }

  @Test
  void fetchAccessToken_emptyRefreshToken_usesClientCredentials() {
    when(httpResponse.statusCode()).thenReturn(200);
    when(httpResponse.body()).thenReturn("{\"access_token\": \"cc-token\"}");

    String token =
        OAuthAccessTokenFetcher.fetchAccessToken(
            url,
            "client_id",
            new Password("client_secret"),
            Optional.empty(),
            Optional.empty(),
            httpClient);

    assertThat(token).isEqualTo("cc-token");
  }

  @Test
  @SuppressWarnings("unchecked")
  void fetchAccessToken_nonSuccessStatus_throwsError1004() throws Exception {
    when(httpResponse.statusCode()).thenReturn(401);
    when(httpResponse.body()).thenReturn("Unauthorized");

    assertThatExceptionOfType(SnowflakeKafkaConnectorException.class)
        .isThrownBy(
            () ->
                OAuthAccessTokenFetcher.fetchAccessToken(
                    url,
                    "client_id",
                    new Password("client_secret"),
                    Optional.of(new Password("token")),
                    Optional.empty(),
                    httpClient))
        .matches(exception -> exception.getCode().equals("1004"));

    // A 401 is a permanent auth error -- it must not be retried.
    verify(httpClient, times(1)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  void fetchAccessToken_missingAccessTokenInResponse_throwsError1004() {
    when(httpResponse.statusCode()).thenReturn(200);
    when(httpResponse.body()).thenReturn("{\"token_type\": \"bearer\"}");

    assertThatExceptionOfType(SnowflakeKafkaConnectorException.class)
        .isThrownBy(
            () ->
                OAuthAccessTokenFetcher.fetchAccessToken(
                    url,
                    "client_id",
                    new Password("client_secret"),
                    Optional.of(new Password("token")),
                    Optional.empty(),
                    httpClient))
        .matches(exception -> exception.getCode().equals("1004"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void fetchAccessToken_httpClientThrows_throwsError1004() throws Exception {
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenThrow(new java.io.IOException("connection refused"));

    assertThatExceptionOfType(SnowflakeKafkaConnectorException.class)
        .isThrownBy(
            () ->
                OAuthAccessTokenFetcher.fetchAccessToken(
                    url,
                    "client_id",
                    new Password("client_secret"),
                    Optional.of(new Password("token")),
                    Optional.empty(),
                    httpClient))
        .matches(exception -> exception.getCode().equals("1004"));
  }

  @Test
  @SuppressWarnings("unchecked")
  void fetchAccessToken_retriesOnFailureThenSucceeds() throws Exception {
    HttpResponse<String> failResponse = mock(HttpResponse.class);
    when(failResponse.statusCode()).thenReturn(503);
    when(failResponse.body()).thenReturn("Service Unavailable");

    HttpResponse<String> successResponse = mock(HttpResponse.class);
    when(successResponse.statusCode()).thenReturn(200);
    when(successResponse.body()).thenReturn("{\"access_token\": \"retry-token\"}");

    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(failResponse)
        .thenReturn(successResponse);

    String token =
        OAuthAccessTokenFetcher.fetchAccessToken(
            url,
            "client_id",
            new Password("client_secret"),
            Optional.of(new Password("token")),
            Optional.empty(),
            httpClient);

    assertThat(token).isEqualTo("retry-token");
    verify(httpClient, times(2)).send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  void fetchAccessToken_withScope_addsScopeFormParam() throws Exception {
    when(httpResponse.statusCode()).thenReturn(200);
    when(httpResponse.body()).thenReturn("{\"access_token\": \"scoped-token\"}");

    OAuthAccessTokenFetcher.fetchAccessToken(
        url,
        "client_id",
        new Password("client_secret"),
        Optional.of(new Password("my_refresh_token")),
        Optional.of("session:role:MY_ROLE"),
        httpClient);

    ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(httpClient).send(captor.capture(), any(HttpResponse.BodyHandler.class));
    assertThat(requestBody(captor.getValue())).contains("scope=session%3Arole%3AMY_ROLE");
  }

  @Test
  @SuppressWarnings("unchecked")
  void fetchAccessToken_withoutScope_omitsScopeFormParam() throws Exception {
    when(httpResponse.statusCode()).thenReturn(200);
    when(httpResponse.body()).thenReturn("{\"access_token\": \"unscoped-token\"}");

    OAuthAccessTokenFetcher.fetchAccessToken(
        url,
        "client_id",
        new Password("client_secret"),
        Optional.of(new Password("my_refresh_token")),
        Optional.empty(),
        httpClient);

    ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(httpClient).send(captor.capture(), any(HttpResponse.BodyHandler.class));
    assertThat(requestBody(captor.getValue())).doesNotContain("scope=");
  }

  private static String requestBody(HttpRequest request) throws InterruptedException {
    HttpRequest.BodyPublisher publisher = request.bodyPublisher().orElseThrow();
    StringBuilder body = new StringBuilder();
    CountDownLatch done = new CountDownLatch(1);
    publisher.subscribe(
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
    done.await();
    return body.toString();
  }

  @Test
  void connectorProxySelectorBypassesMatchingHosts() throws Exception {
    assertThat(
            OAuthAccessTokenFetcher.bypassesProxy(
                "api.snowflakecomputing.com", "*.snowflakecomputing.com"))
        .isTrue();
    assertThat(
            OAuthAccessTokenFetcher.bypassesProxy(
                "snowflakecomputing.com", "*.snowflakecomputing.com"))
        .isFalse();
    assertThat(OAuthAccessTokenFetcher.bypassesProxy("localhost", "localhost|*.example.com"))
        .isTrue();

    ProxySelector selector =
        OAuthAccessTokenFetcher.connectorProxySelector(
            "proxy.example.com", 3128, "*.snowflakecomputing.com|localhost");
    assertThat(selector.select(URI.create("https://api.snowflakecomputing.com/oauth/token")))
        .containsExactly(Proxy.NO_PROXY);
    assertThat(selector.select(URI.create("https://oauth.example.com/token")))
        .extracting(Proxy::type)
        .containsExactly(Proxy.Type.HTTP);
  }

  private static final String TUNNELING_DISABLED_SCHEMES = Utils.JDK_HTTP_AUTH_TUNNELING;
  private static final String PROXY_USER = "proxy-user";
  private static final String PROXY_PASSWORD = "proxy-secret";

  @Test
  void fetchAccessToken_proxyRequiringBasicAuth_returnsTokenWithJdkDefaults(@TempDir Path tempDir)
      throws Exception {
    assertThat(fetchThroughAuthenticatingProxy(tempDir)).isEqualTo("proxied-token");
  }

  @Test
  void allowProxyBasicAuthForHttpsTunnels_clearsDefaultWhenProxyHasCredentials() {
    withTunnelingProperty(
        null,
        () -> {
          OAuthAccessTokenFetcher.allowProxyBasicAuthForHttpsTunnels(
              ProxyConfig.create("proxy.example.com", "3128", null, "user", "secret"));
          assertThat(System.getProperty(TUNNELING_DISABLED_SCHEMES)).isEqualTo("");
        });
  }

  @Test
  void allowProxyBasicAuthForHttpsTunnels_leavesOperatorValueAlone() {
    withTunnelingProperty(
        "Basic,Digest",
        () -> {
          OAuthAccessTokenFetcher.allowProxyBasicAuthForHttpsTunnels(
              ProxyConfig.create("proxy.example.com", "3128", null, "user", "secret"));
          assertThat(System.getProperty(TUNNELING_DISABLED_SCHEMES)).isEqualTo("Basic,Digest");
        });
  }

  @Test
  void allowProxyBasicAuthForHttpsTunnels_doesNothingWithoutProxyCredentials() {
    withTunnelingProperty(
        null,
        () -> {
          OAuthAccessTokenFetcher.allowProxyBasicAuthForHttpsTunnels(ProxyConfig.none());
          OAuthAccessTokenFetcher.allowProxyBasicAuthForHttpsTunnels(
              ProxyConfig.create("proxy.example.com", "3128", null, null, null));
          OAuthAccessTokenFetcher.allowProxyBasicAuthForHttpsTunnels(
              ProxyConfig.create(null, null, null, "user", "secret"));
          assertThat(System.getProperty(TUNNELING_DISABLED_SCHEMES)).isNull();
        });
  }

  /**
   * Runs {@code body} with the tunneling property set to {@code value} (null = unset), then
   * restores it.
   */
  private static void withTunnelingProperty(String value, Runnable body) {
    String previous = System.getProperty(TUNNELING_DISABLED_SCHEMES);
    try {
      if (value == null) {
        System.clearProperty(TUNNELING_DISABLED_SCHEMES);
      } else {
        System.setProperty(TUNNELING_DISABLED_SCHEMES, value);
      }
      body.run();
    } finally {
      if (previous == null) {
        System.clearProperty(TUNNELING_DISABLED_SCHEMES);
      } else {
        System.setProperty(TUNNELING_DISABLED_SCHEMES, previous);
      }
    }
  }

  /**
   * Fetches a token from a local HTTPS endpoint through {@link EmbeddedProxyServer}, which demands
   * Basic proxy credentials on CONNECT, with {@code jdk.http.auth.tunneling.disabledSchemes} left
   * at the JDK default (Basic disabled for HTTPS tunnels). The JDK reads that setting once per JVM,
   * on the first {@code java.net.http} use, and the other tests in this class use it. So the fetch
   * runs in a fresh child JVM, and the result does not depend on test order.
   */
  private static String fetchThroughAuthenticatingProxy(Path tempDir) throws Exception {
    EmbeddedProxyServer proxy = new EmbeddedProxyServer(PROXY_USER, PROXY_PASSWORD);
    HttpsServer tokenEndpoint = null;
    try {
      SSLContext serverContext = selfSignedServerContext(tempDir);
      tokenEndpoint =
          HttpsServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
      tokenEndpoint.setHttpsConfigurator(new HttpsConfigurator(serverContext));
      tokenEndpoint.createContext(
          "/oauth/token-request",
          exchange -> {
            try (InputStream body = exchange.getRequestBody()) {
              body.readAllBytes();
            }
            byte[] response =
                "{\"access_token\": \"proxied-token\"}".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, response.length);
            try (OutputStream out = exchange.getResponseBody()) {
              out.write(response);
            }
          });
      tokenEndpoint.start();
      proxy.start();

      Process child =
          new ProcessBuilder(
                  Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                  "-cp",
                  System.getProperty("java.class.path"),
                  "-Djavax.net.ssl.trustStore=" + tempDir.resolve("trust.p12"),
                  "-Djavax.net.ssl.trustStorePassword=changeit",
                  ProxiedFetchMain.class.getName(),
                  "https://localhost:"
                      + tokenEndpoint.getAddress().getPort()
                      + "/oauth/token-request",
                  String.valueOf(proxy.getPort()),
                  PROXY_USER,
                  PROXY_PASSWORD)
              .redirectErrorStream(true)
              .start();
      String output = new String(child.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      assertThat(child.waitFor()).as("child JVM output: %s", output).isZero();
      return output.lines().reduce((first, second) -> second).orElse("");
    } finally {
      if (tokenEndpoint != null) {
        tokenEndpoint.stop(0);
      }
      if (proxy.isRunning()) {
        proxy.stop();
      }
    }
  }

  /** Entry point for the child JVM: fetches a token through the proxy and prints it. */
  public static final class ProxiedFetchMain {
    public static void main(String[] args) {
      System.out.println(
          OAuthAccessTokenFetcher.fetchAccessToken(
              OAuthURL.from(args[0]),
              "client_id",
              new Password("client_secret"),
              Optional.empty(),
              Optional.empty(),
              ProxyConfig.create("127.0.0.1", args[1], null, args[2], args[3])));
    }
  }

  /**
   * Server SSL context for a throwaway self-signed certificate for localhost. Also writes {@code
   * trust.p12}, a trust store holding just that certificate, next to the key store.
   */
  private static SSLContext selfSignedServerContext(Path tempDir) throws Exception {
    Path keystorePath = tempDir.resolve("server.p12");
    char[] password = "changeit".toCharArray();
    Process keytool =
        new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "keytool").toString(),
                "-genkeypair",
                "-alias",
                "server",
                "-keyalg",
                "RSA",
                "-keysize",
                "2048",
                "-validity",
                "1",
                "-dname",
                "CN=localhost",
                "-ext",
                "san=dns:localhost,ip:127.0.0.1",
                "-storetype",
                "PKCS12",
                "-keystore",
                keystorePath.toString(),
                "-storepass",
                "changeit")
            .redirectErrorStream(true)
            .start();
    String output = new String(keytool.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
    assertThat(keytool.waitFor()).as("keytool: %s", output).isZero();

    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    try (InputStream in = java.nio.file.Files.newInputStream(keystorePath)) {
      keyStore.load(in, password);
    }
    KeyStore trustStore = KeyStore.getInstance("PKCS12");
    trustStore.load(null, null);
    trustStore.setCertificateEntry("server", keyStore.getCertificate("server"));
    try (OutputStream out = java.nio.file.Files.newOutputStream(tempDir.resolve("trust.p12"))) {
      trustStore.store(out, password);
    }

    KeyManagerFactory keyManagers =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagers.init(keyStore, password);
    SSLContext server = SSLContext.getInstance("TLS");
    server.init(keyManagers.getKeyManagers(), null, null);
    return server;
  }
}
