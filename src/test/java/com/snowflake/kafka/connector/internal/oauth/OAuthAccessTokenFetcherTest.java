package com.snowflake.kafka.connector.internal.oauth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.kafka.connector.internal.EmbeddedProxyServer;
import com.snowflake.kafka.connector.internal.JdbcPropertyKeys;
import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import net.snowflake.client.internal.core.HttpClientSettingsKey;
import net.snowflake.client.jdbc.internal.apache.http.HttpVersion;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.CloseableHttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpPost;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpUriRequest;
import net.snowflake.client.jdbc.internal.apache.http.entity.StringEntity;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.apache.http.message.BasicStatusLine;
import net.snowflake.client.jdbc.internal.apache.http.util.EntityUtils;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;

class OAuthAccessTokenFetcherTest {

  private static final String PROXY_USER = "proxy-user";
  private static final String PROXY_PASSWORD = "proxy-secret";

  private CloseableHttpClient httpClient;
  private OAuthURL url;

  @BeforeEach
  void setUp() {
    httpClient = mock(CloseableHttpClient.class);
    url = OAuthURL.from("https://oauth.example.com:443/oauth/token-request");
  }

  @Test
  void fetchAccessToken_refreshTokenGrant_returnsToken() throws Exception {
    respondWith(response(200, "{\"access_token\": \"my-access-token\"}"));

    String token =
        OAuthAccessTokenFetcher.fetchAccessToken(
            url,
            "client_id",
            new Password("client_secret"),
            Optional.of(new Password("my_refresh_token")),
            Optional.empty(),
            httpClient);

    assertThat(token).isEqualTo("my-access-token");
    HttpPost request = sentRequest();
    assertThat(requestBody(request))
        .contains("grant_type=refresh_token", "refresh_token=my_refresh_token");
    assertThat(request.getFirstHeader("Authorization").getValue()).startsWith("Basic ");
    assertThat(request.getEntity().getContentType().getValue())
        .startsWith("application/x-www-form-urlencoded");
  }

  @Test
  void fetchAccessToken_clientCredentialsGrant_returnsToken() throws Exception {
    respondWith(response(200, "{\"access_token\": \"cc-token\"}"));

    String token =
        OAuthAccessTokenFetcher.fetchAccessToken(
            url,
            "client_id",
            new Password("client_secret"),
            Optional.empty(),
            Optional.empty(),
            httpClient);

    assertThat(token).isEqualTo("cc-token");
    assertThat(requestBody(sentRequest())).isEqualTo("grant_type=client_credentials");
  }

  @Test
  void fetchAccessToken_nonSuccessStatus_throwsError1004WithoutRetry() throws Exception {
    respondWith(response(401, "Unauthorized"));

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
    verify(httpClient, times(1)).execute(any(HttpUriRequest.class));
  }

  @Test
  void fetchAccessToken_missingAccessTokenInResponse_throwsError1004() throws Exception {
    respondWith(response(200, "{\"token_type\": \"bearer\"}"));

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
  void fetchAccessToken_httpClientThrows_throwsError1004() throws Exception {
    when(httpClient.execute(any(HttpUriRequest.class)))
        .thenThrow(new IOException("connection refused"));

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
  void fetchAccessToken_retriesOnFailureThenSucceeds() throws Exception {
    CloseableHttpResponse failure = response(503, "Service Unavailable");
    CloseableHttpResponse success = response(200, "{\"access_token\": \"retry-token\"}");
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(failure).thenReturn(success);

    String token =
        OAuthAccessTokenFetcher.fetchAccessToken(
            url,
            "client_id",
            new Password("client_secret"),
            Optional.of(new Password("token")),
            Optional.empty(),
            httpClient);

    assertThat(token).isEqualTo("retry-token");
    verify(httpClient, times(2)).execute(any(HttpUriRequest.class));
  }

  @Test
  void fetchAccessToken_withScope_addsScopeFormParam() throws Exception {
    respondWith(response(200, "{\"access_token\": \"scoped-token\"}"));

    OAuthAccessTokenFetcher.fetchAccessToken(
        url,
        "client_id",
        new Password("client_secret"),
        Optional.of(new Password("my_refresh_token")),
        Optional.of("session:role:MY_ROLE"),
        httpClient);

    assertThat(requestBody(sentRequest())).contains("scope=session%3Arole%3AMY_ROLE");
  }

  @Test
  void fetchAccessToken_withoutScope_omitsScopeFormParam() throws Exception {
    respondWith(response(200, "{\"access_token\": \"unscoped-token\"}"));

    OAuthAccessTokenFetcher.fetchAccessToken(
        url,
        "client_id",
        new Password("client_secret"),
        Optional.of(new Password("my_refresh_token")),
        Optional.empty(),
        httpClient);

    assertThat(requestBody(sentRequest())).doesNotContain("scope=");
  }

  @Test
  void jdbcHttpClientSettings_connectorProxyProperties_selectThatProxy() {
    Properties properties = connectorProxy("proxy.example.com", "3128");
    properties.put(JdbcPropertyKeys.NON_PROXY_HOSTS, "*.internal");
    properties.put(JdbcPropertyKeys.PROXY_USER, "user");
    properties.put(JdbcPropertyKeys.PROXY_PASSWORD, "secret");

    HttpClientSettingsKey settings = OAuthAccessTokenFetcher.jdbcHttpClientSettings(properties);

    assertThat(settings.usesProxy()).isTrue();
    assertThat(settings.getProxyHost()).isEqualTo("proxy.example.com");
    assertThat(settings.getProxyPort()).isEqualTo(3128);
    assertThat(settings.getNonProxyHosts()).isEqualTo("*.internal");
    assertThat(settings.getProxyUser()).isEqualTo("user");
    assertThat(settings.getProxyPassword()).isEqualTo("secret");
  }

  @Test
  void jdbcHttpClientSettings_jvmProxyProperties_selectJvmProxyWithCredentials() {
    Map<String, String> jvmProxy = new HashMap<>();
    jvmProxy.put("http.useProxy", "true");
    jvmProxy.put("http.proxyHost", null);
    jvmProxy.put("http.proxyPort", null);
    jvmProxy.put("http.proxyProtocol", null);
    jvmProxy.put("https.proxyHost", "jvm-proxy.example.com");
    jvmProxy.put("https.proxyPort", "8081");
    jvmProxy.put("https.proxyUser", "jvm-user");
    jvmProxy.put("https.proxyPassword", "jvm-secret");

    withSystemProperties(
        jvmProxy,
        () -> {
          HttpClientSettingsKey settings =
              OAuthAccessTokenFetcher.jdbcHttpClientSettings(new Properties());
          assertThat(settings.usesProxy()).isTrue();
          assertThat(settings.getProxyHost()).isEqualTo("jvm-proxy.example.com");
          assertThat(settings.getProxyPort()).isEqualTo(8081);
          assertThat(settings.getProxyUser()).isEqualTo("jvm-user");
          assertThat(settings.getProxyPassword()).isEqualTo("jvm-secret");
        });
  }

  @Test
  void jdbcHttpClientSettings_noProxyProperties_selectNoExplicitProxy() {
    Map<String, String> noJvmProxy = new HashMap<>();
    noJvmProxy.put("http.useProxy", null);

    withSystemProperties(
        noJvmProxy,
        () ->
            assertThat(OAuthAccessTokenFetcher.jdbcHttpClientSettings(new Properties()).usesProxy())
                .isFalse());
  }

  @Test
  void jdbcHttpClientSettings_invalidProxyPort_throwsError1004() {
    assertThatExceptionOfType(SnowflakeKafkaConnectorException.class)
        .isThrownBy(
            () ->
                OAuthAccessTokenFetcher.jdbcHttpClientSettings(
                    connectorProxy("proxy.example.com", "not-a-port")))
        .matches(exception -> exception.getCode().equals("1004"));
  }

  @Test
  void fetchAccessToken_connectorProxyRequiringAuthWithCredentials_returnsTokenThroughProxy(
      @TempDir Path tempDir) throws Exception {
    ChildFetch fetch =
        fetchThroughProxy(
            tempDir, ProxySettings.CONNECTOR, ProxyAuthentication.REQUIRED, ClientCredentials.SENT);
    assertThat(fetch.exitCode()).as("child JVM output: %s", fetch.output()).isZero();
    assertThat(fetch.lastLine()).isEqualTo("proxied-token");
    assertThat(fetch.tunneledConnects())
        .as("the token request must reach the endpoint through the proxy")
        .isPositive();
  }

  @Test
  void fetchAccessToken_connectorProxyRequiringAuthWithoutCredentials_failsProxyAuthentication(
      @TempDir Path tempDir) throws Exception {
    ChildFetch fetch =
        fetchThroughProxy(
            tempDir,
            ProxySettings.CONNECTOR,
            ProxyAuthentication.REQUIRED,
            ClientCredentials.NOT_SENT);
    assertThat(fetch.exitCode()).as("child JVM output: %s", fetch.output()).isNotZero();
    assertThat(fetch.output())
        .as("the proxy must be what refused the request")
        .contains("HTTP 407");
    assertThat(fetch.tunneledConnects()).isZero();
  }

  @Test
  void fetchAccessToken_openConnectorProxyWithoutCredentials_returnsTokenThroughProxy(
      @TempDir Path tempDir) throws Exception {
    ChildFetch fetch =
        fetchThroughProxy(
            tempDir,
            ProxySettings.CONNECTOR,
            ProxyAuthentication.NOT_REQUIRED,
            ClientCredentials.NOT_SENT);
    assertThat(fetch.exitCode()).as("child JVM output: %s", fetch.output()).isZero();
    assertThat(fetch.lastLine()).isEqualTo("proxied-token");
    assertThat(fetch.tunneledConnects())
        .as("the token request must reach the endpoint through the proxy")
        .isPositive();
  }

  @Test
  void fetchAccessToken_connectorProxyBypassingEndpoint_returnsTokenDirectly(@TempDir Path tempDir)
      throws Exception {
    ChildFetch fetch =
        fetchThroughProxy(
            tempDir,
            ProxySettings.CONNECTOR_BYPASSING_ENDPOINT,
            ProxyAuthentication.REQUIRED,
            ClientCredentials.SENT);
    assertThat(fetch.exitCode()).as("child JVM output: %s", fetch.output()).isZero();
    assertThat(fetch.lastLine()).isEqualTo("proxied-token");
    assertThat(fetch.tunneledConnects()).isZero();
  }

  @Test
  void fetchAccessToken_jvmProxyRequiringAuthWithCredentials_returnsTokenThroughProxy(
      @TempDir Path tempDir) throws Exception {
    ChildFetch fetch =
        fetchThroughProxy(
            tempDir, ProxySettings.JVM, ProxyAuthentication.REQUIRED, ClientCredentials.SENT);
    assertThat(fetch.exitCode()).as("child JVM output: %s", fetch.output()).isZero();
    assertThat(fetch.lastLine()).isEqualTo("proxied-token");
    assertThat(fetch.tunneledConnects())
        .as("the token request must reach the endpoint through the proxy")
        .isPositive();
  }

  @Test
  void fetchAccessToken_jvmProxyRequiringAuthWithoutCredentials_failsProxyAuthentication(
      @TempDir Path tempDir) throws Exception {
    ChildFetch fetch =
        fetchThroughProxy(
            tempDir, ProxySettings.JVM, ProxyAuthentication.REQUIRED, ClientCredentials.NOT_SENT);
    assertThat(fetch.exitCode()).as("child JVM output: %s", fetch.output()).isNotZero();
    assertThat(fetch.output())
        .as("the proxy must be what refused the request")
        .contains("HTTP 407");
    assertThat(fetch.tunneledConnects()).isZero();
  }

  @Test
  void fetchAccessToken_openJvmProxyWithoutCredentials_returnsTokenThroughProxy(
      @TempDir Path tempDir) throws Exception {
    ChildFetch fetch =
        fetchThroughProxy(
            tempDir,
            ProxySettings.JVM,
            ProxyAuthentication.NOT_REQUIRED,
            ClientCredentials.NOT_SENT);
    assertThat(fetch.exitCode()).as("child JVM output: %s", fetch.output()).isZero();
    assertThat(fetch.lastLine()).isEqualTo("proxied-token");
    assertThat(fetch.tunneledConnects())
        .as("the token request must reach the endpoint through the proxy")
        .isPositive();
  }

  private void respondWith(CloseableHttpResponse response) throws IOException {
    when(httpClient.execute(any(HttpUriRequest.class))).thenReturn(response);
  }

  private HttpPost sentRequest() throws IOException {
    ArgumentCaptor<HttpUriRequest> captor = ArgumentCaptor.forClass(HttpUriRequest.class);
    verify(httpClient).execute(captor.capture());
    return (HttpPost) captor.getValue();
  }

  private static CloseableHttpResponse response(int statusCode, String body) {
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    when(response.getStatusLine())
        .thenReturn(new BasicStatusLine(HttpVersion.HTTP_1_1, statusCode, null));
    when(response.getEntity()).thenReturn(new StringEntity(body, StandardCharsets.UTF_8));
    return response;
  }

  private static String requestBody(HttpPost request) throws IOException {
    return EntityUtils.toString(request.getEntity(), StandardCharsets.UTF_8);
  }

  private static Properties connectorProxy(String host, String port) {
    Properties properties = new Properties();
    properties.put(JdbcPropertyKeys.USE_PROXY, "true");
    properties.put(JdbcPropertyKeys.PROXY_HOST, host);
    properties.put(JdbcPropertyKeys.PROXY_PORT, port);
    return properties;
  }

  /** Runs {@code body} with the given system properties (null = unset), then restores them. */
  private static void withSystemProperties(Map<String, String> properties, Runnable body) {
    Map<String, String> previous = new HashMap<>();
    properties.keySet().forEach(key -> previous.put(key, System.getProperty(key)));
    try {
      properties.forEach(OAuthAccessTokenFetcherTest::setOrClear);
      body.run();
    } finally {
      previous.forEach(OAuthAccessTokenFetcherTest::setOrClear);
    }
  }

  private static void setOrClear(String key, String value) {
    if (value == null) {
      System.clearProperty(key);
    } else {
      System.setProperty(key, value);
    }
  }

  /** Where the child JVM's token request gets its proxy settings from. */
  private enum ProxySettings {
    /** The connector's proxy, passed as JDBC connection properties. */
    CONNECTOR,
    /** The connector's proxy, with the token endpoint on its bypass list. */
    CONNECTOR_BYPASSING_ENDPOINT,
    /** The JVM's {@code https.proxy*} system properties. */
    JVM
  }

  private enum ProxyAuthentication {
    REQUIRED,
    NOT_REQUIRED
  }

  /** Whether the child JVM is configured with the proxy's credentials. */
  private enum ClientCredentials {
    SENT,
    NOT_SENT
  }

  private static final class ChildFetch {
    private final int exitCode;
    private final String output;
    private final int tunneledConnects;

    ChildFetch(int exitCode, String output, int tunneledConnects) {
      this.exitCode = exitCode;
      this.output = output;
      this.tunneledConnects = tunneledConnects;
    }

    int exitCode() {
      return exitCode;
    }

    String output() {
      return output;
    }

    int tunneledConnects() {
      return tunneledConnects;
    }

    String lastLine() {
      return output.lines().reduce((first, second) -> second).orElse("");
    }
  }

  /**
   * Fetches a token from a local HTTPS endpoint through {@link EmbeddedProxyServer}, which asks for
   * Basic credentials on CONNECT when {@code authentication} is {@link
   * ProxyAuthentication#REQUIRED}. The fetch runs in a fresh child JVM because the trust store for
   * the endpoint's certificate, the JVM proxy properties and the driver's HTTP clients are all
   * process-wide, so the result does not depend on test order.
   */
  private static ChildFetch fetchThroughProxy(
      Path tempDir,
      ProxySettings settings,
      ProxyAuthentication authentication,
      ClientCredentials credentials)
      throws Exception {
    EmbeddedProxyServer proxy =
        authentication == ProxyAuthentication.REQUIRED
            ? new EmbeddedProxyServer(PROXY_USER, PROXY_PASSWORD)
            : new EmbeddedProxyServer(null, null);
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

      String tokenUrl =
          "https://localhost:" + tokenEndpoint.getAddress().getPort() + "/oauth/token-request";
      List<String> command =
          new ArrayList<>(
              List.of(
                  Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                  "-cp",
                  System.getProperty("java.class.path"),
                  "-Djavax.net.ssl.trustStore=" + tempDir.resolve("trust.p12"),
                  "-Djavax.net.ssl.trustStorePassword=changeit"));
      // The self-signed endpoint certificate has no OCSP responder to ask.
      List<String> jdbcProperties = new ArrayList<>(List.of("disableOCSPChecks=true"));
      boolean sendCredentials = credentials == ClientCredentials.SENT;
      switch (settings) {
        case CONNECTOR:
        case CONNECTOR_BYPASSING_ENDPOINT:
          jdbcProperties.add(JdbcPropertyKeys.USE_PROXY + "=true");
          jdbcProperties.add(JdbcPropertyKeys.PROXY_HOST + "=127.0.0.1");
          jdbcProperties.add(JdbcPropertyKeys.PROXY_PORT + "=" + proxy.getPort());
          if (sendCredentials) {
            jdbcProperties.add(JdbcPropertyKeys.PROXY_USER + "=" + PROXY_USER);
            jdbcProperties.add(JdbcPropertyKeys.PROXY_PASSWORD + "=" + PROXY_PASSWORD);
          }
          if (settings == ProxySettings.CONNECTOR_BYPASSING_ENDPOINT) {
            jdbcProperties.add(JdbcPropertyKeys.NON_PROXY_HOSTS + "=localhost");
          }
          break;
        case JVM:
          // The usual worker setup: one plain HTTP proxy for both schemes, credentials in the
          // https.* properties. With only https.proxyHost set the driver would speak TLS to the
          // proxy itself.
          command.add("-Dhttp.useProxy=true");
          command.add("-Dhttp.proxyHost=127.0.0.1");
          command.add("-Dhttp.proxyPort=" + proxy.getPort());
          command.add("-Dhttps.proxyHost=127.0.0.1");
          command.add("-Dhttps.proxyPort=" + proxy.getPort());
          if (sendCredentials) {
            command.add("-Dhttps.proxyUser=" + PROXY_USER);
            command.add("-Dhttps.proxyPassword=" + PROXY_PASSWORD);
          }
          break;
      }
      command.add(FetchMain.class.getName());
      command.add(tokenUrl);
      command.addAll(jdbcProperties);

      Process child = new ProcessBuilder(command).redirectErrorStream(true).start();
      String output = new String(child.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
      return new ChildFetch(child.waitFor(), output, proxy.getTunneledConnectCount());
    } finally {
      if (tokenEndpoint != null) {
        tokenEndpoint.stop(0);
      }
      if (proxy.isRunning()) {
        proxy.stop();
      }
    }
  }

  /**
   * Child JVM entry point: fetches a token from the URL in the first argument, with the JDBC
   * properties given as {@code key=value} in the remaining arguments, and prints it.
   */
  public static final class FetchMain {
    public static void main(String[] args) {
      Properties jdbcProperties = new Properties();
      for (int i = 1; i < args.length; i++) {
        String[] property = args[i].split("=", 2);
        jdbcProperties.put(property[0], property[1]);
      }
      System.out.println(
          OAuthAccessTokenFetcher.fetchAccessToken(
              OAuthURL.from(args[0]),
              "client_id",
              new Password("client_secret"),
              Optional.empty(),
              Optional.empty(),
              jdbcProperties));
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
