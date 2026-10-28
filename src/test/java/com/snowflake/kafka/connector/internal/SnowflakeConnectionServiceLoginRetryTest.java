package com.snowflake.kafka.connector.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig;
import com.snowflake.kafka.connector.Utils;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyPairGenerator;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Verifies that a key-pair login against a Snowflake endpoint that keeps returning HTTP 503 fails
 * once the login timeout passes, instead of retrying indefinitely and holding a pooled HTTP
 * connection for every retry.
 */
public class SnowflakeConnectionServiceLoginRetryTest {
  private static final int LOGIN_TIMEOUT_SECONDS = 5;
  private static final long DEADLINE_SECONDS = 60;

  private final AtomicInteger loginRequests = new AtomicInteger();
  private HttpServer server;
  private ExecutorService executor;

  @BeforeEach
  public void setUp() throws IOException {
    server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
    server.createContext(
        "/",
        exchange -> {
          try (InputStream in = exchange.getRequestBody()) {
            while (in.read() != -1) {}
          }
          if (exchange.getRequestURI().getPath().contains("login-request")) {
            loginRequests.incrementAndGet();
          }
          byte[] body = "Service Unavailable".getBytes(StandardCharsets.UTF_8);
          exchange.sendResponseHeaders(503, body.length);
          try (OutputStream out = exchange.getResponseBody()) {
            out.write(body);
          }
        });
    server.start();
    executor = Executors.newSingleThreadExecutor();
  }

  @AfterEach
  public void tearDown() {
    executor.shutdownNow();
    server.stop(0);
  }

  @Test
  public void testKeyPairLoginGivesUpWhenSnowflakeReturns503() throws Exception {
    Future<?> login =
        executor.submit(
            () -> SnowflakeConnectionServiceFactory.builder().setProperties(config()).build());

    assertThatThrownBy(() -> login.get(DEADLINE_SECONDS, TimeUnit.SECONDS))
        .hasCauseInstanceOf(SnowflakeKafkaConnectorException.class);
    assertThat(loginRequests.get()).isPositive();
  }

  private Map<String, String> config() throws Exception {
    KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
    generator.initialize(2048);
    String privateKey =
        Base64.getEncoder().encodeToString(generator.generateKeyPair().getPrivate().getEncoded());

    Map<String, String> config = new HashMap<>();
    config.put(Utils.NAME, "login_retry_test");
    config.put(Utils.SF_URL, "http://127.0.0.1:" + server.getAddress().getPort());
    config.put(Utils.SF_USER, "user");
    config.put(Utils.SF_PRIVATE_KEY, privateKey);
    config.put(Utils.SF_DATABASE, "database");
    config.put(Utils.SF_SCHEMA, "schema");
    config.put(
        SnowflakeSinkConnectorConfig.SNOWFLAKE_JDBC_MAP, "loginTimeout:" + LOGIN_TIMEOUT_SECONDS);
    return config;
  }
}
