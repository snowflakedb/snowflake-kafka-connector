package com.snowflake.kafka.connector.internal;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Restarts Kafka Connect tasks that have entered {@code FAILED}.
 *
 * <p>Connect leaves crashed tasks in FAILED until something calls {@code POST
 * /connectors/{name}/tasks/{id}/restart}. This class polls connector status on a timer and issues
 * that restart for every FAILED task.
 */
public final class FailedTaskRestarter {

  private static final KCLogger LOGGER = new KCLogger(FailedTaskRestarter.class.getName());
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final Duration HTTP_TIMEOUT = Duration.ofSeconds(10);
  private static final String FAILED_STATE = "FAILED";

  private final String connectorName;
  private final String restUrl;
  private final long intervalMs;
  private final HttpClient httpClient;
  private final ScheduledExecutorService executor;

  public static FailedTaskRestarter maybeStart(Map<String, String> config) {
    return maybeStart(
        config, HttpClient.newBuilder().connectTimeout(HTTP_TIMEOUT).build(), System::getenv);
  }

  @VisibleForTesting
  static FailedTaskRestarter maybeStart(
      Map<String, String> config, HttpClient httpClient, Function<String, String> env) {
    long intervalMs = parseIntervalMs(config);
    if (intervalMs <= 0) {
      LOGGER.info(
          "Failed-task restarter disabled ({}={})",
          KafkaConnectorConfigParams.SNOWFLAKE_TASK_RESTART_FAILED_INTERVAL_MS,
          intervalMs);
      return null;
    }

    String connectorName = config.get(KafkaConnectorConfigParams.NAME);
    if (connectorName == null || connectorName.isBlank()) {
      LOGGER.warn("Failed-task restarter not started: connector name is missing");
      return null;
    }

    String restUrl = resolveRestUrl(config, env);
    FailedTaskRestarter restarter =
        new FailedTaskRestarter(connectorName, restUrl, intervalMs, httpClient);
    restarter.start();
    return restarter;
  }

  @VisibleForTesting
  FailedTaskRestarter(String connectorName, String restUrl, HttpClient httpClient) {
    this(connectorName, restUrl, 0L, httpClient, null);
  }

  private FailedTaskRestarter(
      String connectorName, String restUrl, long intervalMs, HttpClient httpClient) {
    this(
        connectorName,
        restUrl,
        intervalMs,
        httpClient,
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread thread = new Thread(r, "kc-failed-task-restarter-" + connectorName);
              thread.setDaemon(true);
              return thread;
            }));
  }

  private FailedTaskRestarter(
      String connectorName,
      String restUrl,
      long intervalMs,
      HttpClient httpClient,
      ScheduledExecutorService executor) {
    this.connectorName = connectorName;
    this.restUrl = stripTrailingSlash(restUrl);
    this.intervalMs = intervalMs;
    this.httpClient = httpClient;
    this.executor = executor;
  }

  private void start() {
    executor.scheduleAtFixedRate(
        this::restartFailedTasks, intervalMs, intervalMs, TimeUnit.MILLISECONDS);
    LOGGER.info(
        "Restarting FAILED tasks for connector {} every {} ms via {}",
        connectorName,
        intervalMs,
        restUrl);
  }

  public void stop() {
    if (executor == null || executor.isShutdown()) {
      return;
    }
    LOGGER.info("Stopping failed-task restarter for connector {}", connectorName);
    executor.shutdown();
    try {
      if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  @VisibleForTesting
  List<Integer> restartFailedTasks() {
    List<Integer> restarted = new ArrayList<>();
    try {
      for (int taskId : listFailedTaskIds()) {
        if (restartTask(taskId)) {
          restarted.add(taskId);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn(
          "Interrupted while restarting FAILED tasks for connector {}: {}",
          connectorName,
          e.getMessage());
    } catch (Exception e) {
      LOGGER.warn(
          "Could not restart FAILED tasks for connector {} via {}: {}",
          connectorName,
          restUrl,
          e.getMessage());
    }
    return restarted;
  }

  private List<Integer> listFailedTaskIds() throws IOException, InterruptedException {
    HttpRequest request =
        HttpRequest.newBuilder(statusUri())
            .timeout(HTTP_TIMEOUT)
            .header("Accept", "application/json")
            .GET()
            .build();
    HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() / 100 != 2) {
      throw new IOException("GET " + statusUri() + " returned HTTP " + response.statusCode());
    }

    JsonNode tasks = OBJECT_MAPPER.readTree(response.body()).path("tasks");
    List<Integer> failed = new ArrayList<>();
    if (!tasks.isArray()) {
      return failed;
    }
    for (JsonNode task : tasks) {
      if (FAILED_STATE.equalsIgnoreCase(task.path("state").asText())) {
        failed.add(task.path("id").asInt());
      }
    }
    if (!failed.isEmpty()) {
      LOGGER.info(
          "Connector {} has FAILED tasks {}; requesting Connect restart", connectorName, failed);
    }
    return failed;
  }

  private boolean restartTask(int taskId) {
    URI uri = restartUri(taskId);
    try {
      HttpRequest request =
          HttpRequest.newBuilder(uri)
              .timeout(HTTP_TIMEOUT)
              .POST(HttpRequest.BodyPublishers.noBody())
              .build();
      HttpResponse<String> response =
          httpClient.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() / 100 != 2) {
        LOGGER.warn(
            "POST {} returned HTTP {} when restarting task {}", uri, response.statusCode(), taskId);
        return false;
      }
      LOGGER.info("Requested restart of FAILED task {} for connector {}", taskId, connectorName);
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Interrupted while restarting task {} for connector {}", taskId, connectorName);
      return false;
    } catch (Exception e) {
      LOGGER.warn(
          "Could not restart task {} for connector {} via {}: {}",
          taskId,
          connectorName,
          uri,
          e.getMessage());
      return false;
    }
  }

  private URI statusUri() {
    return URI.create(restUrl + "/connectors/" + encodePath(connectorName) + "/status");
  }

  private URI restartUri(int taskId) {
    return URI.create(
        restUrl + "/connectors/" + encodePath(connectorName) + "/tasks/" + taskId + "/restart");
  }

  static String resolveRestUrl(Map<String, String> config, Function<String, String> env) {
    String fromConfig = config.get(KafkaConnectorConfigParams.SNOWFLAKE_CONNECT_REST_URL);
    if (fromConfig != null && !fromConfig.isBlank()) {
      return stripTrailingSlash(fromConfig.trim());
    }
    String advertised = env.apply("CONNECT_REST_ADVERTISED_URL");
    if (advertised != null && !advertised.isBlank()) {
      return stripTrailingSlash(advertised.trim());
    }
    String connectUrl = env.apply("CONNECT_URL");
    if (connectUrl != null && !connectUrl.isBlank()) {
      return stripTrailingSlash(connectUrl.trim());
    }
    return KafkaConnectorConfigParams.SNOWFLAKE_CONNECT_REST_URL_DEFAULT;
  }

  static long parseIntervalMs(Map<String, String> config) {
    String raw = config.get(KafkaConnectorConfigParams.SNOWFLAKE_TASK_RESTART_FAILED_INTERVAL_MS);
    if (raw == null || raw.isBlank()) {
      return KafkaConnectorConfigParams.SNOWFLAKE_TASK_RESTART_FAILED_INTERVAL_MS_DEFAULT;
    }
    try {
      return Long.parseLong(raw.trim());
    } catch (NumberFormatException e) {
      LOGGER.error(
          "Invalid {}: {}; failed-task restarter disabled",
          KafkaConnectorConfigParams.SNOWFLAKE_TASK_RESTART_FAILED_INTERVAL_MS,
          raw);
      return 0L;
    }
  }

  private static String encodePath(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
  }

  private static String stripTrailingSlash(String url) {
    if (url.endsWith("/")) {
      return url.substring(0, url.length() - 1);
    }
    return url;
  }
}
