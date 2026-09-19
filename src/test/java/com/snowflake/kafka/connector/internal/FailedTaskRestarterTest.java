package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.NAME;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_CONNECT_REST_URL;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_CONNECT_REST_URL_DEFAULT;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_TASK_RESTART_FAILED_INTERVAL_MS;
import static com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams.SNOWFLAKE_TASK_RESTART_FAILED_INTERVAL_MS_DEFAULT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class FailedTaskRestarterTest {

  private HttpClient httpClient;
  private HttpResponse<String> httpResponse;

  @SuppressWarnings("unchecked")
  @BeforeEach
  void setUp() throws Exception {
    httpClient = mock(HttpClient.class);
    httpResponse = mock(HttpResponse.class);
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(httpResponse);
  }

  @Test
  void restartsOnlyFailedTasks() throws Exception {
    when(httpResponse.statusCode()).thenReturn(200);
    when(httpResponse.body())
        .thenReturn(
            "{"
                + "\"name\":\"my-connector\","
                + "\"tasks\":["
                + "{\"id\":0,\"state\":\"RUNNING\"},"
                + "{\"id\":1,\"state\":\"FAILED\"},"
                + "{\"id\":2,\"state\":\"failed\"},"
                + "{\"id\":3,\"state\":\"PAUSED\"}"
                + "]}");

    FailedTaskRestarter restarter =
        new FailedTaskRestarter("my-connector", "http://localhost:8083", httpClient);

    assertThat(restarter.restartFailedTasks()).containsExactly(1, 2);

    ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(httpClient, times(3)).send(captor.capture(), any());
    List<HttpRequest> requests = captor.getAllValues();
    assertThat(requests.get(0).method()).isEqualTo("GET");
    assertThat(requests.get(0).uri().toString())
        .isEqualTo("http://localhost:8083/connectors/my-connector/status");
    assertThat(requests.get(1).method()).isEqualTo("POST");
    assertThat(requests.get(1).uri().toString())
        .isEqualTo("http://localhost:8083/connectors/my-connector/tasks/1/restart");
    assertThat(requests.get(2).uri().toString())
        .isEqualTo("http://localhost:8083/connectors/my-connector/tasks/2/restart");
  }

  @Test
  void doesNotRestartWhenNoTaskHasFailed() throws Exception {
    when(httpResponse.statusCode()).thenReturn(200);
    when(httpResponse.body()).thenReturn("{\"tasks\":[{\"id\":0,\"state\":\"RUNNING\"}]}");

    FailedTaskRestarter restarter =
        new FailedTaskRestarter("my-connector", "http://localhost:8083/", httpClient);

    assertThat(restarter.restartFailedTasks()).isEmpty();
    verify(httpClient, times(1)).send(any(HttpRequest.class), any());
  }

  @Test
  void continuesRestartingAfterOneTaskRestartFails() throws Exception {
    HttpResponse<String> statusResponse = mock(HttpResponse.class);
    HttpResponse<String> firstRestart = mock(HttpResponse.class);
    HttpResponse<String> secondRestart = mock(HttpResponse.class);
    when(statusResponse.statusCode()).thenReturn(200);
    when(statusResponse.body())
        .thenReturn(
            "{\"tasks\":[{\"id\":0,\"state\":\"FAILED\"},{\"id\":1,\"state\":\"FAILED\"}]}");
    when(firstRestart.statusCode()).thenReturn(500);
    when(secondRestart.statusCode()).thenReturn(204);
    when(httpClient.send(any(HttpRequest.class), any(HttpResponse.BodyHandler.class)))
        .thenReturn(statusResponse, firstRestart, secondRestart);

    FailedTaskRestarter restarter =
        new FailedTaskRestarter("my-connector", "http://localhost:8083", httpClient);

    assertThat(restarter.restartFailedTasks()).containsExactly(1);
  }

  @Test
  void statusHttpErrorDoesNotThrow() throws Exception {
    when(httpResponse.statusCode()).thenReturn(404);
    when(httpResponse.body()).thenReturn("not found");

    FailedTaskRestarter restarter =
        new FailedTaskRestarter("my-connector", "http://localhost:8083", httpClient);

    assertThat(restarter.restartFailedTasks()).isEmpty();
  }

  @Test
  void encodesConnectorNameInRestPaths() throws Exception {
    when(httpResponse.statusCode()).thenReturn(200);
    when(httpResponse.body()).thenReturn("{\"tasks\":[{\"id\":0,\"state\":\"FAILED\"}]}");

    FailedTaskRestarter restarter =
        new FailedTaskRestarter("my connector", "http://connect:8083", httpClient);

    restarter.restartFailedTasks();

    ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(httpClient, times(2)).send(captor.capture(), any());
    assertThat(captor.getAllValues().get(0).uri().toString())
        .isEqualTo("http://connect:8083/connectors/my%20connector/status");
    assertThat(captor.getAllValues().get(1).uri().toString())
        .isEqualTo("http://connect:8083/connectors/my%20connector/tasks/0/restart");
  }

  @Test
  void maybeStartReturnsNullWhenIntervalIsZero() {
    Map<String, String> config = new HashMap<>();
    config.put(NAME, "my-connector");
    config.put(SNOWFLAKE_TASK_RESTART_FAILED_INTERVAL_MS, "0");

    assertThat(FailedTaskRestarter.maybeStart(config, httpClient, key -> null)).isNull();
  }

  @Test
  void maybeStartReturnsNullWhenIntervalIsInvalid() {
    Map<String, String> config = new HashMap<>();
    config.put(NAME, "my-connector");
    config.put(SNOWFLAKE_TASK_RESTART_FAILED_INTERVAL_MS, "not-a-number");

    assertThat(FailedTaskRestarter.maybeStart(config, httpClient, key -> null)).isNull();
  }

  @Test
  void parseIntervalDefaultsToOneHour() {
    assertThat(FailedTaskRestarter.parseIntervalMs(Map.of()))
        .isEqualTo(SNOWFLAKE_TASK_RESTART_FAILED_INTERVAL_MS_DEFAULT);
  }

  @Test
  void restUrlPrefersConfigThenAdvertisedEnvThenDefault() {
    Map<String, String> config = new HashMap<>();
    assertThat(FailedTaskRestarter.resolveRestUrl(config, key -> null))
        .isEqualTo(SNOWFLAKE_CONNECT_REST_URL_DEFAULT);

    Map<String, String> env = new HashMap<>();
    env.put("CONNECT_REST_ADVERTISED_URL", "http://worker:8083/");
    assertThat(FailedTaskRestarter.resolveRestUrl(config, env::get))
        .isEqualTo("http://worker:8083");

    config.put(SNOWFLAKE_CONNECT_REST_URL, "http://configured:8083/");
    assertThat(FailedTaskRestarter.resolveRestUrl(config, env::get))
        .isEqualTo("http://configured:8083");
  }

  @Test
  void maybeStartSchedulesThenStops() {
    Map<String, String> config = new HashMap<>();
    config.put(NAME, "my-connector");
    config.put(SNOWFLAKE_TASK_RESTART_FAILED_INTERVAL_MS, "3600000");

    FailedTaskRestarter restarter = FailedTaskRestarter.maybeStart(config, httpClient, key -> null);
    assertThat(restarter).isNotNull();
    restarter.stop();
  }
}
