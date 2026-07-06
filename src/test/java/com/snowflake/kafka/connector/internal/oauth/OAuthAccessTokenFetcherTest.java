package com.snowflake.kafka.connector.internal.oauth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import org.apache.kafka.common.config.types.Password;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
}
