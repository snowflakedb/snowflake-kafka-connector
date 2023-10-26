package com.snowflake.kafka.connector.internal.streaming;

import static net.snowflake.ingest.streaming.internal.StreamingIngestResponseCode.ERR_CHANNEL_DOES_NOT_EXIST_OR_IS_NOT_AUTHORIZED;
import static net.snowflake.ingest.streaming.internal.StreamingIngestResponseCode.ERR_CHANNEL_HAS_INVALID_CLIENT_SEQUENCER;
import static net.snowflake.ingest.streaming.internal.StreamingIngestResponseCode.SUCCESS;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.InvalidKeySpecException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import net.snowflake.client.core.SFSessionProperty;
import net.snowflake.client.jdbc.internal.apache.http.HttpEntity;
import net.snowflake.client.jdbc.internal.apache.http.HttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.HttpStatus;
import net.snowflake.client.jdbc.internal.apache.http.StatusLine;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.CloseableHttpResponse;
import net.snowflake.client.jdbc.internal.apache.http.client.methods.HttpUriRequest;
import net.snowflake.client.jdbc.internal.apache.http.impl.client.CloseableHttpClient;
import net.snowflake.client.jdbc.internal.apache.http.util.EntityUtils;
import net.snowflake.ingest.connection.OAuthCredential;
import net.snowflake.ingest.connection.RequestBuilder;
import net.snowflake.ingest.connection.ServiceResponseHandler;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.utils.BackOffException;
import net.snowflake.ingest.utils.Constants;
import net.snowflake.ingest.utils.ErrorCode;
import net.snowflake.ingest.utils.HttpUtil;
import net.snowflake.ingest.utils.SFException;
import net.snowflake.ingest.utils.SnowflakeURL;

/**
 * Class which handles REST API call directly for a Streaming channel and obtains a response of its
 * existence.
 *
 * <p>This is a bit different from what we are doing in {@link
 * SnowflakeStreamingIngestChannel#getLatestCommittedOffsetToken()} wherein we first have to Open A
 * Channel and then make another call for getOffsetToken.
 *
 * <p>A method (checkChannelExistence) in this class bypasses that open Channel call and directly
 * creates the payload for Get Status {@link
 * StreamingChannelExistenceChecker#CHANNEL_STATUS_ENDPOINT} API.
 */
public class StreamingChannelExistenceChecker {
  private static final KCLogger LOGGER =
      new KCLogger(StreamingChannelExistenceChecker.class.getName());

  private static final String DEBUG_PREFIX = "Channel Existence Checker - Kafka Connector";

  private static final String CHANNEL_STATUS_ENDPOINT = "/v1/streaming/channels/status/";

  // This is just a placeholder String, it has no value since we are not making a call through a
  // Client.
  private static final String STREAMING_CUSTOM_REST_API_CLIENT = "KC_CHANNEL_EXISTENCE_CHECKER";
  private final String tableName;
  private final Map<String, String> connectorConfig;

  // Using SnowflakeURL from IngestSDK
  private final SnowflakeURL snowflakeURL;

  // We always set clientSequencer to 0.
  private final Long clientSequencer;

  @VisibleForTesting
  StreamingChannelExistenceChecker(
      String tableName, Map<String, String> connectorConfig, final Long clientSequencer) {
    this.tableName = tableName;
    this.connectorConfig = connectorConfig;
    this.snowflakeURL = new SnowflakeURL(this.connectorConfig.get(Utils.SF_URL));
    this.clientSequencer = clientSequencer;
  }

  /**
   * C'tor for StreamingChannelExistenceChecker
   *
   * @param tableName TableName for checking Channel Name in it.
   * @param connectorConfig connectorConfig passed in SF Kafka Connect Configuration
   */
  public StreamingChannelExistenceChecker(String tableName, Map<String, String> connectorConfig) {
    this(tableName, connectorConfig, 0L);
  }

  /**
   * Determines if a Channel Exists or not for a table defined in this class's ctor.
   *
   * @param channelName Name of Channel to Check for Existence
   * @return True if it exists, false if it doesn't.
   */
  public boolean checkChannelExistence(final String channelName) {
    // Re-using client from IngestSDK since it handles lots of proxy props.
    // Please don't close this client.
    CloseableHttpClient httpClient = HttpUtil.getHttpClient(snowflakeURL.getAccount());
    Object credential = generateCredentialObjectForStreamingRestApi();
    RequestBuilder requestBuilder =
        new RequestBuilder(
            snowflakeURL,
            connectorConfig.get(Utils.SF_USER),
            credential,
            httpClient,
            /* This String is used for Telemetry */
            String.format(
                "%s_%s_%s",
                STREAMING_CUSTOM_REST_API_CLIENT, channelName, System.currentTimeMillis()));

    ChannelExistenceCheckerRequest channelExistenceCheckerRequest =
        new ChannelExistenceCheckerRequest();
    ChannelExistenceCheckerRequest.ChannelStatusRequestDTO requestDTO =
        new ChannelExistenceCheckerRequest.ChannelStatusRequestDTO(
            this.connectorConfig.get(Utils.SF_DATABASE),
            this.connectorConfig.get(Utils.SF_SCHEMA),
            tableName,
            channelName,
            this.clientSequencer);
    channelExistenceCheckerRequest.setChannels(Collections.singletonList(requestDTO));
    channelExistenceCheckerRequest.setRole(connectorConfig.get(Utils.SF_ROLE));
    channelExistenceCheckerRequest.setRequestId(UUID.randomUUID().toString());

    try {
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      String requestPayload = objectMapper.writeValueAsString(channelExistenceCheckerRequest);

      HttpUriRequest request =
          requestBuilder.generateStreamingIngestPostRequest(
              requestPayload,
              CHANNEL_STATUS_ENDPOINT,
              DEBUG_PREFIX /* Message passed on to Ingest SDK */);

      ChannelExistenceCheckerResponse channelExistenceCheckerResponse;
      try (CloseableHttpResponse closeableHttpResponse = httpClient.execute(request)) {

        if (closeableHttpResponse == null) {
          LOGGER.warn(
              "Null response obtained in {} for channel:{}", CHANNEL_STATUS_ENDPOINT, channelName);
          throw new SFException(ErrorCode.CHANNEL_STATUS_FAILURE);
        }

        // Handle the exceptional status code
        HttpResponse httpResponse =
            handleExceptionalStatus(
                closeableHttpResponse,
                null,
                ServiceResponseHandler.ApiName.STREAMING_CHANNEL_STATUS,
                httpClient,
                request,
                requestBuilder);

        // Grab the string version of the response entity
        String blob = consumeAndReturnResponseEntityAsString(httpResponse.getEntity());

        // Read out our blob into a pojo
        channelExistenceCheckerResponse =
            objectMapper.readValue(blob, ChannelExistenceCheckerResponse.class);
      }

      if (channelExistenceCheckerResponse.getStatusCode() == SUCCESS.getStatusCode()) {
        assert channelExistenceCheckerResponse.getChannels().size() == 1;
        if (channelExistenceCheckerResponse
            .getChannels()
            .get(0)
            .getStatusCode()
            .equals(ERR_CHANNEL_DOES_NOT_EXIST_OR_IS_NOT_AUTHORIZED.getStatusCode())) {
          LOGGER.debug(String.format("%s Channel:%s doesn't exist", DEBUG_PREFIX, channelName));
          return false;
        } else if (channelExistenceCheckerResponse
                .getChannels()
                .get(0)
                .getStatusCode()
                .equals(ERR_CHANNEL_HAS_INVALID_CLIENT_SEQUENCER.getStatusCode())
            || channelExistenceCheckerResponse
                .getChannels()
                .get(0)
                .getStatusCode()
                .equals(SUCCESS.getStatusCode())) {
          // We check two status codes, one for invalid client sequencers and one for a valid client
          // Sequencer
          LOGGER.debug(String.format("%s Channel:%s does exist", DEBUG_PREFIX, channelName));
          return true;
        }
      }
    } catch (Exception e) {
      LOGGER.error(
          "{} Error Getting a valid response for ChannelName:{} error:{}",
          DEBUG_PREFIX,
          channelName,
          e.getMessage());
    }
    return false;
  }

  /** This implementation is copied over from Ingest SDK. */
  private static HttpResponse handleExceptionalStatus(
      HttpResponse response,
      UUID requestId,
      ServiceResponseHandler.ApiName apiName,
      CloseableHttpClient httpClient,
      HttpUriRequest request,
      RequestBuilder requestBuilder)
      throws IOException, BackOffException {
    if (!isStatusOK(response.getStatusLine())) {
      StatusLine statusLine = response.getStatusLine();
      LOGGER.warn(
          "{} Status hit from {}, requestId:{}",
          statusLine.getStatusCode(),
          apiName,
          requestId == null ? "" : requestId.toString());

      // if we have a 503 exception throw a backoff
      switch (statusLine.getStatusCode()) {
          // If we have a 503, BACKOFF
        case HttpStatus.SC_SERVICE_UNAVAILABLE:
          throw new BackOffException();
        case HttpStatus.SC_UNAUTHORIZED:
          LOGGER.warn("Authorization failed, refreshing Token succeeded, retry");
          requestBuilder.refreshToken();
          requestBuilder.addToken(request);
          response = httpClient.execute(request);
          if (!isStatusOK(response.getStatusLine())) {
            throw new SecurityException("Authorization failed after retry");
          }
          break;
        default:
          String blob = consumeAndReturnResponseEntityAsString(response.getEntity());
          final String errMsg =
              String.format("Unknown Exception for:%s with body:%s", CHANNEL_STATUS_ENDPOINT, blob);
          LOGGER.warn(errMsg);
          throw new SFException(ErrorCode.CHANNEL_STATUS_FAILURE, errMsg);
      }
    }
    return response;
  }

  /** This implementation is copied over from Ingest SDK. */
  private static boolean isStatusOK(StatusLine statusLine) {
    // If the status is 200 (OK) or greater but less than 300 (Multiple Choices) we're good
    return statusLine.getStatusCode() >= HttpStatus.SC_OK
        && statusLine.getStatusCode() < HttpStatus.SC_MULTIPLE_CHOICES;
  }

  /** This implementation is copied over from Ingest SDK. */
  private static String consumeAndReturnResponseEntityAsString(HttpEntity httpResponseEntity)
      throws IOException {
    // grab the string version of the response entity
    String responseEntityAsString = EntityUtils.toString(httpResponseEntity);

    EntityUtils.consumeQuietly(httpResponseEntity);
    return responseEntityAsString;
  }

  /** Handles JWT and Oauth token Both! */
  private Object generateCredentialObjectForStreamingRestApi() {
    Properties streamingClientProps = new Properties();
    // These are the properties which Ingest SDK expected when it creates Streaming Client
    streamingClientProps.putAll(
        StreamingUtils.convertConfigForStreamingClient(new HashMap<>(this.connectorConfig)));
    Properties convertedPropertiesFromIngestSDK =
        net.snowflake.ingest.utils.Utils.createProperties(streamingClientProps);
    Object credential;

    if (convertedPropertiesFromIngestSDK
        .getProperty(Constants.AUTHORIZATION_TYPE)
        .equals(Constants.JWT)) {
      try {
        credential =
            net.snowflake.ingest.utils.Utils.createKeyPairFromPrivateKey(
                (PrivateKey)
                    convertedPropertiesFromIngestSDK.get(
                        SFSessionProperty.PRIVATE_KEY.getPropertyKey()));
      } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
        throw new SFException(e, ErrorCode.KEYPAIR_CREATION_FAILURE);
      }
    } else {
      credential =
          new OAuthCredential(
              convertedPropertiesFromIngestSDK.getProperty(Constants.OAUTH_CLIENT_ID),
              convertedPropertiesFromIngestSDK.getProperty(Constants.OAUTH_CLIENT_SECRET),
              convertedPropertiesFromIngestSDK.getProperty(Constants.OAUTH_REFRESH_TOKEN));
    }
    return credential;
  }
}
