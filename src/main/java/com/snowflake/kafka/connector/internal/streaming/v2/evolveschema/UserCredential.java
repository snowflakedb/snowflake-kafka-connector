package com.snowflake.kafka.connector.internal.streaming.v2.evolveschema;

import com.snowflake.kafka.connector.config.SinkTaskConfig;
import com.snowflake.kafka.connector.internal.InternalUtils;
import com.snowflake.kafka.connector.internal.PrivateKeyTool;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.URL;
import com.snowflake.kafka.connector.internal.oauth.OAuthAccessTokenFetcher;
import com.snowflake.kafka.connector.internal.oauth.OAuthURL;
import com.snowflake.kafka.connector.internal.spcs.SpcsEnvironment;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.PrivateKey;
import java.time.Instant;
import java.util.function.Supplier;
import org.apache.kafka.common.config.types.Password;

/**
 * The connector's own credential, exchanged at {@code /oauth/token} for a token scoped to the
 * account's streaming host. Mirrors the streaming SDK's token exchange per authenticator.
 */
final class UserCredential {

  static final String TOKEN_TYPE_KEYPAIR_JWT = "KEYPAIR_JWT";
  static final String TOKEN_TYPE_OAUTH = "OAUTH";
  static final String GRANT_TYPE_JWT_BEARER = "urn:ietf:params:oauth:grant-type:jwt-bearer";
  static final String GRANT_TYPE_TOKEN_EXCHANGE = "urn:ietf:params:oauth:grant-type:token-exchange";
  static final String SUBJECT_TOKEN_TYPE_ACCESS_TOKEN =
      "urn:ietf:params:oauth:token-type:access_token";

  private final String tokenType;
  private final String grantType;
  private final String subjectTokenType;
  private final String scopeRole;
  private final Supplier<String> tokenSupplier;

  UserCredential(
      String tokenType,
      String grantType,
      String subjectTokenType,
      String scopeRole,
      Supplier<String> tokenSupplier) {
    this.tokenType = tokenType;
    this.grantType = grantType;
    this.subjectTokenType = subjectTokenType;
    this.scopeRole = scopeRole;
    this.tokenSupplier = tokenSupplier;
  }

  /** Key-pair JWT. The role travels in the JWT's {@code scp} claim. */
  static UserCredential keyPair(String account, String user, String role, PrivateKey privateKey) {
    KeyPairJwt jwt = new KeyPairJwt(account, user, role, privateKey);
    return new UserCredential(
        TOKEN_TYPE_KEYPAIR_JWT, GRANT_TYPE_JWT_BEARER, null, null, () -> jwt.create(Instant.now()));
  }

  /** OAuth access token. The role, if any, is requested in the exchange scope. */
  static UserCredential oauth(String role, Supplier<String> accessToken) {
    return new UserCredential(
        TOKEN_TYPE_OAUTH,
        GRANT_TYPE_TOKEN_EXCHANGE,
        SUBJECT_TOKEN_TYPE_ACCESS_TOKEN,
        role,
        accessToken);
  }

  /** SPCS session token. The service user's role comes from the service, so none is requested. */
  static UserCredential spcs(Supplier<String> sessionToken) {
    return new UserCredential(
        TOKEN_TYPE_OAUTH,
        GRANT_TYPE_TOKEN_EXCHANGE,
        SUBJECT_TOKEN_TYPE_ACCESS_TOKEN,
        null,
        sessionToken);
  }

  static UserCredential from(SinkTaskConfig config, URL accountUrl, String account) {
    switch (config.getAuthenticator()) {
      case SNOWFLAKE_JWT:
        return keyPair(
            account,
            config.getSnowflakeUser(),
            config.getSnowflakeRole(),
            PrivateKeyTool.parsePrivateKey(
                config
                    .getSnowflakePrivateKey()
                    .orElseThrow(SnowflakeErrors.ERROR_0013::getException),
                config.getSnowflakePrivateKeyPassphrase()));
      case OAUTH:
        String clientId =
            config.getOauthClientId().orElseThrow(SnowflakeErrors.ERROR_0026::getException);
        Password clientSecret =
            config.getOauthClientSecret().orElseThrow(SnowflakeErrors.ERROR_0027::getException);
        URL oauthUrl =
            config.getOauthTokenEndpoint().isPresent()
                ? OAuthURL.from(config.getOauthTokenEndpoint().get())
                : accountUrl;
        return oauth(
            config.getSnowflakeRole(),
            () ->
                OAuthAccessTokenFetcher.fetchAccessToken(
                    oauthUrl,
                    clientId,
                    clientSecret,
                    config.getOauthRefreshToken(),
                    InternalUtils.resolveOauthScope(config)));
      case SPCS:
        return spcs(
            () -> {
              try {
                return SpcsEnvironment.readToken();
              } catch (IllegalStateException e) {
                throw SnowflakeErrors.ERROR_1001.getException(e);
              }
            });
      default:
        throw new IllegalStateException("unhandled authenticator: " + config.getAuthenticator());
    }
  }

  /** Value of {@code X-Snowflake-Authorization-Token-Type} for {@link #token()}. */
  String tokenType() {
    return tokenType;
  }

  /** Returns a current user token. May call out to an OAuth server or read the SPCS token file. */
  String token() {
    return tokenSupplier.get();
  }

  /** Form body of the {@code /oauth/token} request that scopes this credential to {@code host}. */
  String scopedTokenRequestBody(String host) {
    String scope = host;
    if (scopeRole != null && !scopeRole.isBlank()) {
      scope += " session:role:" + percentEncode(scopeRole);
    }
    StringBuilder body = new StringBuilder();
    body.append("grant_type=").append(formEncode(grantType));
    body.append("&scope=").append(formEncode(scope));
    if (subjectTokenType != null) {
      body.append("&subject_token_type=").append(formEncode(subjectTokenType));
    }
    return body.toString();
  }

  private static String formEncode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private static String percentEncode(String value) {
    return formEncode(value).replace("+", "%20");
  }
}
