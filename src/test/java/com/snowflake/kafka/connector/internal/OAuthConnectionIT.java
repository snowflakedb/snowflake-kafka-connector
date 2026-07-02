package com.snowflake.kafka.connector.internal;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.config.AuthenticatorType;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Integration test for OAuth authentication. Reads OAuth credentials from {@code profile.json}
 * (fields: {@code oauth_client_id}, {@code oauth_client_secret}, {@code oauth_refresh_token},
 * {@code oauth_token_endpoint}). Skipped when the profile does not contain OAuth credentials.
 */
class OAuthConnectionIT {

  @Test
  void oauthConnection_shouldConnectAndQuerySnowflake() {
    TestUtils.Profile profile = TestUtils.getProfile();

    assumeTrue(
        profile.oauthClientId != null
            && profile.oauthClientSecret != null
            && profile.oauthTokenEndpoint != null,
        "OAuth credentials not present in profile.json, skipping");

    Map<String, String> config = TestUtils.transformProfileFileToConnectorConfiguration(false);
    config.put(
        KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR,
        AuthenticatorType.OAUTH.toConfigValue());
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_ID, profile.oauthClientId);
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_SECRET, profile.oauthClientSecret);
    config.put(
        KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_TOKEN_ENDPOINT, profile.oauthTokenEndpoint);
    if (profile.oauthRefreshToken != null) {
      config.put(
          KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_REFRESH_TOKEN, profile.oauthRefreshToken);
    }
    config.remove(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY);
    config.remove(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE);

    SnowflakeConnectionService conn =
        SnowflakeConnectionServiceFactory.builder().setProperties(config).build();
    try {
      conn.databaseExists(config.get(KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME));
    } finally {
      conn.close();
    }
  }
}
