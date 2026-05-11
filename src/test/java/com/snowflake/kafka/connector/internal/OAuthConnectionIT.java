package com.snowflake.kafka.connector.internal;

import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Integration test for OAuth authentication. Requires OAuth credentials in the environment:
 *
 * <ul>
 *   <li>{@code SNOWFLAKE_TEST_OAUTH_CLIENT_ID}
 *   <li>{@code SNOWFLAKE_TEST_OAUTH_CLIENT_SECRET}
 *   <li>{@code SNOWFLAKE_TEST_OAUTH_REFRESH_TOKEN} (optional; omit for client_credentials grant)
 *   <li>{@code SNOWFLAKE_TEST_OAUTH_TOKEN_ENDPOINT}
 * </ul>
 *
 * These can be set in {@code .envrc} for local runs or via CI secrets. When credentials are not
 * present, all tests in this class are skipped.
 */
class OAuthConnectionIT {

  private static final String OAUTH_CLIENT_ID = System.getenv("SNOWFLAKE_TEST_OAUTH_CLIENT_ID");
  private static final String OAUTH_CLIENT_SECRET =
      System.getenv("SNOWFLAKE_TEST_OAUTH_CLIENT_SECRET");
  private static final String OAUTH_REFRESH_TOKEN =
      System.getenv("SNOWFLAKE_TEST_OAUTH_REFRESH_TOKEN");
  private static final String OAUTH_TOKEN_ENDPOINT =
      System.getenv("SNOWFLAKE_TEST_OAUTH_TOKEN_ENDPOINT");

  private static boolean oauthCredentialsPresent() {
    return OAUTH_CLIENT_ID != null
        && !OAUTH_CLIENT_ID.isEmpty()
        && OAUTH_CLIENT_SECRET != null
        && !OAUTH_CLIENT_SECRET.isEmpty()
        && OAUTH_TOKEN_ENDPOINT != null
        && !OAUTH_TOKEN_ENDPOINT.isEmpty();
  }

  @Test
  void oauthConnection_shouldConnectAndQuerySnowflake() {
    assumeTrue(oauthCredentialsPresent(), "OAuth test credentials not available, skipping");

    Map<String, String> config = TestUtils.transformProfileFileToConnectorConfiguration(true);
    config.put(
        KafkaConnectorConfigParams.SNOWFLAKE_AUTHENTICATOR,
        KafkaConnectorConfigParams.AUTHENTICATOR_OAUTH);
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_ID, OAUTH_CLIENT_ID);
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_CLIENT_SECRET, OAUTH_CLIENT_SECRET);
    config.put(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_TOKEN_ENDPOINT, OAUTH_TOKEN_ENDPOINT);
    if (OAUTH_REFRESH_TOKEN != null && !OAUTH_REFRESH_TOKEN.isEmpty()) {
      config.put(KafkaConnectorConfigParams.SNOWFLAKE_OAUTH_REFRESH_TOKEN, OAUTH_REFRESH_TOKEN);
    }
    config.remove(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY);
    config.remove(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE);

    SnowflakeConnectionService conn =
        SnowflakeConnectionServiceFactory.builder().setProperties(config).build();
    // databaseExists throws if it does not exist; no exception means success
    conn.databaseExists(config.get(KafkaConnectorConfigParams.SNOWFLAKE_DATABASE_NAME));
    conn.close();
  }
}
