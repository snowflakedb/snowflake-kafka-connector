package com.snowflake.kafka.connector.internal.streaming.v2;

import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.Utils;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class StreamingIngestClientV2ProviderTest {

  @Test
  void test_GetClientProperties_includes_role() {
    // Given
    Map<String, String> connectorConfig = new HashMap<>();
    connectorConfig.put(Utils.SF_URL, "https://test.snowflakecomputing.com");
    connectorConfig.put(Utils.SF_PRIVATE_KEY, "test_private_key");
    connectorConfig.put(Utils.SF_USER, "test_user");
    connectorConfig.put(Utils.SF_ROLE, "TEST_ROLE");

    // When
    Properties properties = StreamingIngestClientProvider.getClientProperties(connectorConfig);

    // Then
    assertThat(properties).isNotNull();
    assertThat(properties.getProperty("role")).isEqualTo("TEST_ROLE");
    assertThat(properties.getProperty("user")).isEqualTo("test_user");
    assertThat(properties.getProperty("private_key")).isEqualTo("test_private_key");
  }
}
