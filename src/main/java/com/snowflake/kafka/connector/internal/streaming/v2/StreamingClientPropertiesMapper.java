package com.snowflake.kafka.connector.internal.streaming.v2;

import com.snowflake.kafka.connector.Utils;
import java.util.Map;
import java.util.Properties;

/**
 * A class for extracting and mapping connector config into properties accepted by
 * SnowflakeStreamingIngestClient TODO - remove this class with StreamingClientProperties when SSv2
 * is ready
 */
class StreamingClientPropertiesMapper {

  static Properties getClientProperties(Map<String, String> connectorConfig) {
    final Properties props = new Properties();

    props.put("role", Utils.role(connectorConfig));
    props.put("private_key", transformPrivateKey(connectorConfig.get(Utils.SF_PRIVATE_KEY)));
    props.put("user", connectorConfig.get(Utils.SF_USER));

    // TODO - in ssv1 we passed simply url
    props.put("account", "sfctest0");
    props.put("host", "sfctest0.snowflakecomputing.com");

    return props;
  }

  // TODO - temporary workaround, SSv2 does not support the same key format as SSv1
  private static String transformPrivateKey(String privateKey) {
    StringBuilder builder = new StringBuilder();
    builder.append("-----BEGIN PRIVATE KEY-----" + "\n");

    for (int i = 0; i < privateKey.length(); i = i + 64) {
      if (i + 64 >= privateKey.length()) {
        builder.append(privateKey.substring(i));
      } else {
        builder.append(privateKey.substring(i, i + 64));
      }
      builder.append("\n");
    }

    builder.append("-----END PRIVATE KEY-----");
    return builder.toString();
  }
}
