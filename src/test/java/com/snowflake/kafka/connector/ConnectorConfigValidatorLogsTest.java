package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.internal.TestUtils.generateAESKey;
import static com.snowflake.kafka.connector.internal.TestUtils.generatePrivateKey;
import static com.snowflake.kafka.connector.internal.TestUtils.getConfig;

import com.snowflake.kafka.connector.Constants.KafkaConnectorConfigParams;
import com.snowflake.kafka.connector.internal.PrivateKeyTool;
import com.snowflake.kafka.connector.internal.streaming.DefaultStreamingConfigValidator;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.security.PrivateKey;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class ConnectorConfigValidatorLogsTest {

  private final ConnectorConfigValidator connectorConfigValidator =
      new DefaultConnectorConfigValidator(new DefaultStreamingConfigValidator());

  @Test
  public void testRSAPasswordOutput() throws Exception {
    // given
    PrivateKey privateKey = generatePrivateKey();
    String testPasswd = "TestPassword1234!";
    String testKey = generateAESKey(privateKey, testPasswd.toCharArray());
    Map<String, String> testConf = getConfig();
    testConf.remove(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY);
    testConf.put(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY, testKey);
    testConf.put(KafkaConnectorConfigParams.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, testPasswd);
    // when
    connectorConfigValidator.validateConfig(testConf);

    // then
    PrivateKeyTool.parsePrivateKey(testKey, testPasswd);
    Assertions.assertFalse(logFileContains(testPasswd));
  }

  // Note that sf.log accumulates logs between the consecutive test runs
  // That's why it's very hard to test many scenarios without hacks like test ordering and deleting
  // log file
  private boolean logFileContains(String str) throws IOException {
    String fileName = "sf.log";
    File log = new File(fileName);
    FileReader fileReader = new FileReader(log);
    BufferedReader buffer = new BufferedReader(fileReader);
    String line;
    while ((line = buffer.readLine()) != null) {
      if (line.contains(str)) {
        return true;
      }
    }
    buffer.close();
    fileReader.close();
    return false;
  }
}
