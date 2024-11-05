package com.snowflake.kafka.connector;

import static com.snowflake.kafka.connector.SnowflakeSinkConnectorConfig.*;
import static com.snowflake.kafka.connector.internal.TestUtils.getConfig;

import com.snowflake.kafka.connector.config.IcebergConfigValidator;
import com.snowflake.kafka.connector.internal.EncryptionUtils;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.internal.streaming.DefaultStreamingConfigValidator;
import com.snowflake.kafka.connector.internal.streaming.IngestionMethodConfig;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.security.PrivateKey;
import java.security.Security;
import java.util.Map;
import org.bouncycastle.asn1.nist.NISTObjectIdentifiers;
import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfoBuilder;
import org.bouncycastle.pkcs.jcajce.JcaPKCS8EncryptedPrivateKeyInfoBuilder;
import org.bouncycastle.pkcs.jcajce.JcePKCSPBEOutputEncryptorBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

public class ConnectorConfigValidatorLogsTest {

  private final ConnectorConfigValidator connectorConfigValidator =
      new DefaultConnectorConfigValidator(
          new DefaultStreamingConfigValidator(), new IcebergConfigValidator());

  @Test
  public void testRSAPasswordOutput() throws IOException, OperatorCreationException {
    // given
    String testPasswd = "TestPassword1234!";
    String testKey = generateAESKey(TestUtils.getPrivateKey(), testPasswd.toCharArray());
    Map<String, String> testConf = getConfig();
    testConf.remove(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY);
    testConf.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY, testKey);
    testConf.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, testPasswd);

    // when
    connectorConfigValidator.validateConfig(testConf);

    // then
    EncryptionUtils.parseEncryptedPrivateKey(testKey, testPasswd);
    Assertions.assertFalse(logFileContains(testPasswd));
  }

  @ParameterizedTest
  @ValueSource(strings = {BUFFER_FLUSH_TIME_SEC, BUFFER_COUNT_RECORDS})
  public void shouldLogWarningIfBufferingPropertiesDefinedForSingleBuffer(String parameter)
      throws IOException {
    // given
    Map<String, String> config = TestUtils.getConfForStreaming();
    config.put(INGESTION_METHOD_OPT, IngestionMethodConfig.SNOWPIPE_STREAMING.toString());
    config.put(SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER, "true");
    config.put(parameter, "1000");
    String expectedLog =
        parameter
            + " parameter value is ignored because internal buffer is disabled. To go back to"
            + " previous behaviour set "
            + SNOWPIPE_STREAMING_ENABLE_SINGLE_BUFFER
            + " to false";

    // when
    connectorConfigValidator.validateConfig(config);

    // then
    Assertions.assertTrue(logFileContains(expectedLog));
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

  private String generateAESKey(PrivateKey key, char[] passwd)
      throws IOException, OperatorCreationException {
    Security.addProvider(new BouncyCastleFipsProvider());
    StringWriter writer = new StringWriter();
    JcaPEMWriter pemWriter = new JcaPEMWriter(writer);
    PKCS8EncryptedPrivateKeyInfoBuilder pkcs8EncryptedPrivateKeyInfoBuilder =
        new JcaPKCS8EncryptedPrivateKeyInfoBuilder(key);
    pemWriter.writeObject(
        pkcs8EncryptedPrivateKeyInfoBuilder.build(
            new JcePKCSPBEOutputEncryptorBuilder(NISTObjectIdentifiers.id_aes256_CBC)
                .setProvider("BCFIPS")
                .build(passwd)));
    pemWriter.close();
    return writer.toString();
  }
}
