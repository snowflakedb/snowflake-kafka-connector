package com.snowflake.kafka.connector;

import com.snowflake.kafka.connector.internal.EncryptionUtils;
import com.snowflake.kafka.connector.internal.FIPSTest;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.records.SnowflakeAvroConverter;
import org.bouncycastle.operator.OperatorCreationException;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

public class SecurityTest
{

  @Test
  public void testRSAPasswordOutput() throws IOException,
    OperatorCreationException
  {
    String testPasswd = "TestPassword1234!";
    String testKey =
      FIPSTest.generateAESKey(TestUtils.getPrivateKey(),
        testPasswd.toCharArray());
    Map<String, String> testConf = ConnectorConfigTest.getConfig();
    testConf.remove(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY);
    testConf.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY, testKey);
    testConf.put(SnowflakeSinkConnectorConfig.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE, testPasswd);
    Utils.validateConfig(testConf);
    EncryptionUtils.parseEncryptedPrivateKey(testKey, testPasswd);
    assert ! searchInLogFile(testPasswd);
  }

  static boolean searchInLogFile(String str) throws IOException
  {
    String fileName = "sf.log";
    File log = new File(fileName);
    FileReader fileReader = new FileReader(log);
    BufferedReader buffer = new BufferedReader(fileReader);
    String line;
    while ((line = buffer.readLine()) != null)
    {
      if (line.contains(str))
      {
        return true;
      }
    }
    buffer.close();
    fileReader.close();
    return false;
  }
}
