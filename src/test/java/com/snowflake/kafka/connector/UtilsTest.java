package com.snowflake.kafka.connector;

import org.junit.Test;

public class UtilsTest
{
  @Test
  public void testObjectIdentifier()
  {
    String name = "DATABASE.SCHEMA.TABLE";
    assert Utils.isValidSnowflakeObjectIdentifier(name);
    String name1 = "table!@#$%^;()";
    assert !Utils.isValidSnowflakeObjectIdentifier(name1);
  }

  @Test
  public void testVersionChecker()
  {
    Utils.checkConnectorVersion();
  }

}
