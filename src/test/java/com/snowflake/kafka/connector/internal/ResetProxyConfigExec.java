package com.snowflake.kafka.connector.internal;

import net.snowflake.client.jdbc.SnowflakeSQLException;

public class ResetProxyConfigExec {
  public static void main(String[] args) throws SnowflakeSQLException {
    System.out.println("ResetProxyConfigExec::Start wiping Proxy config");
    TestUtils.resetProxyParametersInJVM();
    System.out.println("ResetProxyConfigExec::Proxy Parameters reset in JVM in JDBC");
  }
}
