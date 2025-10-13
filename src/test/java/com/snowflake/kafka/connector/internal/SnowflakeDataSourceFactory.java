package com.snowflake.kafka.connector.internal;

import com.snowflake.kafka.connector.Utils;
import java.security.PrivateKey;
import java.util.Map;
import java.util.Properties;
import javax.sql.DataSource;
import net.snowflake.client.jdbc.SnowflakeDriver;
import org.apache.commons.dbcp2.ConnectionFactory;
import org.apache.commons.dbcp2.DriverConnectionFactory;
import org.apache.commons.dbcp2.PoolableConnection;
import org.apache.commons.dbcp2.PoolableConnectionFactory;
import org.apache.commons.dbcp2.PoolingDataSource;
import org.apache.commons.pool2.impl.GenericObjectPool;

/** Factory class for creating DataSource instances using Apache Commons DBCP2 for testing. */
public final class SnowflakeDataSourceFactory {

  private static DataSource dataSource;

  private SnowflakeDataSourceFactory() {}

  public static DataSource get() {
    if (dataSource != null) {
      return dataSource;
    } else {
      try {
        final Map<String, String> conf = TestUtils.getConfForStreaming();
        final SnowflakeURL url = new SnowflakeURL(conf.get(Utils.SF_URL));

        // Extract properties from conf Map
        final String user = conf.get(Utils.SF_USER);
        final String privateKeyStr = conf.get(Utils.SF_PRIVATE_KEY);
        final String privateKeyPassphrase = conf.get(Utils.PRIVATE_KEY_PASSPHRASE);
        final String database = conf.get(Utils.SF_DATABASE);
        final String schema = conf.get(Utils.SF_SCHEMA);
        final String warehouse = conf.get(Utils.SF_WAREHOUSE);
        String authenticator = conf.get(Utils.SF_AUTHENTICATOR);

        // Assert all required properties are present
        assert user != null : "User must not be null";
        assert privateKeyStr != null : "Private key must not be null";
        assert url != null : "Snowflake URL must not be null";
        assert database != null : "Database must not be null";
        assert schema != null : "Schema must not be null";
        assert warehouse != null : "Warehouse must not be null";

        // Set authenticator (default to JWT if not specified)
        if (authenticator == null || authenticator.isEmpty()) {
          authenticator = Utils.SNOWFLAKE_JWT;
        }

        // Only support JWT authentication for now (standard for tests)
        if (!Utils.SNOWFLAKE_JWT.equals(authenticator)) {
          throw new UnsupportedOperationException(
              "SnowflakeDataSourceFactory currently only supports JWT authentication. "
                  + "Found authenticator: "
                  + authenticator);
        }

        // Build connection properties
        final Properties connectionProperties = new Properties();
        connectionProperties.setProperty("authenticator", authenticator);
        connectionProperties.setProperty("user", user);
        connectionProperties.setProperty("db", database);
        connectionProperties.setProperty("schema", schema);
        connectionProperties.setProperty("warehouse", warehouse);

        // JWT key pair auth - set private key
        final PrivateKey privateKey;
        if (privateKeyPassphrase != null && !privateKeyPassphrase.isEmpty()) {
          privateKey =
              EncryptionUtils.parseEncryptedPrivateKey(privateKeyStr, privateKeyPassphrase);
        } else {
          privateKey = InternalUtils.parsePrivateKey(privateKeyStr);
        }
        connectionProperties.put("privateKey", privateKey);

        // Create connection factory with Snowflake driver
        final SnowflakeDriver driver = new SnowflakeDriver();
        final ConnectionFactory connectionFactory =
            new DriverConnectionFactory(driver, url.getJdbcUrl(), connectionProperties);

        // Create poolable connection factory
        final PoolableConnectionFactory poolableConnectionFactory =
            new PoolableConnectionFactory(connectionFactory, null);

        // Create the pool with 1 initial connection
        final GenericObjectPool<PoolableConnection> connectionPool =
            new GenericObjectPool<>(poolableConnectionFactory);
        connectionPool.setMaxTotal(10);
        connectionPool.setMaxIdle(1);
        connectionPool.setMinIdle(1);

        poolableConnectionFactory.setPool(connectionPool);
        dataSource = new PoolingDataSource<>(connectionPool);
        return dataSource;

      } catch (final Exception e) {
        throw new RuntimeException("Failed to create DataSource", e);
      }
    }
  }
}
