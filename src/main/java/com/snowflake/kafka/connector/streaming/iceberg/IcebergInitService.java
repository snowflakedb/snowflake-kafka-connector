package com.snowflake.kafka.connector.streaming.iceberg;

import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;

public class IcebergInitService {

  private final KCLogger LOGGER = new KCLogger(IcebergInitService.class.getName());

  private final SnowflakeConnectionService snowflakeConnectionService;

  public IcebergInitService(SnowflakeConnectionService snowflakeConnectionService) {
    this.snowflakeConnectionService = snowflakeConnectionService;
  }

  public void initializeIcebergTableProperties(String tableName) {
    LOGGER.info("Initializing properties for Iceberg table: {}", tableName);
    snowflakeConnectionService.addMetadataColumnForIcebergIfNotExists(tableName);
    snowflakeConnectionService.initializeMetadataColumnTypeForIceberg(tableName);
  }
}
