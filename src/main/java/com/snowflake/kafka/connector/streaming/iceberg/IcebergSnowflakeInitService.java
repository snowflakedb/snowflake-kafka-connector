package com.snowflake.kafka.connector.streaming.iceberg;

import com.snowflake.kafka.connector.internal.KCLogger;
import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;

public class IcebergSnowflakeInitService {

    private final KCLogger LOGGER = new KCLogger(IcebergSnowflakeInitService.class.getName());

    private final SnowflakeConnectionService snowflakeConnectionService;

    public IcebergSnowflakeInitService(SnowflakeConnectionService snowflakeConnectionService) {
        this.snowflakeConnectionService = snowflakeConnectionService;
    }

    public void initializeIcebergTableProperties(String tableName) {
        LOGGER.info("Initializing properties for Iceberg table: {}", tableName);
        snowflakeConnectionService.initializeMetadataColumnTypeForIceberg(tableName);
    }
}
