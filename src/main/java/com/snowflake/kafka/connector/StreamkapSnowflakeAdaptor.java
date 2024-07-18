package com.snowflake.kafka.connector;

import org.apache.kafka.common.config.ConfigDef;

public class StreamkapSnowflakeAdaptor {
    public static ConfigDef newConfigDef() {
        return SnowflakeSinkConnectorConfig.newConfigDef();
    }
}
