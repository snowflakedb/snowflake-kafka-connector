package com.snowflake.kafka.connector.streaming.iceberg;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import org.junit.jupiter.api.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class IcebergSnowflakeInitServiceTest {

    private final SnowflakeConnectionService mockConnection = mock(SnowflakeConnectionService.class);
    private final IcebergSnowflakeInitService icebergSnowflakeInitService = new IcebergSnowflakeInitService(mockConnection);

    @Test
    void testInitializeIcebergTableProperties() {
        icebergSnowflakeInitService.initializeIcebergTableProperties("test_table");
        verify(mockConnection).initializeMetadataColumnTypeForIceberg("test_table");
        verifyNoMoreInteractions(mockConnection);
    }

}