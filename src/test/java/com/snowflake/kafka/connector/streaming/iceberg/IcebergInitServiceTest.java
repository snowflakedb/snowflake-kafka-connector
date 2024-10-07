package com.snowflake.kafka.connector.streaming.iceberg;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import org.junit.jupiter.api.Test;

class IcebergInitServiceTest {

  private final SnowflakeConnectionService mockConnection = mock(SnowflakeConnectionService.class);
  private final IcebergInitService icebergInitService = new IcebergInitService(mockConnection);

  @Test
  void testInitializeIcebergTableProperties() {
    icebergInitService.initializeIcebergTableProperties("test_table");

    verify(mockConnection).addMetadataColumnForIcebergIfNotExists("test_table");
    verify(mockConnection).initializeMetadataColumnTypeForIceberg("test_table");
    verifyNoMoreInteractions(mockConnection);
  }
}
