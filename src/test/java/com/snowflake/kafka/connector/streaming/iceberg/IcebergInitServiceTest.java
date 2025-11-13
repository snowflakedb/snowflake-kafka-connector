package com.snowflake.kafka.connector.streaming.iceberg;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IcebergInitServiceTest {

  private final SnowflakeConnectionService mockConnection = mock(SnowflakeConnectionService.class);
  private final IcebergInitService icebergInitService = new IcebergInitService(mockConnection);

  @Test
  void testInitializeIcebergTableProperties() {
    Map<String, String> config = new HashMap<>();
    icebergInitService.initializeIcebergTableProperties("test_table", config);

    verify(mockConnection).addMetadataColumnForIcebergIfNotExists("test_table", config);
    verify(mockConnection).initializeMetadataColumnTypeForIceberg("test_table", config);
    verifyNoMoreInteractions(mockConnection);
  }
}
