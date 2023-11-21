package com.snowflake.kafka.connector.internal;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.snowflake.kafka.connector.internal.streaming.ChannelMigrateOffsetTokenResponseDTO;
import org.junit.Test;

public class SnowflakeConnectionServiceV1Test {
  @Test
  public void testChannelMigrationResponse_validResponse() throws Exception {
    SnowflakeConnectionServiceV1 v1MockConnectionService = mock(SnowflakeConnectionServiceV1.class);
    final String validMigrationResponse =
        "{\"responseCode\": 51, \"responseMessage\": \"Source Channel does not exist for Offset"
            + " Migration\"}";
    when(v1MockConnectionService.getChannelMigrateOffsetTokenResponseDTO(anyString()))
        .thenCallRealMethod();

    ChannelMigrateOffsetTokenResponseDTO migrationDTO =
        v1MockConnectionService.getChannelMigrateOffsetTokenResponseDTO(validMigrationResponse);
    assert migrationDTO.getResponseCode() == 51;
    assert migrationDTO
        .getResponseMessage()
        .contains("Source Channel does not exist for Offset Migration");
  }

  @Test(expected = JsonProcessingException.class)
  public void testChannelMigrationResponse_InvalidResponse() throws Exception {
    SnowflakeConnectionServiceV1 v1MockConnectionService = mock(SnowflakeConnectionServiceV1.class);
    final String validMigrationResponse =
        "{\"responseCode\": 51, \"responseMessage\": \"Source Channel does not exist for Offset"
            + " Migration\", \"unknown\":62}";
    when(v1MockConnectionService.getChannelMigrateOffsetTokenResponseDTO(anyString()))
        .thenCallRealMethod();
    v1MockConnectionService.getChannelMigrateOffsetTokenResponseDTO(validMigrationResponse);
  }
}
