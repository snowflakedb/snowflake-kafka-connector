package com.snowflake.kafka.connector.internal;

import static com.snowflake.kafka.connector.internal.SnowflakeConnectionServiceV1.FormattingUtils.formatName;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.snowflake.kafka.connector.internal.streaming.ChannelMigrateOffsetTokenResponseDTO;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

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
    assertThat(migrationDTO.getResponseCode()).isEqualTo(51);
    assertThat(migrationDTO.getResponseMessage())
        .contains("Source Channel does not exist for Offset Migration");
  }

  @Test
  public void testChannelMigrationResponse_InvalidResponse() throws Exception {
    SnowflakeConnectionServiceV1 v1MockConnectionService = mock(SnowflakeConnectionServiceV1.class);
    final String validMigrationResponse =
        "{\"responseCode\": 51, \"responseMessage\": \"Source Channel does not exist for Offset"
            + " Migration\", \"unknown\":62}";
    when(v1MockConnectionService.getChannelMigrateOffsetTokenResponseDTO(anyString()))
        .thenCallRealMethod();
    assertThatThrownBy(
            () ->
                v1MockConnectionService.getChannelMigrateOffsetTokenResponseDTO(
                    validMigrationResponse))
        .isInstanceOf(JsonProcessingException.class);
  }

  @ParameterizedTest
  @CsvSource({"role, ROLE", "Role, ROLE", "\"role\", role", "\"rOle\", rOle"})
  void testFormatNames(String inputName, String expectedName) {
    assertThat(formatName(inputName)).isEqualTo(expectedName);
  }
}
