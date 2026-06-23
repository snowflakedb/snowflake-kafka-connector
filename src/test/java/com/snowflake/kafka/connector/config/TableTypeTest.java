package com.snowflake.kafka.connector.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

public class TableTypeTest {
  @Test
  void fromConfig_parsesAllValuesCaseInsensitively() {
    assertThat(TableType.fromConfig("snowflake")).isEqualTo(TableType.SNOWFLAKE);
    assertThat(TableType.fromConfig("ICEBERG")).isEqualTo(TableType.ICEBERG);
    assertThat(TableType.fromConfig("None")).isEqualTo(TableType.NONE);
  }

  @Test
  void fromConfig_defaultsToSnowflakeOnNullOrEmpty() {
    assertThat(TableType.fromConfig(null)).isEqualTo(TableType.SNOWFLAKE);
    assertThat(TableType.fromConfig("")).isEqualTo(TableType.SNOWFLAKE);
  }

  @Test
  void fromConfig_rejectsUnknownValue() {
    assertThatThrownBy(() -> TableType.fromConfig("delta"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("delta");
  }
}
