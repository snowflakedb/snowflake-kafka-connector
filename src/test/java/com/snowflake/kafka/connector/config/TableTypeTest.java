package com.snowflake.kafka.connector.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Optional;
import org.junit.jupiter.api.Test;

public class TableTypeTest {
  @Test
  void fromConfig_parsesAllValuesCaseInsensitively() {
    assertThat(TableType.fromConfig("snowflake")).isEqualTo(Optional.of(TableType.SNOWFLAKE));
    assertThat(TableType.fromConfig("ICEBERG")).isEqualTo(Optional.of(TableType.ICEBERG));
    assertThat(TableType.fromConfig("None")).isEqualTo(Optional.of(TableType.NONE));
  }

  @Test
  void fromConfig_returnsEmptyOnNullOrEmpty() {
    assertThat(TableType.fromConfig(null)).isEmpty();
    assertThat(TableType.fromConfig("")).isEmpty();
  }

  @Test
  void fromConfig_orElseSnowflake_givesSnowflakeDefault() {
    assertThat(TableType.fromConfig(null).orElse(TableType.SNOWFLAKE))
        .isEqualTo(TableType.SNOWFLAKE);
    assertThat(TableType.fromConfig("").orElse(TableType.SNOWFLAKE)).isEqualTo(TableType.SNOWFLAKE);
    assertThat(TableType.fromConfig("iceberg").orElse(TableType.SNOWFLAKE))
        .isEqualTo(TableType.ICEBERG);
  }

  @Test
  void fromConfig_rejectsUnknownValue() {
    assertThatThrownBy(() -> TableType.fromConfig("delta"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("delta");
  }
}
