package com.snowflake.kafka.connector.internal.schemaevolution;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class ColumnInfosTest {

  @Test
  void getDdlComments_withComment() {
    ColumnInfos infos = new ColumnInfos("VARCHAR", "user name field");
    assertThat(infos.getDdlComments()).isEqualTo(" comment 'user name field' ");
  }

  @Test
  void getDdlComments_withoutComment() {
    ColumnInfos infos = new ColumnInfos("INT");
    assertThat(infos.getDdlComments())
        .isEqualTo(" comment 'column created by schema evolution from Snowflake Kafka Connector' ");
  }

  @Test
  void getDdlComments_withNullComment() {
    ColumnInfos infos = new ColumnInfos("INT", null);
    assertThat(infos.getDdlComments())
        .isEqualTo(" comment 'column created by schema evolution from Snowflake Kafka Connector' ");
  }

  @Test
  void getDdlComments_escapeSingleQuotes() {
    ColumnInfos infos = new ColumnInfos("VARCHAR", "it's a test");
    assertThat(infos.getDdlComments()).isEqualTo(" comment 'it''s a test' ");
  }

  @Test
  void constructorRejectsNullColumnType() {
    assertThrows(NullPointerException.class, () -> new ColumnInfos(null));
    assertThrows(NullPointerException.class, () -> new ColumnInfos(null, "comment"));
  }

  @Test
  void equalityAndHashCode() {
    ColumnInfos a = new ColumnInfos("VARCHAR", "comment");
    ColumnInfos b = new ColumnInfos("VARCHAR", "comment");
    ColumnInfos c = new ColumnInfos("INT", "comment");
    ColumnInfos d = new ColumnInfos("VARCHAR", null);

    assertEquals(a, b);
    assertEquals(a.hashCode(), b.hashCode());
    assertNotEquals(a, c);
    assertNotEquals(a, d);
  }
}
