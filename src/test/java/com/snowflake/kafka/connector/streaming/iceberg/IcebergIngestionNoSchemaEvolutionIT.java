package com.snowflake.kafka.connector.streaming.iceberg;

import com.snowflake.kafka.connector.Utils;
import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Stream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class IcebergIngestionNoSchemaEvolutionIT extends IcebergIngestionIT {

  private static final String RECORD_CONTENT_OBJECT_SCHEMA =
      "object("
          + "id_int8 NUMBER(10,0),"
          + "id_int16 NUMBER(10,0),"
          + "id_int32 NUMBER(10,0),"
          + "id_int64 NUMBER(19,0),"
          + "description VARCHAR(16777216),"
          + "rating_float32 FLOAT,"
          + "rating_float64 FLOAT,"
          + "approval BOOLEAN"
          + ")";

  @Override
  protected Boolean isSchemaEvolutionEnabled() {
    return false;
  }

  @Override
  protected void createIcebergTable() {
    createIcebergTableWithColumnClause(
        tableName,
        Utils.TABLE_COLUMN_METADATA
            + " "
            + IcebergDDLTypes.ICEBERG_METADATA_OBJECT_SCHEMA
            + ", "
            + Utils.TABLE_COLUMN_CONTENT
            + " "
            + RECORD_CONTENT_OBJECT_SCHEMA);
  }

  private static Stream<Arguments> prepareData() {
    return Stream.of(
        Arguments.of("Primitive JSON with schema", primitiveJsonWithSchema, true),
        Arguments.of("Primitive JSON without schema", primitiveJson, false));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("prepareData")
  @Disabled
  void shouldInsertRecords(String description, String message, boolean withSchema)
      throws Exception {
    service.insert(
        Arrays.asList(
            createKafkaRecord(message, 0, withSchema), createKafkaRecord(message, 1, withSchema)));
    waitForOffset(2);
    service.insert(Collections.singletonList(createKafkaRecord(message, 2, withSchema)));
    waitForOffset(3);
  }
}
