package com.snowflake.kafka.connector.streaming.iceberg;

import static com.snowflake.kafka.connector.streaming.iceberg.sql.ComplexVarcharRecord.BOOLEAN_EXAMPLE;
import static com.snowflake.kafka.connector.streaming.iceberg.sql.ComplexVarcharRecord.DOUBLE_EXAMPLE;
import static com.snowflake.kafka.connector.streaming.iceberg.sql.ComplexVarcharRecord.INT_EXAMPLE;
import static com.snowflake.kafka.connector.streaming.iceberg.sql.ComplexVarcharRecord.STRING_EXAMPLE;
import static org.assertj.core.api.Assertions.assertThat;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.streaming.iceberg.sql.ComplexVarcharRecord;
import com.snowflake.kafka.connector.streaming.iceberg.sql.MetadataRecord;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class IcebergIngestionVarcharIT extends IcebergIngestionIT {

  @Override
  protected void createIcebergTable() {
    createIcebergTableWithColumnClause(
        tableName,
        Utils.TABLE_COLUMN_METADATA
            + " "
            + IcebergDDLTypes.ICEBERG_METADATA_OBJECT_SCHEMA
            + ", "
            + "varchar_column VARCHAR(16777216),"
            + "varchar_list_column ARRAY(VARCHAR(16777216)),"
            + "object_column OBJECT("
            + " varchar_column VARCHAR(16777216),"
            + " varchar_list_column ARRAY(VARCHAR(16777216))"
            + ")");
  }

  @Override
  protected Boolean isSchemaEvolutionEnabled() {
    return true;
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("prepareData")
  void shouldInsertDifferentTypesIntoVarcharType(
      String description, String message, ComplexVarcharRecord result) throws Exception {
    service.insert(Collections.singletonList(createKafkaRecord(message, 0, false)));
    waitForOffset(1);
    List<MetadataRecord.RecordWithMetadata<ComplexVarcharRecord>> recordWithMetadata =
        selectAllComplexVarcharRecords();
    assertThat(recordWithMetadata).hasSize(1);
    assertThat(recordWithMetadata.get(0).getRecord()).isEqualTo(result);
  }

  private static Stream<Arguments> prepareData() {
    return Stream.of(
        Arguments.of(
            "intExample",
            INT_EXAMPLE,
            new ComplexVarcharRecord(
                "8",
                Arrays.asList("16", "24"),
                new ComplexVarcharRecord.VarcharRecord("32", Arrays.asList("64", "128")))),
        Arguments.of(
            "stringExample",
            STRING_EXAMPLE,
            new ComplexVarcharRecord(
                "eight",
                Arrays.asList("sixteen", "twenty-four"),
                new ComplexVarcharRecord.VarcharRecord(
                    "thirty-two", Arrays.asList("sixty-four", "one-twenty-eight")))),
        Arguments.of(
            "doubleExample",
            DOUBLE_EXAMPLE,
            new ComplexVarcharRecord(
                "8.5",
                Arrays.asList("16.5", "24.5"),
                new ComplexVarcharRecord.VarcharRecord("32.5", Arrays.asList("64.5", "128.5")))),
        Arguments.of(
            "booleanExample",
            BOOLEAN_EXAMPLE,
            new ComplexVarcharRecord(
                "true",
                Arrays.asList("false", "true"),
                new ComplexVarcharRecord.VarcharRecord("false", Arrays.asList("true", "false"))))
        // TODO: uncomment in SNOW-1842256
        //        Arguments.of(
        //            "listExample",
        //            ComplexVarcharRecord.LIST_EXAMPLE,
        //            new ComplexVarcharRecord(
        //                "[8]",
        //                Collections.singletonList("[16]"),
        //                new ComplexVarcharRecord.VarcharRecord("[32]", Arrays.asList("[64]",
        // "[128]")))),
        //        Arguments.of(
        //            "objectExample",
        //            ComplexVarcharRecord.OBJECT_EXAMPLE,
        //            new ComplexVarcharRecord(
        //                "{\"object\": 1}",
        //                Collections.singletonList("{\"object\": 1}"),
        //                new ComplexVarcharRecord.VarcharRecord(
        //                    "{\"object\": 1}", Collections.singletonList("{\"object\": 1}"))))
        );
  }
}
