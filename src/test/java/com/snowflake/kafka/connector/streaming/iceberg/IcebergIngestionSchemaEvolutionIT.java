package com.snowflake.kafka.connector.streaming.iceberg;

import static com.snowflake.kafka.connector.streaming.iceberg.TestJsons.*;
import static com.snowflake.kafka.connector.streaming.iceberg.sql.ComplexJsonRecord.*;
import static com.snowflake.kafka.connector.streaming.iceberg.sql.PrimitiveJsonRecord.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.dlq.InMemoryKafkaRecordErrorReporter;
import com.snowflake.kafka.connector.internal.DescribeTableRow;
import com.snowflake.kafka.connector.internal.TestUtils;
import com.snowflake.kafka.connector.streaming.iceberg.sql.MetadataRecord;
import com.snowflake.kafka.connector.streaming.iceberg.sql.MetadataRecord.RecordWithMetadata;
import com.snowflake.kafka.connector.streaming.iceberg.sql.PrimitiveJsonRecord;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class IcebergIngestionSchemaEvolutionIT extends IcebergIngestionIT {

  private static final String RECORD_METADATA_TYPE =
      "OBJECT(offset NUMBER(19,0), topic VARCHAR(134217728), partition NUMBER(10,0), key"
          + " VARCHAR(134217728), schema_id NUMBER(10,0), key_schema_id NUMBER(10,0),"
          + " CreateTime NUMBER(19,0), LogAppendTime NUMBER(19,0),"
          + " SnowflakeConnectorPushTime NUMBER(19,0), headers MAP(VARCHAR(134217728),"
          + " VARCHAR(134217728)))";

  @Override
  protected Boolean isSchemaEvolutionEnabled() {
    return true;
  }

  @Override
  protected void createIcebergTable() {
    createIcebergTable(tableName);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("prepareData")
  void shouldEvolveSchemaAndInsertRecords(
      String description, String message, DescribeTableRow[] expectedSchema, boolean withSchema)
      throws Exception {
    // start off with just one column
    List<DescribeTableRow> rows = describeTable(tableName);
    assertThat(rows)
        .hasSize(1)
        .extracting(DescribeTableRow::getColumn)
        .contains(Utils.TABLE_COLUMN_METADATA);

    SinkRecord record = createKafkaRecord(message, 0, withSchema);
    service.insert(Collections.singletonList(record));
    waitForOffset(-1);
    rows = describeTable(tableName);
    assertThat(rows.size()).isEqualTo(9);

    // don't check metadata column schema, we have different tests for that
    rows =
        rows.stream()
            .filter(r -> !r.getColumn().equals(Utils.TABLE_COLUMN_METADATA))
            .collect(Collectors.toList());

    assertThat(rows).containsExactlyInAnyOrder(expectedSchema);

    // resend and store same record without any issues now
    service.insert(Collections.singletonList(record));
    waitForOffset(1);

    // and another record with same schema
    service.insert(Collections.singletonList(createKafkaRecord(message, 1, withSchema)));
    waitForOffset(2);

    // and another record with extra field - schema evolves again
    service.insert(Collections.singletonList(createKafkaRecord(simpleRecordJson, 2, false)));

    rows = describeTable(tableName);
    assertThat(rows).hasSize(10).contains(new DescribeTableRow("SIMPLE", "VARCHAR(134217728)"));

    // reinsert record with extra field
    service.insert(Collections.singletonList(createKafkaRecord(simpleRecordJson, 2, false)));
    waitForOffset(3);

    assertRecordsInTable();
  }

  private static Stream<Arguments> prepareData() {
    return Stream.of(
        Arguments.of(
            "Primitive JSON with schema",
            primitiveJsonWithSchemaExample,
            new DescribeTableRow[] {
              new DescribeTableRow("ID_INT8", "NUMBER(10,0)"),
              new DescribeTableRow("ID_INT16", "NUMBER(10,0)"),
              new DescribeTableRow("ID_INT32", "NUMBER(10,0)"),
              new DescribeTableRow("ID_INT64", "NUMBER(19,0)"),
              new DescribeTableRow("DESCRIPTION", "VARCHAR(134217728)"),
              new DescribeTableRow("RATING_FLOAT32", "FLOAT"),
              new DescribeTableRow("RATING_FLOAT64", "FLOAT"),
              new DescribeTableRow("APPROVAL", "BOOLEAN")
            },
            true),
        Arguments.of(
            "Primitive JSON without schema",
            primitiveJsonExample,
            new DescribeTableRow[] {
              new DescribeTableRow("ID_INT8", "NUMBER(19,0)"),
              new DescribeTableRow("ID_INT16", "NUMBER(19,0)"),
              new DescribeTableRow("ID_INT32", "NUMBER(19,0)"),
              new DescribeTableRow("ID_INT64", "NUMBER(19,0)"),
              new DescribeTableRow("DESCRIPTION", "VARCHAR(134217728)"),
              new DescribeTableRow("RATING_FLOAT32", "FLOAT"),
              new DescribeTableRow("RATING_FLOAT64", "FLOAT"),
              new DescribeTableRow("APPROVAL", "BOOLEAN")
            },
            false));
  }

  /**
   * Verify a scenario when structure is enriched with another field. Verifies that data type of
   * column is not modified, when value is different or is not even present in a next record.
   */
  @Test
  public void alterStructure_noSchema() throws Exception {
    // k1, k2
    String testStruct1 = "{ \"testStruct\": { \"k1\" : 1, \"k2\" : 2 } }";
    insertWithRetry(testStruct1, 0, false);

    // k1, k2 + k3
    String testStruct2 = "{ \"testStruct\": { \"k1\" : 1, \"k2\" : 2, \"k3\" : \"foo\" } }";
    insertWithRetry(testStruct2, 1, false);

    // k1, k2, k3 + k4
    String testStruct3 =
        "{ \"testStruct\": { \"k1\" : 1, \"k2\" : 2, \"k3\" : \"bar\", \"k4\" : 4.5 } }";
    insertWithRetry(testStruct3, 2, false);

    List<DescribeTableRow> columns = describeTable(tableName);
    assertEquals(
        columns.get(1).getType(),
        "OBJECT(k1 NUMBER(19,0), k2 NUMBER(19,0), k3 VARCHAR(134217728), k4 FLOAT)");

    // k2, k3, k4
    String testStruct4 = "{ \"testStruct\": { \"k2\" : 2, \"k3\" : 3, \"k4\" : 4.34 } }";
    insertWithRetry(testStruct4, 3, false);

    columns = describeTable(tableName);
    assertEquals(
        columns.get(1).getType(),
        "OBJECT(k1 NUMBER(19,0), k2 NUMBER(19,0), k3 VARCHAR(134217728), k4 FLOAT)");

    // k5, k6
    String testStruct5 = "{ \"testStruct\": { \"k5\" : 2, \"k6\" : 3 } }";
    insertWithRetry(testStruct5, 4, false);
    waitForOffset(5);

    columns = describeTable(tableName);
    assertEquals(
        columns.get(1).getType(),
        "OBJECT(k1 NUMBER(19,0), k2 NUMBER(19,0), k3 VARCHAR(134217728), k4 FLOAT, k5 NUMBER(19,0),"
            + " k6 NUMBER(19,0))");
    assertEquals(columns.size(), 2);
  }

  private void insertWithRetry(String record, int offset, boolean withSchema) {
    service.insert(Collections.singletonList(createKafkaRecord(record, offset, withSchema)));
    service.insert(Collections.singletonList(createKafkaRecord(record, offset, withSchema)));
  }

  private void assertRecordsInTable() {
    List<RecordWithMetadata<PrimitiveJsonRecord>> recordsWithMetadata =
        selectAllSchematizedRecords();

    assertThat(recordsWithMetadata)
        .hasSize(3)
        .extracting(RecordWithMetadata::getRecord)
        .containsExactly(
            primitiveJsonRecordValueExample,
            primitiveJsonRecordValueExample,
            emptyPrimitiveJsonRecordValueExample);
    List<MetadataRecord> metadataRecords =
        recordsWithMetadata.stream()
            .map(RecordWithMetadata::getMetadata)
            .collect(Collectors.toList());
    assertThat(metadataRecords).extracting(MetadataRecord::getOffset).containsExactly(0L, 1L, 2L);
    assertThat(metadataRecords)
        .hasSize(3)
        .allMatch(
            r ->
                r.getTopic().equals(topicPartition.topic())
                    && r.getPartition().equals(topicPartition.partition())
                    && r.getKey().equals("test")
                    && r.getSnowflakeConnectorPushTime() != null);
  }

  @Test
  public void testComplexRecordEvolution_withSchema() throws Exception {
    insertWithRetry(complexJsonWithSchemaExample, 0, true);
    waitForOffset(1);

    List<DescribeTableRow> columns = describeTable(tableName);
    assertEquals(columns.size(), 16);

    DescribeTableRow[] expectedSchema =
        new DescribeTableRow[] {
          new DescribeTableRow("RECORD_METADATA", RECORD_METADATA_TYPE),
          new DescribeTableRow("ID_INT8", "NUMBER(10,0)"),
          new DescribeTableRow("ID_INT16", "NUMBER(10,0)"),
          new DescribeTableRow("ID_INT32", "NUMBER(10,0)"),
          new DescribeTableRow("ID_INT64", "NUMBER(19,0)"),
          new DescribeTableRow("DESCRIPTION", "VARCHAR(134217728)"),
          new DescribeTableRow("RATING_FLOAT32", "FLOAT"),
          new DescribeTableRow("RATING_FLOAT64", "FLOAT"),
          new DescribeTableRow("APPROVAL", "BOOLEAN"),
          new DescribeTableRow("ARRAY1", "ARRAY(NUMBER(10,0))"),
          new DescribeTableRow("ARRAY2", "ARRAY(VARCHAR(134217728))"),
          new DescribeTableRow("ARRAY3", "ARRAY(BOOLEAN)"),
          new DescribeTableRow("ARRAY4", "ARRAY(NUMBER(10,0))"),
          new DescribeTableRow("ARRAY5", "ARRAY(ARRAY(NUMBER(10,0)))"),
          new DescribeTableRow(
              "NESTEDRECORD",
              "OBJECT(id_int8 NUMBER(10,0), id_int16 NUMBER(10,0), id_int32 NUMBER(10,0), id_int64"
                  + " NUMBER(19,0), description VARCHAR(134217728), rating_float32 FLOAT,"
                  + " rating_float64 FLOAT, approval BOOLEAN)"),
          new DescribeTableRow(
              "NESTEDRECORD2",
              "OBJECT(id_int8 NUMBER(10,0), id_int16 NUMBER(10,0), id_int32 NUMBER(10,0), id_int64"
                  + " NUMBER(19,0), description VARCHAR(134217728), rating_float32 FLOAT,"
                  + " rating_float64 FLOAT, approval BOOLEAN)"),
        };
    assertThat(columns).containsExactlyInAnyOrder(expectedSchema);
  }

  @Test
  public void testComplexRecordEvolution() throws Exception {
    insertWithRetry(complexJsonPayloadExample, 0, false);
    waitForOffset(1);

    List<DescribeTableRow> columns = describeTable(tableName);
    assertEquals(columns.size(), 16);

    DescribeTableRow[] expectedSchema =
        new DescribeTableRow[] {
          new DescribeTableRow("RECORD_METADATA", RECORD_METADATA_TYPE),
          new DescribeTableRow("ID_INT8", "NUMBER(19,0)"),
          new DescribeTableRow("ID_INT16", "NUMBER(19,0)"),
          new DescribeTableRow("ID_INT32", "NUMBER(19,0)"),
          new DescribeTableRow("ID_INT64", "NUMBER(19,0)"),
          new DescribeTableRow("DESCRIPTION", "VARCHAR(134217728)"),
          new DescribeTableRow("RATING_FLOAT32", "FLOAT"),
          new DescribeTableRow("RATING_FLOAT64", "FLOAT"),
          new DescribeTableRow("APPROVAL", "BOOLEAN"),
          new DescribeTableRow("ARRAY1", "ARRAY(NUMBER(19,0))"),
          new DescribeTableRow("ARRAY2", "ARRAY(VARCHAR(134217728))"),
          new DescribeTableRow("ARRAY3", "ARRAY(BOOLEAN)"),
          new DescribeTableRow("ARRAY4", "ARRAY(NUMBER(19,0))"),
          new DescribeTableRow("ARRAY5", "ARRAY(ARRAY(NUMBER(19,0)))"),
          new DescribeTableRow(
              "NESTEDRECORD",
              "OBJECT(id_int8 NUMBER(19,0), id_int16 NUMBER(19,0), rating_float32 FLOAT,"
                  + " rating_float64 FLOAT, approval BOOLEAN, id_int32 NUMBER(19,0), description"
                  + " VARCHAR(134217728), id_int64 NUMBER(19,0))"),
          new DescribeTableRow(
              "NESTEDRECORD2",
              "OBJECT(id_int8 NUMBER(19,0), id_int16 NUMBER(19,0), rating_float32 FLOAT,"
                  + " rating_float64 FLOAT, approval BOOLEAN, id_int32 NUMBER(19,0), description"
                  + " VARCHAR(134217728), id_int64 NUMBER(19,0))"),
        };
    assertThat(columns).containsExactlyInAnyOrder(expectedSchema);
  }

  /** Test just for a scenario when we see a record for the first time. */
  @ParameterizedTest
  @MethodSource("schemasAndPayloads_brandNewColumns")
  public void addBrandNewColumns_withSchema(
      String payloadWithSchema, String expectedColumnName, String expectedType) throws Exception {
    // when
    insertWithRetry(payloadWithSchema, 0, true);
    waitForOffset(1);
    // then
    List<DescribeTableRow> columns = describeTable(tableName);

    assertEquals(2, columns.size());
    assertEquals(expectedColumnName, columns.get(1).getColumn());
    assertEquals(expectedType, columns.get(1).getType());
  }

  private static Stream<Arguments> schemasAndPayloads_brandNewColumns() {
    return Stream.of(
        Arguments.of(
            nestedObjectWithSchema(),
            "OBJECT_WITH_NESTED_OBJECTS",
            "OBJECT(nestedStruct OBJECT(description VARCHAR(134217728)))"),
        Arguments.of(
            simpleMapWithSchema(), "SIMPLE_TEST_MAP", "MAP(VARCHAR(134217728), NUMBER(10,0))"),
        Arguments.of(simpleArrayWithSchema(), "SIMPLE_ARRAY", "ARRAY(NUMBER(10,0))"),
        Arguments.of(
            complexPayloadWithSchema(),
            "OBJECT",
            "OBJECT(arrayOfMaps ARRAY(MAP(VARCHAR(134217728), FLOAT)))"));
  }

  @ParameterizedTest
  @MethodSource("primitiveEvolutionDataSource")
  public void testEvolutionOfPrimitives_withSchema(
      String singleBooleanField,
      String booleanAndInt,
      String booleanAndAllKindsOfInt,
      String allPrimitives,
      boolean withSchema)
      throws Exception {
    // when insert BOOLEAN
    insertWithRetry(singleBooleanField, 0, withSchema);
    List<DescribeTableRow> columns = describeTable(tableName);
    // verify number of columns, datatype and column name
    assertEquals(2, columns.size());
    assertEquals("TEST_BOOLEAN", columns.get(1).getColumn());
    assertEquals("BOOLEAN", columns.get(1).getType());

    // evolve the schema BOOLEAN, INT64
    insertWithRetry(booleanAndInt, 1, withSchema);
    columns = describeTable(tableName);
    assertEquals(3, columns.size());
    // verify data types in already existing column were not changed
    assertEquals("TEST_BOOLEAN", columns.get(1).getColumn());
    assertEquals("BOOLEAN", columns.get(1).getType());
    // verify new columns
    assertEquals("TEST_INT64", columns.get(2).getColumn());
    assertEquals("NUMBER(19,0)", columns.get(2).getType());

    // evolve the schema BOOLEAN, INT64, INT32, INT16, INT8,
    insertWithRetry(booleanAndAllKindsOfInt, 2, withSchema);
    columns = describeTable(tableName);
    assertEquals(6, columns.size());
    // verify data types in already existing column were not changed

    // without schema every number is parsed to NUMBER(19,0)
    String SMALL_INT = withSchema ? "NUMBER(10,0)" : "NUMBER(19,0)";
    DescribeTableRow[] expectedSchema =
        new DescribeTableRow[] {
          new DescribeTableRow("RECORD_METADATA", RECORD_METADATA_TYPE),
          new DescribeTableRow("TEST_BOOLEAN", "BOOLEAN"),
          new DescribeTableRow("TEST_INT8", SMALL_INT),
          new DescribeTableRow("TEST_INT16", SMALL_INT),
          new DescribeTableRow("TEST_INT32", SMALL_INT),
          new DescribeTableRow("TEST_INT64", "NUMBER(19,0)")
        };
    assertThat(columns).containsExactlyInAnyOrder(expectedSchema);

    // evolve the schema BOOLEAN, INT64, INT32, INT16, INT8, FLOAT, DOUBLE, STRING
    insertWithRetry(allPrimitives, 3, withSchema);
    waitForOffset(4);
    columns = describeTable(tableName);
    assertEquals(9, columns.size());

    expectedSchema =
        new DescribeTableRow[] {
          new DescribeTableRow("RECORD_METADATA", RECORD_METADATA_TYPE),
          new DescribeTableRow("TEST_BOOLEAN", "BOOLEAN"),
          new DescribeTableRow("TEST_INT8", SMALL_INT),
          new DescribeTableRow("TEST_INT16", SMALL_INT),
          new DescribeTableRow("TEST_INT32", SMALL_INT),
          new DescribeTableRow("TEST_INT64", "NUMBER(19,0)"),
          new DescribeTableRow("TEST_STRING", "VARCHAR(134217728)"),
          new DescribeTableRow("TEST_FLOAT", "FLOAT"),
          new DescribeTableRow("TEST_DOUBLE", "FLOAT")
        };

    assertThat(columns).containsExactlyInAnyOrder(expectedSchema);
  }

  private static Stream<Arguments> primitiveEvolutionDataSource() {
    return Stream.of(
        Arguments.of(
            singleBooleanFieldWithSchema(),
            booleanAndIntWithSchema(),
            booleanAndAllKindsOfIntWithSchema(),
            allPrimitivesWithSchema(),
            true),
        Arguments.of(
            singleBooleanFieldPayload(),
            booleanAndIntPayload(),
            booleanAndAllKindsOfIntPayload(),
            allPrimitivesPayload(),
            false));
  }

  @ParameterizedTest
  @MethodSource("testEvolutionOfComplexTypes_dataSource")
  public void testEvolutionOfComplexTypes_withSchema(
      String objectVarchar,
      String objectWithNestedObject,
      String twoObjects,
      String twoObjectsExtendedWithMapAndArray,
      boolean withSchema)
      throws Exception {
    // insert
    insertWithRetry(objectVarchar, 0, withSchema);
    List<DescribeTableRow> columns = describeTable(tableName);
    // verify number of columns, datatype and column name
    assertEquals(2, columns.size());
    assertEquals("OBJECT", columns.get(1).getColumn());
    assertEquals("OBJECT(test_string VARCHAR(134217728))", columns.get(1).getType());

    // evolution
    insertWithRetry(objectWithNestedObject, 1, withSchema);
    columns = describeTable(tableName);
    // verify number of columns, datatype and column name
    assertEquals(2, columns.size());
    assertEquals("OBJECT", columns.get(1).getColumn());
    assertEquals(
        "OBJECT(test_string VARCHAR(134217728), nested_object OBJECT(test_string"
            + " VARCHAR(134217728)))",
        columns.get(1).getType());

    // evolution
    insertWithRetry(twoObjects, 2, withSchema);
    columns = describeTable(tableName);

    assertEquals(3, columns.size());
    // 1st column
    assertEquals("OBJECT", columns.get(1).getColumn());
    assertEquals(
        "OBJECT(test_string VARCHAR(134217728), nested_object OBJECT(test_string"
            + " VARCHAR(134217728)))",
        columns.get(1).getType());
    // 2nd column
    assertEquals("OBJECT_WITH_NESTED_OBJECTS", columns.get(2).getColumn());
    assertEquals(
        "OBJECT(nestedStruct OBJECT(description VARCHAR(134217728)))", columns.get(2).getType());

    // evolution
    insertWithRetry(twoObjectsExtendedWithMapAndArray, 3, withSchema);
    waitForOffset(4);
    columns = describeTable(tableName);

    assertEquals(3, columns.size());
    // 1st column
    assertEquals("OBJECT", columns.get(1).getColumn());
    if (withSchema) {
      // MAP is not supported without schema, execute this assertion only when there is a schema,
      assertEquals(
          "OBJECT(test_string VARCHAR(134217728), nested_object OBJECT(test_string"
              + " VARCHAR(134217728)), Test_Map MAP(VARCHAR(134217728), OBJECT(test_string"
              + " VARCHAR(134217728))))",
          columns.get(1).getType());
    }
    // 2nd column
    assertEquals("OBJECT_WITH_NESTED_OBJECTS", columns.get(2).getColumn());
    assertEquals(
        "OBJECT(nestedStruct OBJECT(description VARCHAR(134217728), test_array ARRAY(FLOAT)))",
        columns.get(2).getType());
  }

  private static Stream<Arguments> testEvolutionOfComplexTypes_dataSource() {
    return Stream.of(
        Arguments.of(
            objectVarcharWithSchema(),
            objectWithNestedObjectWithSchema(),
            twoObjectsWithSchema(),
            twoObjectsExtendedWithMapAndArrayWithSchema(),
            true),
        Arguments.of(
            objectVarcharPayload,
            objectWithNestedObjectPayload(),
            twoObjectsWithSchemaPayload(),
            twoObjectsExtendedWithMapAndArrayPayload(),
            false));
  }

  @Test
  void shouldAppendCommentTest() throws Exception {
    // when
    // insert record with a comment
    insertWithRetry(schemaAndPayloadWithComment(), 0, true);
    // insert record without a comment
    insertWithRetry(singleBooleanFieldWithSchema(), 1, true);
    waitForOffset(2);

    // then
    // comment is read from schema and set into first column
    List<DescribeTableRow> columns = describeTable(tableName);
    assertEquals("Test comment", columns.get(1).getComment());
    // default comment is set into second column
    assertEquals(
        "column created by schema evolution from Snowflake Kafka Connector",
        columns.get(2).getComment());
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("nullOrEmptyValueShouldBeSentToDLQOnlyWhenNoSchema_dataSource")
  void nullOrEmptyValueShouldBeSentToDLQOnlyWhenNoSchema(
      String description, String jsonWithNullOrEmpty, String jsonWithFullData) throws Exception {
    // given
    SinkRecord emptyOrNullRecord = createKafkaRecord(jsonWithNullOrEmpty, 0, false);

    // when
    // sending null value or empty list/object record with no schema defined should be sent to DLQ,
    // second record with full data should create schema so the same null/empty record sent again
    // should be ingested without any issues
    service.insert(Arrays.asList(emptyOrNullRecord, createKafkaRecord(jsonWithFullData, 1, false)));
    // retry due to schema evolution
    service.insert(
        Arrays.asList(
            createKafkaRecord(jsonWithFullData, 1, false),
            createKafkaRecord(jsonWithNullOrEmpty, 2, false)));

    // then
    waitForOffset(3);
    List<InMemoryKafkaRecordErrorReporter.ReportedRecord> reportedRecords =
        kafkaRecordErrorReporter.getReportedRecords();
    assertThat(reportedRecords).hasSize(1);
    assertThat(reportedRecords.get(0).getRecord()).isEqualTo(emptyOrNullRecord);
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("wrongTypeValueMessages_dataSource")
  void shouldSendValueWithWrongTypeToDLQ(
      String description, String correctValueJson, String wrongValueJson) throws Exception {
    // when
    // init schema with first correct value
    insertWithRetry(correctValueJson, 0, false);

    // insert record with wrong value followed by
    SinkRecord wrongValueRecord = createKafkaRecord(wrongValueJson, 1, false);
    service.insert(Arrays.asList(wrongValueRecord, createKafkaRecord(correctValueJson, 2, false)));

    // then
    waitForOffset(3);
    List<InMemoryKafkaRecordErrorReporter.ReportedRecord> reportedRecords =
        kafkaRecordErrorReporter.getReportedRecords();
    assertThat(reportedRecords).hasSize(1);
    assertThat(reportedRecords.get(0).getRecord()).isEqualTo(wrongValueRecord);
  }

  /**
   * Test for SNOW-2266941: Unable to insert timestamp (google.protobuf.Timestamp) type into iceberg
   * table (via protobuf). This test reproduces the issue using JSON with schema instead of protobuf
   * to validate that timestamp logical types are handled correctly.
   */
  @Test
  public void testTimestampLogicalTypeSchemaEvolution() throws Exception {
    // Insert a record with timestamp logical type schema
    insertWithRetry(timestampWithSchemaExample(), 0, true);
    waitForOffset(1);

    // Verify the schema was created correctly
    List<DescribeTableRow> columns = describeTable(tableName);
    DescribeTableRow[] expectedSchema =
        new DescribeTableRow[] {
          new DescribeTableRow("RECORD_METADATA", RECORD_METADATA_TYPE),
          new DescribeTableRow("TIMESTAMP_RECEIVED", "TIMESTAMP_NTZ(6)")
        };
    assertThat(columns).containsExactlyInAnyOrder(expectedSchema);

    // Verify the timestamp content was inserted correctly
    Map<String, Object> expectedContent = new HashMap<>();
    expectedContent.put("RECORD_METADATA", "RECORD_METADATA_PLACE_HOLDER");
    expectedContent.put(
        "TIMESTAMP_RECEIVED", java.sql.Timestamp.valueOf("2023-01-01 00:00:00.000"));
    TestUtils.checkTableContentOneRow(tableName, expectedContent);

    // Insert another record with the same schema to ensure it works consistently
    insertWithRetry(timestampWithSchemaExample(), 1, true);
    waitForOffset(2);
  }

  private static Stream<Arguments> nullOrEmptyValueShouldBeSentToDLQOnlyWhenNoSchema_dataSource() {
    return Stream.of(
        Arguments.of("Null int", "{\"test\" : null }", "{\"test\" : 1 }"),
        // only test for int, no other primitive types to speed up the test
        Arguments.of("Null list", "{\"test\" : null }", "{\"test\" : [1,2] }"),
        Arguments.of("Null object", "{\"test\" : null }", "{\"test\" : {\"test2\": 1} }"),
        Arguments.of("Empty list", "{\"test\" : [] }", "{\"test\" : [1,2] }"),
        Arguments.of("Empty object", "{\"test\" : {} }", "{\"test\" : {\"test2\": 1} }"),
        Arguments.of(
            "Null in nested object",
            "{\"test\" : {\"test2\": null} }",
            "{\"test\" : {\"test2\": 1} }"),
        Arguments.of(
            "Empty list in nested object",
            "{\"test\" : {\"test2\": []} }",
            "{\"test\" : {\"test2\": [1]} }"),
        Arguments.of(
            "Empty object in nested object",
            "{\"test\" : {\"test2\": {}} }",
            "{\"test\" : {\"test2\": {\"test3\": 1}} }"));
  }

  private static Stream<Arguments> wrongTypeValueMessages_dataSource() {
    return Stream.of(
        Arguments.of("Boolean into double column", "{\"test\" : 2.5 }", "{\"test\" : true }"),
        Arguments.of("String into double column", "{\"test\" : 2.5 }", "{\"test\" : \"Solnik\" }"),
        Arguments.of("Int into list", "{\"test\" : [1,2] }", "{\"test\" : 1 }"),
        Arguments.of("Int into object", "{\"test\" : {\"test2\": 1} }", "{\"test\" : 1 }"),
        Arguments.of(
            "String into boolean in nested object",
            "{\"test\" : {\"test2\": true} }",
            "{\"test\" : {\"test2\": \"solnik\"} }"));
  }
}
