package com.snowflake.kafka.connector.streaming.iceberg;

import static com.snowflake.kafka.connector.streaming.iceberg.sql.ComplexJsonRecord.*;
import static com.snowflake.kafka.connector.streaming.iceberg.sql.PrimitiveJsonRecord.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.DescribeTableRow;
import com.snowflake.kafka.connector.streaming.iceberg.sql.MetadataRecord;
import com.snowflake.kafka.connector.streaming.iceberg.sql.MetadataRecord.RecordWithMetadata;
import com.snowflake.kafka.connector.streaming.iceberg.sql.PrimitiveJsonRecord;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class IcebergIngestionSchemaEvolutionIT extends IcebergIngestionIT {

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
    assertThat(rows).hasSize(10).contains(new DescribeTableRow("SIMPLE", "VARCHAR(16777216)"));

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
              new DescribeTableRow("DESCRIPTION", "VARCHAR(16777216)"),
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
              new DescribeTableRow("DESCRIPTION", "VARCHAR(16777216)"),
              new DescribeTableRow("RATING_FLOAT32", "FLOAT"),
              new DescribeTableRow("RATING_FLOAT64", "FLOAT"),
              new DescribeTableRow("APPROVAL", "BOOLEAN")
            },
            false));
  }

  /** Verify a scenario when structure is enriched with another field. */
  @Test
  public void alterStructure_noSchema() throws Exception {
    // k1, k2
    String testStruct1 = "{ \"testStruct\": { \"k1\" : 1, \"k2\" : 2 } }";
    insertWithRetry(testStruct1, 0, false);
    waitForOffset(1);

    // k1, k2 + k3
    String testStruct2 = "{ \"testStruct\": { \"k1\" : 1, \"k2\" : 2, \"k3\" : \"foo\" } }";
    insertWithRetry(testStruct2, 1, false);
    waitForOffset(2);

    // k1, k2, k3 + k4
    String testStruct3 =
        "{ \"testStruct\": { \"k1\" : 1, \"k2\" : 2, \"k3\" : \"bar\", \"k4\" : 4.5 } }";
    insertWithRetry(testStruct3, 2, false);
    waitForOffset(3);

    List<DescribeTableRow> columns = describeTable(tableName);
    assertEquals(
        columns.get(1).getType(),
        "OBJECT(k1 NUMBER(19,0), k2 NUMBER(19,0), k3 VARCHAR(16777216), k4 FLOAT)");

    // k2, k3, k4
    String testStruct4 = "{ \"testStruct\": { \"k2\" : 2, \"k3\" : 3, \"k4\" : 4.34 } }";
    insertWithRetry(testStruct4, 3, false);
    waitForOffset(4);

    columns = describeTable(tableName);
    assertEquals(
        columns.get(1).getType(),
        "OBJECT(k1 NUMBER(19,0), k2 NUMBER(19,0), k3 VARCHAR(16777216), k4 FLOAT)");

    // k5, k6
    String testStruct5 = "{ \"testStruct\": { \"k5\" : 2, \"k6\" : 3 } }";
    insertWithRetry(testStruct5, 4, false);
    waitForOffset(5);

    columns = describeTable(tableName);
    assertEquals(
        columns.get(1).getType(),
        "OBJECT(k1 NUMBER(19,0), k2 NUMBER(19,0), k3 VARCHAR(16777216), k4 FLOAT, k5 NUMBER(19,0),"
            + " k6 NUMBER(19,0))");
    assertEquals(columns.size(), 2);
  }

  @Test
  public void evolveSchemaRandomDataTest() throws Exception {
    String testStruct1 =
        "{"
            + "    \"_id\": \"673f3b93a56dd01a8a0cb6a4\","
            + "    \"index\": 0,"
            + "    \"guid\": \"738142bb-5878-42ad-bf35-6015f63b67dd\","
            + "    \"isActive\": true,"
            + "    \"balance\": \"$1,690.88\","
            + "    \"picture\": \"http://placehold.it/32x32\","
            + "    \"age\": 38,"
            + "    \"eyeColor\": \"blue\","
            + "    \"name\": \"Davis Heath\","
            + "    \"gender\": \"male\","
            + "    \"company\": \"EVENTEX\","
            + "    \"email\": \"davisheath@eventex.com\","
            + "    \"phone\": \"+1 (987) 471-3852\","
            + "    \"address\": \"768 Cypress Court, Lookingglass, Kansas, 5659\","
            + "    \"about\": \"Nisi voluptate id occaecat nisi pariatur dolore laborum labore ea"
            + " reprehenderit consequat sint fugiat sunt. Et consequat esse ex cillum deserunt"
            + " Lorem. Enim nisi tempor non nisi. Consectetur ut ad reprehenderit fugiat et"
            + " adipisicing sint. Deserunt est proident exercitation sit cillum in non excepteur"
            + " aliqua qui amet cillum sint aliquip.\\r"
            + "\","
            + "    \"registered\": \"2023-09-19T09:41:45 -02:00\","
            + "    \"latitude\": 13.997901,"
            + "    \"longitude\": 130.854106,"
            + "    \"tags\": ["
            + "      \"et\","
            + "      \"ut\","
            + "      \"elit\","
            + "      \"do\","
            + "      \"nostrud\","
            + "      \"id\","
            + "      \"veniam\""
            + "    ],"
            + "    \"friends\": ["
            + "      {"
            + "        \"id\": 0,"
            + "        \"name\": \"Sandoval Hodges\""
            + "      },"
            + "      {"
            + "        \"id\": 1,"
            + "        \"name\": \"Ramirez Brooks\""
            + "      },"
            + "      {"
            + "        \"id\": 2,"
            + "        \"name\": \"Vivian Whitfield\""
            + "      }"
            + "    ],"
            + "    \"greeting\": \"Hello, Davis Heath! You have 4 unread messages.\","
            + "    \"favoriteFruit\": \"strawberry\""
            + "  }";
    insertWithRetry(testStruct1, 0, false);
    waitForOffset(1);

    List<DescribeTableRow> columns = describeTable(tableName);
    assertEquals(columns.size(), 23);
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

  /** Verify a scenario when structure object is inserted for the first time. */
  @Test
  public void testComplexRecordEvolution_withSchema() throws Exception {
    insertWithRetry(complexJsonWithSchemaExample, 0, true);
    waitForOffset(1);

    List<DescribeTableRow> columns = describeTable(tableName);
    assertEquals(columns.size(), 16);
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
            TestJsons.schemaNestedObjects(TestJsons.nestedObjectsPayload),
            "OBJECT_WITH_NESTED_OBJECTS",
            "OBJECT(nestedStruct OBJECT(description VARCHAR(16777216)))"),
        Arguments.of(
            TestJsons.simpleMapSchema(TestJsons.simpleMapPayload),
            "SIMPLE_TEST_MAP",
            "MAP(VARCHAR(16777216), NUMBER(10,0))"),
        Arguments.of(
            TestJsons.simpleArraySchema(TestJsons.simpleArrayPayload),
            "SIMPLE_ARRAY",
            "ARRAY(NUMBER(10,0))"),
        Arguments.of(
            TestJsons.complexSchema(TestJsons.complexPayload),
            "OBJECT",
            "OBJECT(arrayOfMaps ARRAY(MAP(VARCHAR(16777216), FLOAT)))"));
  }

  @Test
  public void testEvolutionOfPrimitives_withSchema() throws Exception {
    // when insert BOOLEAN
    insertWithRetry(TestJsons.singleBooleanField(), 0, true);
    waitForOffset(1);
    List<DescribeTableRow> columns = describeTable(tableName);
    // verify number of columns, datatype and column name
    assertEquals(2, columns.size());
    assertEquals("TEST_BOOLEAN", columns.get(1).getColumn());
    assertEquals("BOOLEAN", columns.get(1).getType());

    // evolve the schema BOOLEAN, INT64
    insertWithRetry(TestJsons.booleanAndInt(), 1, true);
    waitForOffset(2);
    columns = describeTable(tableName);
    assertEquals(3, columns.size());
    // verify data types in already existing column were not changed
    assertEquals("TEST_BOOLEAN", columns.get(1).getColumn());
    assertEquals("BOOLEAN", columns.get(1).getType());
    // verify new columns
    assertEquals("TEST_INT64", columns.get(2).getColumn());
    assertEquals("NUMBER(19,0)", columns.get(2).getType());

    // evolve the schema BOOLEAN, INT64, INT32, INT16, INT8,
    insertWithRetry(TestJsons.booleanAndAllKindsOfInt(), 2, true);
    waitForOffset(3);
    columns = describeTable(tableName);
    assertEquals(6, columns.size());
    // verify data types in already existing column were not changed
    assertEquals("TEST_BOOLEAN", columns.get(1).getColumn());
    assertEquals("BOOLEAN", columns.get(1).getType());
    assertEquals("TEST_INT64", columns.get(2).getColumn());
    assertEquals("NUMBER(19,0)", columns.get(2).getType());
    // verify new columns
    assertEquals("TEST_INT32", columns.get(3).getColumn());
    assertEquals("NUMBER(10,0)", columns.get(3).getType());
    assertEquals("TEST_INT16", columns.get(4).getColumn());
    assertEquals("NUMBER(10,0)", columns.get(4).getType());
    assertEquals("TEST_INT8", columns.get(5).getColumn());
    assertEquals("NUMBER(10,0)", columns.get(5).getType());

    // evolve the schema BOOLEAN, INT64, INT32, INT16, INT8, FLOAT, DOUBLE, STRING
    insertWithRetry(TestJsons.allPrimitives(), 3, true);
    waitForOffset(4);
    columns = describeTable(tableName);
    assertEquals(9, columns.size());
    // verify data types in already existing column were not changed
    assertEquals("TEST_BOOLEAN", columns.get(1).getColumn());
    assertEquals("BOOLEAN", columns.get(1).getType());
    assertEquals("TEST_INT64", columns.get(2).getColumn());
    assertEquals("NUMBER(19,0)", columns.get(2).getType());
    assertEquals("TEST_INT32", columns.get(3).getColumn());
    assertEquals("NUMBER(10,0)", columns.get(3).getType());
    assertEquals("TEST_INT16", columns.get(4).getColumn());
    assertEquals("NUMBER(10,0)", columns.get(4).getType());
    assertEquals("TEST_INT8", columns.get(5).getColumn());
    assertEquals("NUMBER(10,0)", columns.get(5).getType());
    // verify new columns
    assertEquals("TEST_FLOAT", columns.get(6).getColumn());
    assertEquals("FLOAT", columns.get(6).getType());
    assertEquals("TEST_DOUBLE", columns.get(7).getColumn());
    assertEquals("FLOAT", columns.get(7).getType());
    assertEquals("TEST_STRING", columns.get(8).getColumn());
    assertEquals("VARCHAR(16777216)", columns.get(8).getType());
  }
}
