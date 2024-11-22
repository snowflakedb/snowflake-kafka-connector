package com.snowflake.kafka.connector.streaming.iceberg;

import static com.snowflake.kafka.connector.streaming.iceberg.sql.PrimitiveJsonRecord.emptyPrimitiveJsonRecordValueExample;
import static com.snowflake.kafka.connector.streaming.iceberg.sql.PrimitiveJsonRecord.primitiveJsonExample;
import static com.snowflake.kafka.connector.streaming.iceberg.sql.PrimitiveJsonRecord.primitiveJsonRecordValueExample;
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
  // @Disabled
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

  /** Verify a scenario when structure object is inserted for the first time. */
  @Test
  // @Disabled
  public void shouldResolveNewlyInsertedStructuredObjects() throws Exception {
    String testStruct1 = "{ \"testStruct\": { \"k1\" : 1, \"k2\" : 2 } }";
    insertWithRetry(testStruct1, 0, false);
    waitForOffset(1);

    String testStruct2 = "{ \"testStruct2\": {\"k1\" : 1, \"k3\" : 2 } }";
    insertWithRetry(testStruct2, 1, false);
    waitForOffset(2);

    String testStruct3 =
        "{ \"testStruct3\": {"
            + "\"k1\" : { \"car\" : { \"brand\" : \"vw\" } },"
            + "\"k2\" : { \"car\" : { \"brand\" : \"toyota\" } }"
            + "}}";
    insertWithRetry(testStruct3, 2, false);
    waitForOffset(3);

    List<DescribeTableRow> rows = describeTable(tableName);
    assertEquals(rows.size(), 4);
  }

  /** Verify a scenario when structure is enriched with another field. */
  @Test
  // @Disabled
  public void alterAlreadyExistingStructure() throws Exception {
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
  // @Disabled
  public void alterAlreadyExistingStructure_timestamp() throws Exception {
    // k1, k2
    String testStruct1 =
        "{\n"
            + "    \"_id\": \"673f3b93a56dd01a8a0cb6a4\",\n"
            + "    \"index\": 0,\n"
            + "    \"guid\": \"738142bb-5878-42ad-bf35-6015f63b67dd\",\n"
            + "    \"isActive\": true,\n"
            + "    \"balance\": \"$1,690.88\",\n"
            + "    \"picture\": \"http://placehold.it/32x32\",\n"
            + "    \"age\": 38,\n"
            + "    \"eyeColor\": \"blue\",\n"
            + "    \"name\": \"Davis Heath\",\n"
            + "    \"gender\": \"male\",\n"
            + "    \"company\": \"EVENTEX\",\n"
            + "    \"email\": \"davisheath@eventex.com\",\n"
            + "    \"phone\": \"+1 (987) 471-3852\",\n"
            + "    \"address\": \"768 Cypress Court, Lookingglass, Kansas, 5659\",\n"
            + "    \"about\": \"Nisi voluptate id occaecat nisi pariatur dolore laborum labore ea"
            + " reprehenderit consequat sint fugiat sunt. Et consequat esse ex cillum deserunt"
            + " Lorem. Enim nisi tempor non nisi. Consectetur ut ad reprehenderit fugiat et"
            + " adipisicing sint. Deserunt est proident exercitation sit cillum in non excepteur"
            + " aliqua qui amet cillum sint aliquip.\\r"
            + "\\n"
            + "\",\n"
            + "    \"registered\": \"2023-09-19T09:41:45 -02:00\",\n"
            + "    \"latitude\": 13.997901,\n"
            + "    \"longitude\": 130.854106,\n"
            + "    \"tags\": [\n"
            + "      \"et\",\n"
            + "      \"ut\",\n"
            + "      \"elit\",\n"
            + "      \"do\",\n"
            + "      \"nostrud\",\n"
            + "      \"id\",\n"
            + "      \"veniam\"\n"
            + "    ],\n"
            + "    \"friends\": [\n"
            + "      {\n"
            + "        \"id\": 0,\n"
            + "        \"name\": \"Sandoval Hodges\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"id\": 1,\n"
            + "        \"name\": \"Ramirez Brooks\"\n"
            + "      },\n"
            + "      {\n"
            + "        \"id\": 2,\n"
            + "        \"name\": \"Vivian Whitfield\"\n"
            + "      }\n"
            + "    ],\n"
            + "    \"greeting\": \"Hello, Davis Heath! You have 4 unread messages.\",\n"
            + "    \"favoriteFruit\": \"strawberry\"\n"
            + "  }";
    insertWithRetry(testStruct1, 0, false);
    waitForOffset(1);

    List<DescribeTableRow> columns = describeTable(tableName);
    assertEquals(columns.size(), 23);
  }

  @Test
  void test3() {
    String message =
        " {   \"name\": \"machine name\",  \"parts\": {\n"
            + "     \"1stPartId\": { \"group\": \"part group\", \"description\": \"abcd\" },\n"
            + "     \"2ndPartId\": { \"group\": \"part group\", \"description\": \"cda\","
            + " \"floatingNumber\" : 4.3 }\n"
            + "  }}\n";
    insertWithRetry(message, 0, false);
    List<DescribeTableRow> columns = describeTable(tableName);
    assertEquals(columns.size(), 2);
  }

  //  "PARTS OBJECT(" +
  //          "1stPartId OBJECT(description VARCHAR, group VARCHAR), " +
  //          "2ndPartId OBJECT(description VARCHAR, group VARCHAR))"

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

  private static Stream<Arguments> prepareData() {
    return Stream.of(
        // READING SCHEMA FROM A RECORD IS NOT YET SUPPORTED.
        //        Arguments.of(
        //            "Primitive JSON with schema",
        //            primitiveJsonWithSchemaExample,
        //            new DescribeTableRow[] {
        //              new DescribeTableRow("ID_INT8", "NUMBER(10,0)"),
        //              new DescribeTableRow("ID_INT16", "NUMBER(10,0)"),
        //              new DescribeTableRow("ID_INT32", "NUMBER(10,0)"),
        //              new DescribeTableRow("ID_INT64", "NUMBER(19,0)"),
        //              new DescribeTableRow("DESCRIPTION", "VARCHAR(16777216)"),
        //              new DescribeTableRow("RATING_FLOAT32", "FLOAT"),
        //              new DescribeTableRow("RATING_FLOAT64", "FLOAT"),
        //              new DescribeTableRow("APPROVAL", "BOOLEAN")
        //            },
        //            true),
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
}
