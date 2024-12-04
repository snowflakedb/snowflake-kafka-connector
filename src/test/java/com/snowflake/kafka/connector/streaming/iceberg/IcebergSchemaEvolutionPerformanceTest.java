package com.snowflake.kafka.connector.streaming.iceberg;

import static com.snowflake.kafka.connector.streaming.iceberg.TestJsons.STRING_PAYLOAD;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.snowflake.kafka.connector.internal.DescribeTableRow;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class IcebergSchemaEvolutionPerformanceTest extends IcebergIngestionIT {

  @Override
  protected void createIcebergTable() {
    createIcebergTable(tableName);
  }

  @Override
  protected Boolean isSchemaEvolutionEnabled() {
    return true;
  }

  @Test
  @Disabled
  void performanceNoSchemaEvolution() throws Exception {
    String payload = "{" + STRING_PAYLOAD + "}";
    int numberOfRecords = 333;
    long start = System.currentTimeMillis();

    for (int offset = 0; offset < numberOfRecords; offset++) {
      insertWithRetry(payload, offset, false);
    }
    long end = System.currentTimeMillis();
    long time = end - start;

    List<DescribeTableRow> columns = describeTable(tableName);
    assertEquals(2, columns.size());
    waitForOffset(numberOfRecords);
  }

  @Test
  @Disabled
  void testPerformanceInDepth() throws Exception {
    // given
    // number of object nested in one another, this is maximum limit fo ingest-sdk
    int depth = 333;
    ArrayList<String> payloads = preparePayloadsForNestedObjectEvolution(depth);
    // when
    long start = System.currentTimeMillis();
    int offset = 0;
    for (String payload : payloads) {
      insertWithRetry(payload, offset, false);
      offset++;
    }
    long end = System.currentTimeMillis();
    long time = end - start;

    // The average time is 800 ms/per single schema evolution. Assume we can't go above 1200
    // ms/evolution
    assertTrue(time / depth < 1200);

    List<DescribeTableRow> columns = describeTable(tableName);
    assertEquals(2, columns.size());
    waitForOffset(depth);
  }

  @Test
  @Disabled
  void testPerformanceWhenAddingNewColumns() throws Exception {
    // given
    // number of object nested in one another, this is maximum limit fo ingest-sdk
    int columnQuantity = 333;
    ArrayList<String> payloads = preparePayloadWithNewColumns(columnQuantity);
    // when
    long start = System.currentTimeMillis();
    int offset = 0;
    for (String payload : payloads) {
      insertWithRetry(payload, offset, false);
      offset++;
    }
    long end = System.currentTimeMillis();
    long time = end - start;

    // The average time is 800 ms/per single schema evolution. Assume we can't go above 1200
    // ms/evolution
    assertTrue(time / columnQuantity < 1200);

    List<DescribeTableRow> columns = describeTable(tableName);
    assertEquals(columnQuantity + 1, columns.size());
    waitForOffset(columnQuantity);
  }

  /** Every next record has additional column. */
  private ArrayList<String> preparePayloadWithNewColumns(int depth) {
    ArrayList<String> payloads = new ArrayList<>();
    StringJoiner joiner = new StringJoiner(",");

    for (int level = 0; level < depth; level++) {
      String objectName = "object" + level;
      joiner.add("\"" + objectName + "\" : \"text\"");
      payloads.add(toValidPayloadNewColumns(joiner));
    }
    return payloads;
  }

  private String toValidPayloadNewColumns(StringJoiner joiner) {
    return "{" + joiner.toString() + "}";
  }

  /**
   * Every next payload has one more nested object. { "object0": { "description": "text", "object1":
   * { "description": "text", "object2": { ... "objectN": { "description": "text" } }
   */
  private ArrayList<String> preparePayloadsForNestedObjectEvolution(int depth) {
    ArrayList<String> payloads = new ArrayList<>();
    StringJoiner joiner = new StringJoiner(",");

    for (int level = 0; level < depth; level++) {
      String objectName = "object" + level;
      joiner.add("\"" + objectName + "\" : { \"description\": \"text\"");
      payloads.add(toValidNestedPayloadJson(level, joiner));
    }

    return payloads;
  }

  private String toValidNestedPayloadJson(int depth, StringJoiner joiner) {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(joiner.toString());
    for (int i = 0; i <= depth; i++) {
      sb.append("}");
    }
    sb.append("}");
    return sb.toString();
  }
}
