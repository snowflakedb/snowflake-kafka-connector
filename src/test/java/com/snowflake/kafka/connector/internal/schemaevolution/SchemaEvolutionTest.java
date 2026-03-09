/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 *
 * Tests for schema evolution components (Commit 5).
 */

package com.snowflake.kafka.connector.internal.schemaevolution;

import static org.junit.jupiter.api.Assertions.*;

import com.snowflake.kafka.connector.internal.validation.ValidationResult;
import java.util.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

/** Tests for schema evolution components */
public class SchemaEvolutionTest {

  // ================ SnowflakeColumnTypeMapper Tests ================

  @Test
  public void testMapKafkaTypesToSnowflake() {
    SnowflakeColumnTypeMapper mapper = new SnowflakeColumnTypeMapper();

    assertEquals("BYTEINT", mapper.mapToColumnType(Schema.Type.INT8));
    assertEquals("SMALLINT", mapper.mapToColumnType(Schema.Type.INT16));
    assertEquals("INT", mapper.mapToColumnType(Schema.Type.INT32));
    assertEquals("BIGINT", mapper.mapToColumnType(Schema.Type.INT64));
    assertEquals("FLOAT", mapper.mapToColumnType(Schema.Type.FLOAT32));
    assertEquals("DOUBLE", mapper.mapToColumnType(Schema.Type.FLOAT64));
    assertEquals("BOOLEAN", mapper.mapToColumnType(Schema.Type.BOOLEAN));
    assertEquals("VARCHAR", mapper.mapToColumnType(Schema.Type.STRING));
    assertEquals("ARRAY", mapper.mapToColumnType(Schema.Type.ARRAY));
    assertEquals("VARIANT", mapper.mapToColumnType(Schema.Type.MAP));
    assertEquals("VARIANT", mapper.mapToColumnType(Schema.Type.STRUCT));
  }

  @Test
  public void testMapLogicalTypes() {
    SnowflakeColumnTypeMapper mapper = new SnowflakeColumnTypeMapper();

    assertEquals(
        "DATE", mapper.mapToColumnType(Schema.Type.INT32, "org.apache.kafka.connect.data.Date"));
    assertEquals(
        "TIME(6)", mapper.mapToColumnType(Schema.Type.INT32, "org.apache.kafka.connect.data.Time"));
    assertEquals(
        "TIMESTAMP(6)",
        mapper.mapToColumnType(Schema.Type.INT64, "org.apache.kafka.connect.data.Timestamp"));
    assertEquals(
        "VARCHAR",
        mapper.mapToColumnType(Schema.Type.BYTES, "org.apache.kafka.connect.data.Decimal"));
  }

  @Test
  public void testInferTypeFromJavaValue() {
    SnowflakeColumnTypeMapper mapper = new SnowflakeColumnTypeMapper();

    assertEquals("VARCHAR", mapper.inferTypeFromJavaValue(null));
    assertEquals("VARCHAR", mapper.inferTypeFromJavaValue("hello"));
    assertEquals("INT", mapper.inferTypeFromJavaValue(42));
    assertEquals("BIGINT", mapper.inferTypeFromJavaValue(42L));
    assertEquals("SMALLINT", mapper.inferTypeFromJavaValue((short) 42));
    assertEquals("FLOAT", mapper.inferTypeFromJavaValue(1.5f));
    assertEquals("DOUBLE", mapper.inferTypeFromJavaValue(1.5d));
    assertEquals("BOOLEAN", mapper.inferTypeFromJavaValue(true));
    assertEquals("BINARY", mapper.inferTypeFromJavaValue(new byte[] {1, 2, 3}));
    assertEquals("ARRAY", mapper.inferTypeFromJavaValue(Arrays.asList(1, 2, 3)));
    assertEquals("VARIANT", mapper.inferTypeFromJavaValue(new HashMap<>()));
  }

  // ================ ColumnInfos Tests ================

  @Test
  public void testColumnInfosDdlComments() {
    ColumnInfos withComment = new ColumnInfos("VARCHAR", "user name field");
    assertEquals(" comment 'user name field' ", withComment.getDdlComments());

    ColumnInfos withoutComment = new ColumnInfos("INT");
    assertEquals(
        " comment 'column created by schema evolution from Snowflake Kafka Connector' ",
        withoutComment.getDdlComments());
  }

  @Test
  public void testColumnInfosEquality() {
    ColumnInfos a = new ColumnInfos("VARCHAR", "comment");
    ColumnInfos b = new ColumnInfos("VARCHAR", "comment");
    ColumnInfos c = new ColumnInfos("INT", "comment");

    assertEquals(a, b);
    assertNotEquals(a, c);
    assertEquals(a.hashCode(), b.hashCode());
  }

  // ================ SchemaEvolutionTargetItems Tests ================

  @Test
  public void testSchemaEvolutionTargetItemsHasData() {
    SchemaEvolutionTargetItems withAddCols =
        new SchemaEvolutionTargetItems("test_table", new HashSet<>(Arrays.asList("COL1")));
    assertTrue(withAddCols.hasDataForSchemaEvolution());

    SchemaEvolutionTargetItems withDropNonNull =
        new SchemaEvolutionTargetItems(
            "test_table", new HashSet<>(Arrays.asList("COL2")), Collections.emptySet());
    assertTrue(withDropNonNull.hasDataForSchemaEvolution());

    SchemaEvolutionTargetItems empty =
        new SchemaEvolutionTargetItems(
            "test_table", Collections.emptySet(), Collections.emptySet());
    assertFalse(empty.hasDataForSchemaEvolution());
  }

  @Test
  public void testSchemaEvolutionTargetItemsNullSafety() {
    SchemaEvolutionTargetItems items = new SchemaEvolutionTargetItems("table", null, null);
    assertNotNull(items.getColumnsToAdd());
    assertNotNull(items.getColumnsToDropNonNullability());
    assertTrue(items.getColumnsToAdd().isEmpty());
    assertTrue(items.getColumnsToDropNonNullability().isEmpty());
  }

  // ================ ValidationResultMapper Tests ================

  @Test
  public void testMapValidationResultToSchemaEvolution() {
    Set<String> extraCols = new HashSet<>(Arrays.asList("NEW_COL1", "NEW_COL2"));
    Set<String> missingNotNull = new HashSet<>(Arrays.asList("REQUIRED_COL"));
    Set<String> nullNotNull = new HashSet<>(Arrays.asList("ANOTHER_COL"));

    ValidationResult result =
        ValidationResult.structuralError(extraCols, missingNotNull, nullNotNull);

    SchemaEvolutionTargetItems items =
        ValidationResultMapper.mapToSchemaEvolutionItems(result, "MY_TABLE");

    assertEquals("MY_TABLE", items.getTableName());
    assertEquals(2, items.getColumnsToAdd().size());
    assertTrue(items.getColumnsToAdd().contains("NEW_COL1"));
    assertTrue(items.getColumnsToAdd().contains("NEW_COL2"));
    assertEquals(2, items.getColumnsToDropNonNullability().size());
    assertTrue(items.getColumnsToDropNonNullability().contains("REQUIRED_COL"));
    assertTrue(items.getColumnsToDropNonNullability().contains("ANOTHER_COL"));
    assertTrue(items.hasDataForSchemaEvolution());
  }

  @Test
  public void testMapEmptyValidationResult() {
    ValidationResult result =
        ValidationResult.structuralError(
            Collections.emptySet(), Collections.emptySet(), Collections.emptySet());

    SchemaEvolutionTargetItems items =
        ValidationResultMapper.mapToSchemaEvolutionItems(result, "MY_TABLE");

    assertFalse(items.hasDataForSchemaEvolution());
  }

  // ================ TableSchema Tests ================

  @Test
  public void testTableSchemaWrapper() {
    Map<String, ColumnInfos> columnInfos = new HashMap<>();
    columnInfos.put("COL1", new ColumnInfos("VARCHAR"));
    columnInfos.put("COL2", new ColumnInfos("INT"));

    TableSchema schema = new TableSchema(columnInfos);
    assertEquals(2, schema.getColumnInfos().size());
    assertEquals("VARCHAR", schema.getColumnInfos().get("COL1").getColumnType());
  }

  // ================ TableSchemaResolver Tests ================

  @Test
  public void testResolveSchemaFromSchemafulRecord() {
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("name", Schema.STRING_SCHEMA)
            .field("age", Schema.INT32_SCHEMA)
            .field("active", Schema.BOOLEAN_SCHEMA)
            .build();

    Struct value = new Struct(valueSchema);
    value.put("name", "Alice");
    value.put("age", 30);
    value.put("active", true);

    SinkRecord record = new SinkRecord("topic", 0, null, null, valueSchema, value, 0);

    TableSchemaResolver resolver = new TableSchemaResolver();
    TableSchema result =
        resolver.resolveTableSchemaFromRecord(record, Arrays.asList("\"NAME\"", "\"AGE\""));

    assertFalse(result.getColumnInfos().isEmpty());
  }

  @Test
  public void testResolveSchemaFromSchemalessRecord() {
    Map<String, Object> value = new HashMap<>();
    value.put("name", "Alice");
    value.put("count", 42);
    value.put("active", true);

    SinkRecord record = new SinkRecord("topic", 0, null, null, null, value, 0);

    TableSchemaResolver resolver = new TableSchemaResolver();
    TableSchema result =
        resolver.resolveTableSchemaFromRecord(record, Arrays.asList("\"NAME\"", "\"COUNT\""));

    assertFalse(result.getColumnInfos().isEmpty());
  }

  @Test
  public void testResolveSchemaEmptyColumnsToInclude() {
    SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 0);

    TableSchemaResolver resolver = new TableSchemaResolver();
    TableSchema result = resolver.resolveTableSchemaFromRecord(record, Collections.emptyList());

    assertTrue(result.getColumnInfos().isEmpty());
  }

  @Test
  public void testResolveSchemaNull() {
    SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 0);

    TableSchemaResolver resolver = new TableSchemaResolver();
    TableSchema result = resolver.resolveTableSchemaFromRecord(record, null);

    assertTrue(result.getColumnInfos().isEmpty());
  }
}
