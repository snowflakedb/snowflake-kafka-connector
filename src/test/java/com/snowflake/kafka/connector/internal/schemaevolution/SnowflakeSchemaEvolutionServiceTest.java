/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 *
 * Tests for schema evolution service and DDL execution (Commit 6).
 */

package com.snowflake.kafka.connector.internal.schemaevolution;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.snowflake.kafka.connector.internal.SnowflakeConnectionService;
import java.util.*;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

/** Tests for SnowflakeSchemaEvolutionService */
public class SnowflakeSchemaEvolutionServiceTest {

  private SnowflakeConnectionService mockConn;
  private SnowflakeSchemaEvolutionService service;

  @BeforeEach
  public void setUp() {
    mockConn = mock(SnowflakeConnectionService.class);
    service = new SnowflakeSchemaEvolutionService(mockConn);
  }

  @Test
  public void testEvolveSchemaAddColumns() {
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("name", Schema.STRING_SCHEMA)
            .field("new_col", Schema.INT32_SCHEMA)
            .build();

    Struct value = new Struct(valueSchema);
    value.put("name", "Alice");
    value.put("new_col", 42);

    SinkRecord record = new SinkRecord("topic", 0, null, null, valueSchema, value, 0);

    SchemaEvolutionTargetItems items =
        new SchemaEvolutionTargetItems(
            "test_table", Collections.emptySet(), new HashSet<>(Arrays.asList("\"NEW_COL\"")));

    service.evolveSchemaIfNeeded(items, record);

    verify(mockConn).appendColumnsToTable(eq("test_table"), anyMap());
    verify(mockConn, never()).alterNonNullableColumns(anyString(), anyList());
  }

  @Test
  public void testEvolveSchemaDropNotNull() {
    SinkRecord record = new SinkRecord("topic", 0, null, null, null, new HashMap<>(), 0);

    SchemaEvolutionTargetItems items =
        new SchemaEvolutionTargetItems(
            "test_table", new HashSet<>(Arrays.asList("COL1", "COL2")), Collections.emptySet());

    service.evolveSchemaIfNeeded(items, record);

    ArgumentCaptor<List<String>> colsCaptor = ArgumentCaptor.forClass(List.class);
    verify(mockConn).alterNonNullableColumns(eq("test_table"), colsCaptor.capture());
    List<String> droppedCols = colsCaptor.getValue();
    assertEquals(2, droppedCols.size());
    assertTrue(droppedCols.contains("COL1"));
    assertTrue(droppedCols.contains("COL2"));
    verify(mockConn, never()).appendColumnsToTable(anyString(), anyMap());
  }

  @Test
  public void testEvolveSchemaNoDataSkipsExecution() {
    SinkRecord record = new SinkRecord("topic", 0, null, null, null, null, 0);

    SchemaEvolutionTargetItems items =
        new SchemaEvolutionTargetItems(
            "test_table", Collections.emptySet(), Collections.emptySet());

    service.evolveSchemaIfNeeded(items, record);

    verify(mockConn, never()).appendColumnsToTable(anyString(), anyMap());
    verify(mockConn, never()).alterNonNullableColumns(anyString(), anyList());
  }

  @Test
  public void testEvolveSchemaHandlesAddColumnFailure() {
    Schema valueSchema = SchemaBuilder.struct().field("col1", Schema.STRING_SCHEMA).build();

    Struct value = new Struct(valueSchema);
    value.put("col1", "test");

    SinkRecord record = new SinkRecord("topic", 0, null, null, valueSchema, value, 0);

    doThrow(
            new com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException(
                "race", "2001"))
        .when(mockConn)
        .appendColumnsToTable(anyString(), anyMap());

    SchemaEvolutionTargetItems items =
        new SchemaEvolutionTargetItems(
            "test_table", Collections.emptySet(), new HashSet<>(Arrays.asList("\"COL1\"")));

    assertDoesNotThrow(() -> service.evolveSchemaIfNeeded(items, record));
  }

  @Test
  public void testEvolveSchemaHandlesDropNotNullFailure() {
    SinkRecord record = new SinkRecord("topic", 0, null, null, null, new HashMap<>(), 0);

    doThrow(
            new com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException(
                "race", "2001"))
        .when(mockConn)
        .alterNonNullableColumns(anyString(), anyList());

    SchemaEvolutionTargetItems items =
        new SchemaEvolutionTargetItems(
            "test_table", new HashSet<>(Arrays.asList("COL1")), Collections.emptySet());

    assertDoesNotThrow(() -> service.evolveSchemaIfNeeded(items, record));
  }

  @Test
  public void testEvolveSchemaAddColumnsBeforeDropNotNull() {
    Schema valueSchema =
        SchemaBuilder.struct()
            .field("existing_col", Schema.STRING_SCHEMA)
            .field("new_col", Schema.INT32_SCHEMA)
            .build();

    Struct value = new Struct(valueSchema);
    value.put("existing_col", "hello");
    value.put("new_col", 99);

    SinkRecord record = new SinkRecord("topic", 0, null, null, valueSchema, value, 0);

    SchemaEvolutionTargetItems items =
        new SchemaEvolutionTargetItems(
            "test_table",
            new HashSet<>(Arrays.asList("EXISTING_COL")),
            new HashSet<>(Arrays.asList("\"NEW_COL\"")));

    service.evolveSchemaIfNeeded(items, record);

    InOrder inOrder = inOrder(mockConn);
    inOrder.verify(mockConn).appendColumnsToTable(eq("test_table"), anyMap());
    inOrder.verify(mockConn).alterNonNullableColumns(eq("test_table"), anyList());
  }
}
