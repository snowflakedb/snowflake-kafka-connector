/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 *
 * Tests for the validation integration layer (Commit 4).
 */

package com.snowflake.kafka.connector.internal.validation;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

/** Tests for RowValidator, ColumnSchema, and ValidationResult */
public class RowValidatorTest {

  // ================ ColumnSchema Tests ================

  @Test
  public void testColumnSchemaParseNumber() throws SQLException {
    ResultSet rs = mockDescribeTableRow("COL1", "NUMBER(38,0)", "Y");
    ColumnSchema schema = ColumnSchema.fromDescribeTableRow(rs);

    assertEquals("COL1", schema.getName());
    assertEquals(ColumnLogicalType.FIXED, schema.getLogicalType());
    assertEquals(ColumnPhysicalType.SB16, schema.getPhysicalType());
    assertTrue(schema.isNullable());
    assertEquals(38, schema.getPrecision());
    assertEquals(0, schema.getScale());
  }

  @Test
  public void testColumnSchemaParseVarchar() throws SQLException {
    ResultSet rs = mockDescribeTableRow("COL2", "VARCHAR(16777216)", "N");
    ColumnSchema schema = ColumnSchema.fromDescribeTableRow(rs);

    assertEquals("COL2", schema.getName());
    assertEquals(ColumnLogicalType.TEXT, schema.getLogicalType());
    assertEquals(ColumnPhysicalType.LOB, schema.getPhysicalType());
    assertFalse(schema.isNullable());
    assertEquals(16777216, schema.getLength());
    // byteLength capped at 16MB (SSv1 SDK limit), not 16777216 * 4 = 64MB
    assertEquals(16777216, schema.getByteLength());
  }

  @Test
  public void testColumnSchemaParseVarcharSmall() throws SQLException {
    // For small VARCHAR, byteLength = length * 4 (no capping needed)
    ResultSet rs = mockDescribeTableRow("COL3", "VARCHAR(1000)", "Y");
    ColumnSchema schema = ColumnSchema.fromDescribeTableRow(rs);

    assertEquals("COL3", schema.getName());
    assertEquals(ColumnLogicalType.TEXT, schema.getLogicalType());
    assertEquals(1000, schema.getLength());
    assertEquals(4000, schema.getByteLength()); // 1000 * 4, no capping
  }

  @Test
  public void testColumnSchemaParseTimestampNtz() throws SQLException {
    ResultSet rs = mockDescribeTableRow("COL3", "TIMESTAMP_NTZ(9)", "Y");
    ColumnSchema schema = ColumnSchema.fromDescribeTableRow(rs);

    assertEquals("COL3", schema.getName());
    assertEquals(ColumnLogicalType.TIMESTAMP_NTZ, schema.getLogicalType());
    assertEquals(ColumnPhysicalType.SB8, schema.getPhysicalType());
    assertEquals(9, schema.getScale());
  }

  @Test
  public void testColumnSchemaParseBinary() throws SQLException {
    ResultSet rs = mockDescribeTableRow("COL4", "BINARY(8388608)", "Y");
    ColumnSchema schema = ColumnSchema.fromDescribeTableRow(rs);

    assertEquals("COL4", schema.getName());
    assertEquals(ColumnLogicalType.BINARY, schema.getLogicalType());
    assertEquals(ColumnPhysicalType.BINARY, schema.getPhysicalType());
    assertEquals(8388608, schema.getByteLength());
  }

  @Test
  public void testColumnSchemaParseVariant() throws SQLException {
    ResultSet rs = mockDescribeTableRow("COL5", "VARIANT", "Y");
    ColumnSchema schema = ColumnSchema.fromDescribeTableRow(rs);

    assertEquals("COL5", schema.getName());
    assertEquals(ColumnLogicalType.VARIANT, schema.getLogicalType());
    assertEquals(ColumnPhysicalType.LOB, schema.getPhysicalType());
  }

  @Test
  public void testColumnSchemaParseArray() throws SQLException {
    ResultSet rs = mockDescribeTableRow("COL6", "ARRAY", "Y");
    ColumnSchema schema = ColumnSchema.fromDescribeTableRow(rs);

    assertEquals(ColumnLogicalType.ARRAY, schema.getLogicalType());
    assertEquals(ColumnPhysicalType.LOB, schema.getPhysicalType());
  }

  @Test
  public void testColumnSchemaParseBoolean() throws SQLException {
    ResultSet rs = mockDescribeTableRow("COL7", "BOOLEAN", "Y");
    ColumnSchema schema = ColumnSchema.fromDescribeTableRow(rs);

    assertEquals(ColumnLogicalType.BOOLEAN, schema.getLogicalType());
    assertEquals(ColumnPhysicalType.SB1, schema.getPhysicalType());
  }

  @Test
  public void testColumnSchemaParseUnknownType() throws SQLException {
    ResultSet rs = mockDescribeTableRow("COL8", "GEOGRAPHY", "Y");
    ColumnSchema schema = ColumnSchema.fromDescribeTableRow(rs);

    assertNull(schema.getLogicalType()); // Unknown types return null
    assertNull(schema.getPhysicalType());
  }

  // ================ ValidationResult Tests ================

  @Test
  public void testValidationResultValid() {
    ValidationResult result = ValidationResult.valid();

    assertTrue(result.isValid());
    assertFalse(result.hasTypeError());
    assertFalse(result.hasStructuralError());
    assertFalse(result.needsSchemaEvolution());
  }

  @Test
  public void testValidationResultTypeError() {
    ValidationResult result = ValidationResult.typeError("COL1", "Invalid type");

    assertFalse(result.isValid());
    assertTrue(result.hasTypeError());
    assertFalse(result.hasStructuralError());
    assertEquals("COL1", result.getColumnName());
    assertEquals("Invalid type", result.getValueError());
    assertEquals("type_error", result.getErrorType());
  }

  @Test
  public void testValidationResultStructuralError() {
    Set<String> extraCols = new HashSet<>(Arrays.asList("EXTRA1", "EXTRA2"));
    Set<String> missingNotNull = new HashSet<>(Arrays.asList("REQUIRED1"));
    Set<String> nullNotNull = new HashSet<>(Arrays.asList("COL2"));

    ValidationResult result =
        ValidationResult.structuralError(extraCols, missingNotNull, nullNotNull);

    assertFalse(result.isValid());
    assertFalse(result.hasTypeError());
    assertTrue(result.hasStructuralError());
    assertTrue(result.needsSchemaEvolution());
    assertEquals(2, result.getExtraColNames().size());
    assertEquals(1, result.getMissingNotNullColNames().size());
    assertEquals(1, result.getNullValueForNotNullColNames().size());
    assertEquals("structural_error", result.getErrorType());
  }

  @Test
  public void testValidationResultEmptyStructuralError() {
    ValidationResult result =
        ValidationResult.structuralError(
            Collections.emptySet(), Collections.emptySet(), Collections.emptySet());

    assertFalse(result.isValid());
    assertTrue(result.hasStructuralError());
    assertFalse(result.needsSchemaEvolution()); // No actual errors
  }

  // ================ RowValidator Tests ================

  @Test
  public void testValidateRowValid() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("COL1", createColumnSchema("COL1", ColumnLogicalType.TEXT, true, null, null, 100));
    schema.put("COL2", createColumnSchema("COL2", ColumnLogicalType.FIXED, true, 38, 0, null));

    RowValidator validator = new RowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("COL1", "test value");
    row.put("COL2", 123);

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid());
  }

  @Test
  public void testValidateRowExtraColumn() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("COL1", createColumnSchema("COL1", ColumnLogicalType.TEXT, true, null, null, 100));

    RowValidator validator = new RowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("COL1", "test value");
    row.put("COL2", "extra column"); // Extra column not in schema

    ValidationResult result = validator.validateRow(row);
    assertFalse(result.isValid());
    assertTrue(result.hasStructuralError());
    assertTrue(result.getExtraColNames().contains("COL2"));
  }

  @Test
  public void testValidateRowMissingNotNull() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "COL1",
        createColumnSchema("COL1", ColumnLogicalType.TEXT, false, null, null, 100)); // NOT NULL

    RowValidator validator = new RowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    // COL1 is missing

    ValidationResult result = validator.validateRow(row);
    assertFalse(result.isValid());
    assertTrue(result.hasStructuralError());
    assertTrue(result.getMissingNotNullColNames().contains("COL1"));
  }

  @Test
  public void testValidateRowNullInNotNull() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "COL1",
        createColumnSchema("COL1", ColumnLogicalType.TEXT, false, null, null, 100)); // NOT NULL

    RowValidator validator = new RowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("COL1", null); // Null value in NOT NULL column

    ValidationResult result = validator.validateRow(row);
    assertFalse(result.isValid());
    assertTrue(result.hasStructuralError());
    assertTrue(result.getNullValueForNotNullColNames().contains("COL1"));
  }

  @Test
  public void testValidateRowInvalidType() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("COL1", createColumnSchema("COL1", ColumnLogicalType.FIXED, true, 38, 0, null));

    RowValidator validator = new RowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("COL1", "not a number"); // String in numeric column

    ValidationResult result = validator.validateRow(row);
    assertFalse(result.isValid());
    assertTrue(result.hasTypeError());
    assertEquals("COL1", result.getColumnName());
    assertNotNull(result.getValueError());
  }

  @Test
  public void testValidateRowQuotedColumnName() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "COL NAME", createColumnSchema("COL NAME", ColumnLogicalType.TEXT, true, null, null, 100));

    RowValidator validator = new RowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("\"COL NAME\"", "test value"); // Quoted column name

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid());
  }

  @Test
  public void testValidateSchemaUnsupportedType() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    ColumnSchema unknownCol =
        createColumnSchema("COL1", null, true, null, null, null); // null logicalType
    schema.put("COL1", unknownCol);

    assertThrows(SFExceptionValidation.class, () -> RowValidator.validateSchema(schema));
  }

  @Test
  public void testValidateSchemaCollatedColumn() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    ColumnSchema collatedCol =
        new ColumnSchema(
            "COL1",
            ColumnLogicalType.TEXT,
            ColumnPhysicalType.LOB,
            true,
            null,
            null,
            100,
            400,
            "en-ci"); // Collated column
    schema.put("COL1", collatedCol);

    assertThrows(SFExceptionValidation.class, () -> RowValidator.validateSchema(schema));
  }

  @Test
  public void testValidateSchemaValid() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("COL1", createColumnSchema("COL1", ColumnLogicalType.TEXT, true, null, null, 100));
    schema.put("COL2", createColumnSchema("COL2", ColumnLogicalType.FIXED, true, 38, 0, null));
    schema.put(
        "COL3", createColumnSchema("COL3", ColumnLogicalType.VARIANT, true, null, null, null));

    assertDoesNotThrow(() -> RowValidator.validateSchema(schema));
  }

  @Test
  public void testValidateRowEmptyColumnName() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("COL1", createColumnSchema("COL1", ColumnLogicalType.TEXT, true, null, null, 100));

    RowValidator validator = new RowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("", "value"); // Empty column name
    row.put("COL1", "test value");

    // Empty column name should be caught - it becomes empty after unquoting
    ValidationResult result = validator.validateRow(row);
    assertFalse(result.isValid());
    // Empty column will be treated as extra column or skipped with warning
  }

  @Test
  public void testValidateRowWhitespaceColumnName() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("COL1", createColumnSchema("COL1", ColumnLogicalType.TEXT, true, null, null, 100));

    RowValidator validator = new RowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("   ", "value"); // Whitespace-only column name
    row.put("\t\n", "value2"); // Control characters
    row.put("COL1", "test value");

    // Whitespace column names should be caught
    ValidationResult result = validator.validateRow(row);
    assertFalse(result.isValid());
    // Whitespace columns will be treated as extra columns or skipped with warning
  }

  // ================ Code Review Fix Tests ================

  /**
   * Test that structured OBJECT types are rejected (Issue #1 from code review).
   * SSv1 SDK doesn't support structured OBJECT types like OBJECT(a INT, b TEXT).
   */
  @Test
  public void testStructuredObjectTypeRejected() throws SQLException {
    ResultSet rs = mockDescribeTableRow("COL1", "OBJECT(a NUMBER(38,0), b VARCHAR(16777216))", "Y");

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> ColumnSchema.fromDescribeTableRow(rs));

    assertTrue(exception.getMessage().contains("Structured OBJECT types are not supported"));
    assertTrue(exception.getMessage().contains("unstructured OBJECT"));
  }

  /**
   * Test that structured ARRAY types are rejected (Issue #1 from code review).
   * SSv1 SDK doesn't support structured ARRAY types like ARRAY(INT).
   */
  @Test
  public void testStructuredArrayTypeRejected() throws SQLException {
    ResultSet rs = mockDescribeTableRow("COL1", "ARRAY(INT)", "Y");

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> ColumnSchema.fromDescribeTableRow(rs));

    assertTrue(exception.getMessage().contains("Structured ARRAY types are not supported"));
    assertTrue(exception.getMessage().contains("unstructured ARRAY"));
  }

  /**
   * Test that unstructured OBJECT types are accepted.
   */
  @Test
  public void testUnstructuredObjectTypeAccepted() throws SQLException {
    ResultSet rs = mockDescribeTableRow("COL1", "OBJECT", "Y");
    ColumnSchema schema = ColumnSchema.fromDescribeTableRow(rs);

    assertEquals(ColumnLogicalType.OBJECT, schema.getLogicalType());
    assertEquals(ColumnPhysicalType.LOB, schema.getPhysicalType());
  }

  /**
   * Test that unstructured ARRAY types are accepted.
   */
  @Test
  public void testUnstructuredArrayTypeAccepted() throws SQLException {
    ResultSet rs = mockDescribeTableRow("COL1", "ARRAY", "Y");
    ColumnSchema schema = ColumnSchema.fromDescribeTableRow(rs);

    assertEquals(ColumnLogicalType.ARRAY, schema.getLogicalType());
    assertEquals(ColumnPhysicalType.LOB, schema.getPhysicalType());
  }

  /**
   * Test that nested type parsing uses lastIndexOf for correct parameter extraction (Issue #1).
   * Without lastIndexOf, "OBJECT(a NUMBER(38,0), b TEXT)" would incorrectly extract params as
   * "a NUMBER(38,0" instead of the full parameter list.
   */
  @Test
  public void testNestedTypeParsingWithLastIndexOf() throws SQLException {
    // This should fail with structured type error, not parsing error
    ResultSet rs = mockDescribeTableRow("COL1", "OBJECT(a NUMBER(38,0), b VARCHAR(100))", "Y");

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> ColumnSchema.fromDescribeTableRow(rs));

    // Should get structured type error, not malformed type string error
    assertTrue(exception.getMessage().contains("Structured OBJECT types are not supported"));
    assertFalse(exception.getMessage().contains("Malformed type string"));
  }

  /**
   * Test that missing NOT NULL columns trigger schema evolution (Issue #3 from code review).
   * KC v3 treated missing and null NOT NULL columns identically - both drop NOT NULL.
   */
  @Test
  public void testMissingNotNullColumnTriggersSchemaEvolution() {
    Map<String, ColumnSchema> schemaMap = new HashMap<>();
    schemaMap.put(
        "COL1",
        createColumnSchema("COL1", ColumnLogicalType.FIXED, false, 38, 0, null)); // NOT NULL

    RowValidator validator = new RowValidator(schemaMap);

    // Missing COL1 entirely (not in row)
    Map<String, Object> row = new HashMap<>();
    // Empty row - missing NOT NULL column

    ValidationResult result = validator.validateRow(row);

    assertFalse(result.isValid());
    assertTrue(result.hasStructuralError());
    assertEquals(1, result.getMissingNotNullColNames().size());
    assertTrue(result.getMissingNotNullColNames().contains("COL1"));

    // Should trigger schema evolution (matches KC v3 behavior)
    assertTrue(result.needsSchemaEvolution());
    assertFalse(result.hasUnresolvableError()); // NOT unresolvable anymore
  }

  /**
   * Test that null NOT NULL columns trigger schema evolution (Issue #3 from code review).
   * This was already working, but verify it still works after fix.
   */
  @Test
  public void testNullNotNullColumnTriggersSchemaEvolution() {
    Map<String, ColumnSchema> schemaMap = new HashMap<>();
    schemaMap.put(
        "COL1",
        createColumnSchema("COL1", ColumnLogicalType.FIXED, false, 38, 0, null)); // NOT NULL

    RowValidator validator = new RowValidator(schemaMap);

    // COL1 present but null
    Map<String, Object> row = new HashMap<>();
    row.put("COL1", null);

    ValidationResult result = validator.validateRow(row);

    assertFalse(result.isValid());
    assertTrue(result.hasStructuralError());
    assertEquals(1, result.getNullValueForNotNullColNames().size());
    assertTrue(result.getNullValueForNotNullColNames().contains("COL1"));

    // Should trigger schema evolution
    assertTrue(result.needsSchemaEvolution());
    assertFalse(result.hasUnresolvableError());
  }

  // ================ Helper Methods ================

  private ResultSet mockDescribeTableRow(String name, String type, String nullable)
      throws SQLException {
    ResultSet rs = Mockito.mock(ResultSet.class);
    Mockito.when(rs.getString("name")).thenReturn(name);
    Mockito.when(rs.getString("type")).thenReturn(type);
    Mockito.when(rs.getString("null?")).thenReturn(nullable);
    return rs;
  }

  private ColumnSchema createColumnSchema(
      String name,
      ColumnLogicalType logicalType,
      boolean nullable,
      Integer precision,
      Integer scale,
      Integer length) {
    ColumnPhysicalType physicalType =
        logicalType != null
            ? (logicalType == ColumnLogicalType.FIXED
                ? ColumnPhysicalType.SB16
                : logicalType == ColumnLogicalType.TEXT
                    ? ColumnPhysicalType.LOB
                    : logicalType == ColumnLogicalType.BOOLEAN
                        ? ColumnPhysicalType.SB1
                        : ColumnPhysicalType.LOB)
            : null;

    Integer byteLength = length != null ? length * 4 : null;

    return new ColumnSchema(
        name, logicalType, physicalType, nullable, precision, scale, length, byteLength, null);
  }
}
