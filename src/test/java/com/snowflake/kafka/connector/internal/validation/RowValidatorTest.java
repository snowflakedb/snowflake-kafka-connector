/*
 * Copyright (c) 2026 Snowflake Computing Inc. All rights reserved.
 *
 * Tests for the validation integration layer (Commit 4).
 */

package com.snowflake.kafka.connector.internal.validation;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalTime;
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

    RowValidator validator = newRowValidator(schema);

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

    RowValidator validator = newRowValidator(schema);

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

    RowValidator validator = newRowValidator(schema);

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

    RowValidator validator = newRowValidator(schema);

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

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("COL1", "not a number"); // String in numeric column

    ValidationResult result = validator.validateRow(row);
    assertFalse(result.isValid());
    assertTrue(result.hasTypeError());
    assertEquals("COL1", result.getColumnName());
    assertNotNull(result.getValueError());
  }

  @Test
  public void testValidateRowNumberOverflowRejected() {
    // SNOW-3675649: a value whose integer part exceeds the column's (precision - scale) digits must
    // be rejected client-side. Previously it passed validation and failed server-side ("Failed to
    // cast variant value", error 100071), silently dropping the record.
    // NUMBER(38,20) allows 38 - 20 = 18 integer digits; this value has 30.
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("COL1", createColumnSchema("COL1", ColumnLogicalType.FIXED, true, 38, 20, null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("COL1", "999999999999999999999999999999.999999999");

    ValidationResult result = validator.validateRow(row);
    assertFalse(result.isValid());
    assertTrue(result.hasTypeError());
    assertEquals("COL1", result.getColumnName());
    assertNotNull(result.getValueError());
  }

  @Test
  public void testValidateRowNumberWithinRangeAccepted() {
    // A value that fits within NUMBER(38,20) (18 integer digits, 9 fractional) must pass.
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("COL1", createColumnSchema("COL1", ColumnLogicalType.FIXED, true, 38, 20, null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("COL1", "123456789012345678.123456789");

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid());
  }

  @Test
  public void testValidateRowNumberRoundsUpToOverflow() {
    // Mirror the server: excess fractional digits are rounded to the column scale before the range
    // check. NUMBER(2,1) allows 1 integer digit; 9.96 rounds (HALF_UP) to 10.0, which overflows.
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("COL1", createColumnSchema("COL1", ColumnLogicalType.FIXED, true, 2, 1, null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("COL1", "9.96");

    ValidationResult result = validator.validateRow(row);
    assertFalse(result.isValid());
    assertTrue(result.hasTypeError());
    assertEquals("COL1", result.getColumnName());
  }

  @Test
  public void testValidateRowNumberExcessFractionalDigitsAccepted() {
    // Excess fractional digits alone (without integer overflow) are rounded by the server, not
    // rejected. NUMBER(38,2) with extra fractional precision must pass client-side validation.
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("COL1", createColumnSchema("COL1", ColumnLogicalType.FIXED, true, 38, 2, null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("COL1", "123.456789");

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid());
  }

  @Test
  public void testValidateRowNumberNegativeOverflowRejected() {
    // Sign must not matter: a large-magnitude negative value also overflows.
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("COL1", createColumnSchema("COL1", ColumnLogicalType.FIXED, true, 38, 20, null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("COL1", "-999999999999999999999999999999.999999999");

    ValidationResult result = validator.validateRow(row);
    assertFalse(result.isValid());
    assertTrue(result.hasTypeError());
  }

  @Test
  public void testValidateRowNumberScientificNotationOverflowRejected() {
    // Scientific notation must be range-checked after parsing: 1E30 overflows NUMBER(38,20).
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("COL1", createColumnSchema("COL1", ColumnLogicalType.FIXED, true, 38, 20, null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("COL1", "1E30");

    ValidationResult result = validator.validateRow(row);
    assertFalse(result.isValid());
    assertTrue(result.hasTypeError());
  }

  @Test
  public void testValidateRowNumberNativeLongOverflowRejected() {
    // The range check must apply to native numeric inputs too, not just strings. Long.MAX_VALUE
    // (19 digits) overflows NUMBER(38,20), which permits 18 integer digits.
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("COL1", createColumnSchema("COL1", ColumnLogicalType.FIXED, true, 38, 20, null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("COL1", Long.MAX_VALUE); // 9223372036854775807 — 19 digits

    ValidationResult result = validator.validateRow(row);
    assertFalse(result.isValid());
    assertTrue(result.hasTypeError());
  }

  @Test
  public void testValidateRowNumberBoundaryMaxAccepted() {
    // NUMBER(5,0) permits magnitudes < 10^5. 99999 is the largest in-range value.
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("COL1", createColumnSchema("COL1", ColumnLogicalType.FIXED, true, 5, 0, null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("COL1", "99999");

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid());
  }

  @Test
  public void testValidateRowNumberBoundaryOverflowRejected() {
    // NUMBER(5,0): 100000 (= 10^5) is the smallest value that overflows.
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("COL1", createColumnSchema("COL1", ColumnLogicalType.FIXED, true, 5, 0, null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("COL1", "100000");

    ValidationResult result = validator.validateRow(row);
    assertFalse(result.isValid());
    assertTrue(result.hasTypeError());
  }

  @Test
  public void testValidateRowNumberFullPrecisionAccepted() {
    // Guard against over-rejection: the full 38-digit precision (38 nines) fits NUMBER(38,0).
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("COL1", createColumnSchema("COL1", ColumnLogicalType.FIXED, true, 38, 0, null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("COL1", "99999999999999999999999999999999999999"); // 38 nines

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid());
  }

  @Test
  public void testValidateRowVarcharExceedsLengthRejected() {
    // Parallel server-constraint check: a plain string longer than VARCHAR(N) must be rejected
    // client-side, not silently truncated/dropped server-side.
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("COL1", createColumnSchema("COL1", ColumnLogicalType.TEXT, true, null, null, 5));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("COL1", "abcdef"); // 6 characters, exceeds VARCHAR(5)

    ValidationResult result = validator.validateRow(row);
    assertFalse(result.isValid());
    assertTrue(result.hasTypeError());
  }

  @Test
  public void testValidateRowVarcharExactLengthAccepted() {
    // A string exactly at the VARCHAR(N) limit must be accepted (boundary guard).
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("COL1", createColumnSchema("COL1", ColumnLogicalType.TEXT, true, null, null, 5));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("COL1", "abcde"); // exactly 5 characters

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid());
  }

  @Test
  public void testValidateRowMatchingColumnName() {
    // Column names are expected to be already normalized by the caller (SnowflakeSinkRecord).
    // RowValidator just does direct comparison against raw column names.
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "COL NAME", createColumnSchema("COL NAME", ColumnLogicalType.TEXT, true, null, null, 100));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("COL NAME", "test value"); // Raw column name (already normalized)

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

    RowValidator validator = newRowValidator(schema);

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

    RowValidator validator = newRowValidator(schema);

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
   * Test that structured OBJECT types are rejected (Issue #1 from code review). SSv1 SDK doesn't
   * support structured OBJECT types like OBJECT(a INT, b TEXT).
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
   * Test that structured ARRAY types are rejected (Issue #1 from code review). SSv1 SDK doesn't
   * support structured ARRAY types like ARRAY(INT).
   */
  @Test
  public void testStructuredArrayTypeRejected() throws SQLException {
    ResultSet rs = mockDescribeTableRow("COL1", "ARRAY(INT)", "Y");

    IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> ColumnSchema.fromDescribeTableRow(rs));

    assertTrue(exception.getMessage().contains("Structured ARRAY types are not supported"));
    assertTrue(exception.getMessage().contains("unstructured ARRAY"));
  }

  /** Test that unstructured OBJECT types are accepted. */
  @Test
  public void testUnstructuredObjectTypeAccepted() throws SQLException {
    ResultSet rs = mockDescribeTableRow("COL1", "OBJECT", "Y");
    ColumnSchema schema = ColumnSchema.fromDescribeTableRow(rs);

    assertEquals(ColumnLogicalType.OBJECT, schema.getLogicalType());
    assertEquals(ColumnPhysicalType.LOB, schema.getPhysicalType());
  }

  /** Test that unstructured ARRAY types are accepted. */
  @Test
  public void testUnstructuredArrayTypeAccepted() throws SQLException {
    ResultSet rs = mockDescribeTableRow("COL1", "ARRAY", "Y");
    ColumnSchema schema = ColumnSchema.fromDescribeTableRow(rs);

    assertEquals(ColumnLogicalType.ARRAY, schema.getLogicalType());
    assertEquals(ColumnPhysicalType.LOB, schema.getPhysicalType());
  }

  /**
   * Test that nested type parsing uses lastIndexOf for correct parameter extraction (Issue #1).
   * Without lastIndexOf, "OBJECT(a NUMBER(38,0), b TEXT)" would incorrectly extract params as "a
   * NUMBER(38,0" instead of the full parameter list.
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
   * Test that missing NOT NULL columns trigger schema evolution (Issue #3 from code review). KC v3
   * treated missing and null NOT NULL columns identically - both drop NOT NULL.
   */
  @Test
  public void testMissingNotNullColumnTriggersSchemaEvolution() {
    Map<String, ColumnSchema> schemaMap = new HashMap<>();
    schemaMap.put(
        "COL1",
        createColumnSchema("COL1", ColumnLogicalType.FIXED, false, 38, 0, null)); // NOT NULL

    RowValidator validator = newRowValidator(schemaMap);

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
   * Test that null NOT NULL columns trigger schema evolution (Issue #3 from code review). This was
   * already working, but verify it still works after fix.
   */
  @Test
  public void testNullNotNullColumnTriggersSchemaEvolution() {
    Map<String, ColumnSchema> schemaMap = new HashMap<>();
    schemaMap.put(
        "COL1",
        createColumnSchema("COL1", ColumnLogicalType.FIXED, false, 38, 0, null)); // NOT NULL

    RowValidator validator = newRowValidator(schemaMap);

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

  /**
   * Test that null values in nullable columns are valid (Graphite bot feedback). When a nullable
   * column has a null value, it should pass validation.
   */
  @Test
  public void testNullValueInNullableColumnIsValid() {
    Map<String, ColumnSchema> schemaMap = new HashMap<>();
    schemaMap.put(
        "COL1", createColumnSchema("COL1", ColumnLogicalType.FIXED, true, 38, 0, null)); // NULLABLE
    schemaMap.put(
        "COL2",
        createColumnSchema("COL2", ColumnLogicalType.TEXT, true, null, null, 100)); // NULLABLE

    RowValidator validator = newRowValidator(schemaMap);

    // Both columns present with null values (valid for nullable columns)
    Map<String, Object> row = new HashMap<>();
    row.put("COL1", null);
    row.put("COL2", null);

    ValidationResult result = validator.validateRow(row);

    // Should be valid - null is allowed for nullable columns
    assertTrue(result.isValid());
    assertFalse(result.hasStructuralError());
    assertFalse(result.hasTypeError());
  }

  /** Test that nullable column with actual value also validates correctly. */
  @Test
  public void testNullableColumnWithValue() {
    Map<String, ColumnSchema> schemaMap = new HashMap<>();
    schemaMap.put(
        "COL1", createColumnSchema("COL1", ColumnLogicalType.FIXED, true, 38, 0, null)); // NULLABLE

    RowValidator validator = newRowValidator(schemaMap);

    // Nullable column with actual value
    Map<String, Object> row = new HashMap<>();
    row.put("COL1", 42);

    ValidationResult result = validator.validateRow(row);

    // Should be valid
    assertTrue(result.isValid());
  }

  /**
   * Test that large VARCHAR lengths don't cause integer overflow (Graphite security issue). Without
   * long cast, info.length * 4 can overflow for corrupted/malformed lengths.
   */
  @Test
  public void testVarcharLargeValueNoOverflow() throws SQLException {
    // Test with a value that would overflow if multiplied as int: Integer.MAX_VALUE / 2
    // This simulates corrupted DESCRIBE TABLE result
    int largeLength = Integer.MAX_VALUE / 2; // ~1 billion
    ResultSet rs = mockDescribeTableRow("COL1", "VARCHAR(" + largeLength + ")", "Y");

    ColumnSchema schema = ColumnSchema.fromDescribeTableRow(rs);

    // Should not overflow - byteLength should be capped at MAX_LOB_SIZE_BYTES (16MB)
    assertEquals(16777216, schema.getByteLength()); // 16MB cap
    assertEquals(largeLength, schema.getLength()); // Original length preserved
  }

  // ================ Server-Filled Column Tests (FR7) ================

  @Test
  public void testValidateRow_missingIdentityColumn_passes() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "ID",
        new ColumnSchema(
            "ID",
            ColumnLogicalType.FIXED,
            ColumnPhysicalType.SB16,
            false,
            38,
            0,
            null,
            null,
            null,
            false,
            true)); // NOT NULL, autoincrement=true
    schema.put("DATA", createColumnSchema("DATA", ColumnLogicalType.TEXT, true, null, null, 100));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("DATA", "hello"); // ID is missing — server fills it

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid(), "Record should be valid when identity column is omitted");
  }

  @Test
  public void testValidateRow_missingDefaultNotNullColumn_passes() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("DATA", createColumnSchema("DATA", ColumnLogicalType.TEXT, true, null, null, 100));
    schema.put(
        "CREATED_AT",
        new ColumnSchema(
            "CREATED_AT",
            ColumnLogicalType.TIMESTAMP_NTZ,
            ColumnPhysicalType.SB8,
            false,
            null,
            9,
            null,
            null,
            null,
            true,
            false)); // NOT NULL, hasDefault=true

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("DATA", "hello"); // CREATED_AT is missing — server fills it

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid(), "Record should be valid when default NOT NULL column is omitted");
  }

  @Test
  public void testValidateRow_missingRegularNotNullColumn_stillFails() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "REQUIRED",
        new ColumnSchema(
            "REQUIRED",
            ColumnLogicalType.TEXT,
            ColumnPhysicalType.LOB,
            false,
            null,
            null,
            100,
            400,
            null,
            false,
            false)); // NOT NULL, no default, no autoincrement

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    // REQUIRED is missing — no server default, should fail

    ValidationResult result = validator.validateRow(row);
    assertFalse(
        result.isValid(), "Record should be invalid when regular NOT NULL column is missing");
    assertTrue(result.getMissingNotNullColNames().contains("REQUIRED"));
  }

  @Test
  public void testValidateRow_mixedServerFilledAndRegularColumns() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "ID",
        new ColumnSchema(
            "ID",
            ColumnLogicalType.FIXED,
            ColumnPhysicalType.SB16,
            false,
            38,
            0,
            null,
            null,
            null,
            false,
            true)); // autoincrement
    schema.put("DATA", createColumnSchema("DATA", ColumnLogicalType.TEXT, true, null, null, 100));
    schema.put(
        "CREATED_AT",
        new ColumnSchema(
            "CREATED_AT",
            ColumnLogicalType.TIMESTAMP_NTZ,
            ColumnPhysicalType.SB8,
            false,
            null,
            9,
            null,
            null,
            null,
            true,
            false)); // default
    schema.put(
        "STATUS",
        new ColumnSchema(
            "STATUS",
            ColumnLogicalType.FIXED,
            ColumnPhysicalType.SB16,
            false,
            38,
            0,
            null,
            null,
            null,
            true,
            false)); // default

    RowValidator validator = newRowValidator(schema);

    // Only DATA provided — ID, CREATED_AT, STATUS are server-filled
    Map<String, Object> row = new HashMap<>();
    row.put("DATA", "hello");

    ValidationResult result = validator.validateRow(row);
    assertTrue(
        result.isValid(),
        "Record should be valid when only server-filled NOT NULL columns are missing");
  }

  @Test
  public void testValidateRow_explicitValueForIdentityColumn_passes() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "ID",
        new ColumnSchema(
            "ID",
            ColumnLogicalType.FIXED,
            ColumnPhysicalType.SB16,
            false,
            38,
            0,
            null,
            null,
            null,
            false,
            true)); // autoincrement
    schema.put("DATA", createColumnSchema("DATA", ColumnLogicalType.TEXT, true, null, null, 100));

    RowValidator validator = newRowValidator(schema);

    // User explicitly provides a value for the identity column — should still be accepted
    Map<String, Object> row = new HashMap<>();
    row.put("ID", 42);
    row.put("DATA", "hello");

    ValidationResult result = validator.validateRow(row);
    assertTrue(
        result.isValid(), "Record should be valid when identity column is explicitly provided");
  }

  @Test
  public void testColumnSchema_isServerFilled() {
    ColumnSchema autoincCol =
        new ColumnSchema(
            "ID",
            ColumnLogicalType.FIXED,
            ColumnPhysicalType.SB16,
            false,
            38,
            0,
            null,
            null,
            null,
            false,
            true);
    assertTrue(autoincCol.isServerFilled());
    assertTrue(autoincCol.isAutoincrement());
    assertFalse(autoincCol.hasDefault());

    ColumnSchema defaultCol =
        new ColumnSchema(
            "TS",
            ColumnLogicalType.TIMESTAMP_NTZ,
            ColumnPhysicalType.SB8,
            false,
            null,
            9,
            null,
            null,
            null,
            true,
            false);
    assertTrue(defaultCol.isServerFilled());
    assertFalse(defaultCol.isAutoincrement());
    assertTrue(defaultCol.hasDefault());

    ColumnSchema regularCol =
        createColumnSchema("REG", ColumnLogicalType.TEXT, false, null, null, 100);
    assertFalse(regularCol.isServerFilled());
    assertFalse(regularCol.isAutoincrement());
    assertFalse(regularCol.hasDefault());
  }

  /** Hex string for a BINARY column is converted to byte[] in-place during validation. */
  @Test
  public void testValidateRowBinaryHexStringConvertedToByteArray() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "BIN_COL",
        new ColumnSchema(
            "BIN_COL",
            ColumnLogicalType.BINARY,
            ColumnPhysicalType.BINARY,
            true,
            null,
            null,
            null,
            8388608,
            null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("BIN_COL", "FFFFFFFF");

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid());
    // Row map must now contain byte[] instead of the original hex string
    assertInstanceOf(byte[].class, row.get("BIN_COL"));
    assertArrayEquals(
        new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF},
        (byte[]) row.get("BIN_COL"));
  }

  /** byte[] input for a BINARY column is preserved as-is. */
  @Test
  public void testValidateRowBinaryByteArrayPassthrough() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "BIN_COL",
        new ColumnSchema(
            "BIN_COL",
            ColumnLogicalType.BINARY,
            ColumnPhysicalType.BINARY,
            true,
            null,
            null,
            null,
            8388608,
            null));

    RowValidator validator = newRowValidator(schema);

    byte[] input = new byte[] {0x01, 0x02, 0x03};
    Map<String, Object> row = new HashMap<>();
    row.put("BIN_COL", input);

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid());
    assertInstanceOf(byte[].class, row.get("BIN_COL"));
    assertArrayEquals(input, (byte[]) row.get("BIN_COL"));
  }

  /** Empty hex string ("") for a BINARY column is decoded to byte[0]. */
  @Test
  public void testValidateRowBinaryEmptyHexString() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "BIN_COL",
        new ColumnSchema(
            "BIN_COL",
            ColumnLogicalType.BINARY,
            ColumnPhysicalType.BINARY,
            true,
            null,
            null,
            null,
            8388608,
            null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("BIN_COL", "");

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid());
    assertInstanceOf(byte[].class, row.get("BIN_COL"));
    assertArrayEquals(new byte[0], (byte[]) row.get("BIN_COL"));
  }

  /** Odd-length hex string for a BINARY column produces a type error. */
  @Test
  public void testValidateRowBinaryOddLengthHexStringFails() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "BIN_COL",
        new ColumnSchema(
            "BIN_COL",
            ColumnLogicalType.BINARY,
            ColumnPhysicalType.BINARY,
            true,
            null,
            null,
            null,
            8388608,
            null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("BIN_COL", "FFF");

    ValidationResult result = validator.validateRow(row);
    assertFalse(result.isValid());
    assertTrue(result.hasTypeError());
    assertEquals("BIN_COL", result.getColumnName());
  }

  /** Lowercase hex string for a BINARY column is decoded case-insensitively. */
  @Test
  public void testValidateRowBinaryLowercaseHexString() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "BIN_COL",
        new ColumnSchema(
            "BIN_COL",
            ColumnLogicalType.BINARY,
            ColumnPhysicalType.BINARY,
            true,
            null,
            null,
            null,
            8388608,
            null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("BIN_COL", "ffffffff");

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid());
    assertInstanceOf(byte[].class, row.get("BIN_COL"));
    assertArrayEquals(
        new byte[] {(byte) 0xFF, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF},
        (byte[]) row.get("BIN_COL"));
  }

  // ================ VARCHAR Map/List serialization Tests ================

  /**
   * Map sent to a VARCHAR column is serialized to JSON string, matching SSv1/SSv2 SDK behavior.
   * Both SDKs serialize complex objects via Jackson inside appendRow(); RowValidator must
   * replicate.
   */
  @Test
  public void testValidateRowVarcharMapSerializedToJson() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "STR_COL", createColumnSchema("STR_COL", ColumnLogicalType.TEXT, true, null, null, null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> inputMap = new LinkedHashMap<>();
    inputMap.put("key", "value");

    Map<String, Object> row = new HashMap<>();
    row.put("STR_COL", inputMap);

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid());
    assertEquals("{\"key\":\"value\"}", row.get("STR_COL"));
  }

  /** List sent to a VARCHAR column is serialized to JSON array string. */
  @Test
  public void testValidateRowVarcharListSerializedToJson() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "STR_COL", createColumnSchema("STR_COL", ColumnLogicalType.TEXT, true, null, null, null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("STR_COL", Arrays.asList(1, 2, 3));

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid());
    assertEquals("[1,2,3]", row.get("STR_COL"));
  }

  /** Nested Map sent to VARCHAR is serialized recursively. */
  @Test
  public void testValidateRowVarcharNestedMapSerializedToJson() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "STR_COL", createColumnSchema("STR_COL", ColumnLogicalType.TEXT, true, null, null, null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> nested = new LinkedHashMap<>();
    nested.put("b", 1);
    Map<String, Object> inputMap = new LinkedHashMap<>();
    inputMap.put("a", nested);

    Map<String, Object> row = new HashMap<>();
    row.put("STR_COL", inputMap);

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid());
    assertEquals("{\"a\":{\"b\":1}}", row.get("STR_COL"));
  }

  /** Map serialized to JSON that exceeds VARCHAR(N) length limit produces a type error. */
  @Test
  public void testValidateRowVarcharMapExceedsLengthLimit() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "STR_COL", createColumnSchema("STR_COL", ColumnLogicalType.TEXT, true, null, null, 5));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> inputMap = new LinkedHashMap<>();
    inputMap.put("key", "value"); // {"key":"value"} = 15 chars, exceeds 5

    Map<String, Object> row = new HashMap<>();
    row.put("STR_COL", inputMap);

    ValidationResult result = validator.validateRow(row);
    assertFalse(result.isValid());
    assertTrue(result.hasTypeError());
  }

  // ================ Boolean Normalization Tests ================

  /**
   * Integer 0/1 must be normalized to Boolean before reaching the SSv2 SDK. The SDK only accepts
   * Boolean for BOOLEAN columns — Integer inputs are silently dropped without this normalization.
   */
  @Test
  public void testValidateRowBooleanIntegerZeroNormalizedToFalse() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "BOOL_COL",
        createColumnSchema("BOOL_COL", ColumnLogicalType.BOOLEAN, true, null, null, null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("BOOL_COL", 0);

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid());
    assertEquals(Boolean.FALSE, row.get("BOOL_COL"));
  }

  @Test
  public void testValidateRowBooleanIntegerOneNormalizedToTrue() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "BOOL_COL",
        createColumnSchema("BOOL_COL", ColumnLogicalType.BOOLEAN, true, null, null, null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("BOOL_COL", 1);

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid());
    assertEquals(Boolean.TRUE, row.get("BOOL_COL"));
  }

  /** Native Boolean values must also be normalized (no-op in effect, but consistent). */
  @Test
  public void testValidateRowBooleanNativeBooleanPassthrough() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "BOOL_COL",
        createColumnSchema("BOOL_COL", ColumnLogicalType.BOOLEAN, true, null, null, null));

    RowValidator validator = newRowValidator(schema);

    for (Object input : Arrays.asList(Boolean.TRUE, Boolean.FALSE)) {
      Map<String, Object> row = new HashMap<>();
      row.put("BOOL_COL", input);
      ValidationResult result = validator.validateRow(row);
      assertTrue(result.isValid());
      assertInstanceOf(Boolean.class, row.get("BOOL_COL"));
      assertEquals(input, row.get("BOOL_COL"));
    }
  }

  /** String tokens are normalized to Boolean (previously accepted as String by SDK). */
  @Test
  public void testValidateRowBooleanStringTokensNormalizedToBoolean() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "BOOL_COL",
        createColumnSchema("BOOL_COL", ColumnLogicalType.BOOLEAN, true, null, null, null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> trueInputs = new LinkedHashMap<>();
    trueInputs.put("true", Boolean.TRUE);
    trueInputs.put("yes", Boolean.TRUE);
    trueInputs.put("on", Boolean.TRUE);

    Map<String, Object> falseInputs = new LinkedHashMap<>();
    falseInputs.put("false", Boolean.FALSE);
    falseInputs.put("no", Boolean.FALSE);
    falseInputs.put("off", Boolean.FALSE);

    for (Map.Entry<String, Object> entry : trueInputs.entrySet()) {
      Map<String, Object> row = new HashMap<>();
      row.put("BOOL_COL", entry.getKey());
      ValidationResult result = validator.validateRow(row);
      assertTrue(result.isValid(), "Expected valid for input: " + entry.getKey());
      assertEquals(entry.getValue(), row.get("BOOL_COL"), "Expected TRUE for: " + entry.getKey());
    }

    for (Map.Entry<String, Object> entry : falseInputs.entrySet()) {
      Map<String, Object> row = new HashMap<>();
      row.put("BOOL_COL", entry.getKey());
      ValidationResult result = validator.validateRow(row);
      assertTrue(result.isValid(), "Expected valid for input: " + entry.getKey());
      assertEquals(entry.getValue(), row.get("BOOL_COL"), "Expected FALSE for: " + entry.getKey());
    }
  }

  /** Invalid inputs for BOOLEAN still produce a type error. */
  @Test
  public void testValidateRowBooleanInvalidInputProducesTypeError() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "BOOL_COL",
        createColumnSchema("BOOL_COL", ColumnLogicalType.BOOLEAN, true, null, null, null));

    RowValidator validator = newRowValidator(schema);

    for (Object invalid : Arrays.asList(new HashMap<>(), new ArrayList<>(), "not_a_bool")) {
      Map<String, Object> row = new HashMap<>();
      row.put("BOOL_COL", invalid);
      ValidationResult result = validator.validateRow(row);
      assertFalse(result.isValid(), "Expected type error for input: " + invalid);
      assertTrue(result.hasTypeError(), "Expected type error for input: " + invalid);
      assertEquals("BOOL_COL", result.getColumnName());
    }
  }

  /**
   * Non-0/1 numeric values for BOOLEAN produce a type error. Although SSv1 SDK's
   * DataValidationUtil.validateAndParseBoolean accepts any Number directly, in KC v3 the record
   * mapper converts all values to Strings first — and SSv1's convertStringToBoolean only accepts
   * "0"/"1"/"true"/"false"/"yes"/"no"/"on"/"off". "42" is not in that set, so it's rejected.
   * RowValidator pre-rejects non-0/1 Numbers to match end-to-end KC v3 behavior.
   */
  @Test
  public void testValidateRowBooleanNonZeroOneIntegerProducesTypeError() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "BOOL_COL",
        createColumnSchema("BOOL_COL", ColumnLogicalType.BOOLEAN, true, null, null, null));

    RowValidator validator = newRowValidator(schema);

    for (Object input : Arrays.asList(42, -1, 999, 2L, -100L)) {
      Map<String, Object> row = new HashMap<>();
      row.put("BOOL_COL", input);
      ValidationResult result = validator.validateRow(row);
      assertFalse(result.isValid(), "Expected type error for numeric input: " + input);
      assertTrue(result.hasTypeError(), "Expected type error for numeric input: " + input);
      assertEquals("BOOL_COL", result.getColumnName());
    }
  }

  // ================ VARIANT normalization (String → native object) ================

  /**
   * JSON object string sent to VARIANT is parsed back to a Map so the SSv2 SDK stores it as a
   * native VARIANT object, not a JSON-quoted string.
   */
  @Test
  public void testValidateRowVariantJsonObjectStringNormalizedToMap() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("V", createColumnSchema("V", ColumnLogicalType.VARIANT, true, null, null, null));
    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("V", "{\"a\":1}");
    ValidationResult result = validator.validateRow(row);

    assertTrue(result.isValid());
    Object normalized = row.get("V");
    assertTrue(normalized instanceof Map, "Expected Map but got: " + normalized.getClass());
    assertEquals(1, ((Map<?, ?>) normalized).size());
    assertEquals(1, ((Map<?, ?>) normalized).get("a"));
  }

  /**
   * JSON array string sent to VARIANT is parsed back to a List so the SSv2 SDK stores it as a
   * native array.
   */
  @Test
  public void testValidateRowVariantJsonArrayStringNormalizedToList() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("V", createColumnSchema("V", ColumnLogicalType.VARIANT, true, null, null, null));
    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("V", "[1,2,3]");
    ValidationResult result = validator.validateRow(row);

    assertTrue(result.isValid());
    Object normalized = row.get("V");
    assertTrue(normalized instanceof List, "Expected List but got: " + normalized.getClass());
    assertEquals(Arrays.asList(1, 2, 3), normalized);
  }

  /** Non-String native objects passed to VARIANT are returned unchanged. */
  @Test
  public void testValidateRowVariantNativeObjectPassthrough() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("V", createColumnSchema("V", ColumnLogicalType.VARIANT, true, null, null, null));
    RowValidator validator = newRowValidator(schema);

    Map<String, Object> nativeMap = new HashMap<>();
    nativeMap.put("key", "value");
    Map<String, Object> row = new HashMap<>();
    row.put("V", nativeMap);

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid());
    assertSame(nativeMap, row.get("V"), "Native Map should not be replaced");
  }

  /** Invalid (non-JSON) string sent to VARIANT produces a type error. */
  @Test
  public void testValidateRowVariantInvalidJsonStringProducesTypeError() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("V", createColumnSchema("V", ColumnLogicalType.VARIANT, true, null, null, null));
    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("V", "not valid json");
    ValidationResult result = validator.validateRow(row);

    assertFalse(result.isValid());
    assertTrue(result.hasTypeError());
    assertEquals("V", result.getColumnName());
  }

  // ================ ARRAY normalization (String → List) ================

  /**
   * JSON array string sent to ARRAY is parsed back to a List so the SSv2 SDK stores it as a proper
   * array, not a single-element array wrapping the literal string.
   */
  @Test
  public void testValidateRowArrayJsonStringNormalizedToList() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("A", createColumnSchema("A", ColumnLogicalType.ARRAY, true, null, null, null));
    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("A", "[1,2,3]");
    ValidationResult result = validator.validateRow(row);

    assertTrue(result.isValid());
    Object normalized = row.get("A");
    assertTrue(normalized instanceof List, "Expected List but got: " + normalized.getClass());
    assertEquals(Arrays.asList(1, 2, 3), normalized);
  }

  /**
   * Non-array JSON string sent to ARRAY is wrapped in a single-element List (matching
   * validateAndParseArray behavior which wraps non-arrays into single-element arrays).
   */
  @Test
  public void testValidateRowArrayNonArrayJsonStringWrappedInList() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("A", createColumnSchema("A", ColumnLogicalType.ARRAY, true, null, null, null));
    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("A", "\"hello\""); // JSON string (not an array)
    ValidationResult result = validator.validateRow(row);

    assertTrue(result.isValid());
    Object normalized = row.get("A");
    assertTrue(normalized instanceof List, "Expected List but got: " + normalized.getClass());
    assertEquals(Arrays.asList("hello"), normalized);
  }

  /** Native List passed to ARRAY is returned unchanged. */
  @Test
  public void testValidateRowArrayNativeListPassthrough() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("A", createColumnSchema("A", ColumnLogicalType.ARRAY, true, null, null, null));
    RowValidator validator = newRowValidator(schema);

    List<Integer> nativeList = Arrays.asList(10, 20, 30);
    Map<String, Object> row = new HashMap<>();
    row.put("A", nativeList);

    ValidationResult result = validator.validateRow(row);
    assertTrue(result.isValid());
    assertSame(nativeList, row.get("A"), "Native List should not be replaced");
  }

  /** Invalid (non-JSON) string sent to ARRAY produces a type error. */
  @Test
  public void testValidateRowArrayInvalidJsonStringProducesTypeError() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("A", createColumnSchema("A", ColumnLogicalType.ARRAY, true, null, null, null));
    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("A", "not_json");
    ValidationResult result = validator.validateRow(row);

    assertFalse(result.isValid());
    assertTrue(result.hasTypeError());
    assertEquals("A", result.getColumnName());
  }

  // ================ OBJECT validation Tests ================

  /** Invalid (non-JSON) string sent to OBJECT produces a type error. */
  @Test
  public void testValidateRowObjectInvalidJsonStringProducesTypeError() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("O", createColumnSchema("O", ColumnLogicalType.OBJECT, true, null, null, null));
    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("O", "not_json");
    ValidationResult result = validator.validateRow(row);

    assertFalse(result.isValid());
    assertTrue(result.hasTypeError());
    assertEquals("O", result.getColumnName());
  }

  /** Valid JSON array string sent to OBJECT is rejected (not an object). */
  @Test
  public void testValidateRowObjectArrayJsonStringProducesTypeError() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("O", createColumnSchema("O", ColumnLogicalType.OBJECT, true, null, null, null));
    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("O", "[1,2,3]");
    ValidationResult result = validator.validateRow(row);

    assertFalse(result.isValid());
    assertTrue(result.hasTypeError());
    assertEquals("O", result.getColumnName());
  }

  /** Valid JSON object string sent to OBJECT is accepted. */
  @Test
  public void testValidateRowObjectValidJsonStringAccepted() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("O", createColumnSchema("O", ColumnLogicalType.OBJECT, true, null, null, null));
    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("O", "{\"key\":\"value\"}");
    ValidationResult result = validator.validateRow(row);

    assertTrue(result.isValid());
  }

  /** Invalid hex string for a BINARY column produces a type error. */
  @Test
  public void testValidateRowBinaryInvalidHexStringFails() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put(
        "BIN_COL",
        new ColumnSchema(
            "BIN_COL",
            ColumnLogicalType.BINARY,
            ColumnPhysicalType.BINARY,
            true,
            null,
            null,
            null,
            8388608,
            null));

    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("BIN_COL", "not-valid-hex!");

    ValidationResult result = validator.validateRow(row);
    assertFalse(result.isValid());
    assertTrue(result.hasTypeError());
    assertEquals("BIN_COL", result.getColumnName());
  }

  // ================ TIME normalization (SNOW-3766306) ================

  /**
   * With normalization enabled (default), a TIME string with a UTC offset is stripped to LocalTime.
   * The row value must be a LocalTime after validation so the SSv2 SDK can serialize it correctly.
   */
  @Test
  public void validateRow_timeOffset_isNormalized() {
    Map<String, ColumnSchema> schema =
        Collections.singletonMap("TS", ColumnSchema.fromDescribeTableFields("TS", "TIME(9)", "Y"));
    RowValidator v = newRowValidator(schema);
    Map<String, Object> row = new HashMap<>();
    row.put("TS", "00:00:00Z");
    ValidationResult r = v.validateRow(row);
    assertTrue(r.isValid());
    assertEquals(LocalTime.of(0, 0), row.get("TS"));
  }

  /**
   * normalizeTime=true (explicit) — offset is stripped to LocalTime, same as the default
   * constructor.
   */
  @Test
  public void validateRow_timeOffset_normalizeEnabled_returnsLocalTime() {
    Map<String, ColumnSchema> schema =
        Collections.singletonMap("TS", ColumnSchema.fromDescribeTableFields("TS", "TIME(9)", "Y"));
    RowValidator v = new RowValidator(schema, /* normalizeTime= */ true);
    Map<String, Object> row = new HashMap<>();
    row.put("TS", "13:02:06.123456789+05:00");
    ValidationResult r = v.validateRow(row);
    assertTrue(r.isValid());
    assertEquals(LocalTime.of(13, 2, 6, 123456789), row.get("TS"));
  }

  /**
   * normalizeTime=false — the TIME value is still validated but NOT normalized; the raw String is
   * passed through unchanged. This is the pre-PR (kill-switch) behaviour.
   */
  @Test
  public void validateRow_timeOffset_normalizeDisabled_rawValuePassedThrough() {
    Map<String, ColumnSchema> schema =
        Collections.singletonMap("TS", ColumnSchema.fromDescribeTableFields("TS", "TIME(9)", "Y"));
    RowValidator v = new RowValidator(schema, /* normalizeTime= */ false);
    Map<String, Object> row = new HashMap<>();
    row.put("TS", "00:00:00Z");
    ValidationResult r = v.validateRow(row);
    // Validated (valid value), but the raw String is left untouched — no normalization.
    assertTrue(r.isValid());
    assertEquals("00:00:00Z", row.get("TS"));
  }

  /**
   * normalizeTime=false — validation still runs, so a malformed TIME value is rejected (routed to
   * the DLQ), exactly as pre-PR. Guards against the flag disabling validation entirely.
   */
  @Test
  public void validateRow_invalidTime_normalizeDisabled_isRejected() {
    Map<String, ColumnSchema> schema =
        Collections.singletonMap("TS", ColumnSchema.fromDescribeTableFields("TS", "TIME(9)", "Y"));
    RowValidator v = new RowValidator(schema, /* normalizeTime= */ false);
    Map<String, Object> row = new HashMap<>();
    row.put("TS", "not_a_time");
    ValidationResult r = v.validateRow(row);
    assertFalse(r.isValid());
    assertEquals("TS", r.getColumnName());
  }

  // ================ Timestamp normalization Tests ================

  /**
   * Integer epoch for TIMESTAMP_NTZ must be normalized to an ISO timestamp string. The SSv2 SDK
   * passes raw integers to the Snowflake backend which interprets them using the channel's default
   * timezone (America/Los_Angeles) instead of UTC. SSv1 SDK converts epochs to UTC client-side.
   */
  @Test
  public void testValidateRowTimestampNtzIntegerEpochNormalized() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("TS", createTimestampColumnSchema("TS", ColumnLogicalType.TIMESTAMP_NTZ));
    RowValidator validator = newRowValidator(schema);

    // 1705312800 = 2024-01-15T10:00:00Z
    Map<String, Object> row = new HashMap<>();
    row.put("TS", 1705312800);
    ValidationResult result = validator.validateRow(row);

    assertTrue(result.isValid());
    Object normalized = row.get("TS");
    assertInstanceOf(String.class, normalized, "Integer epoch should be normalized to String");
    assertEquals("2024-01-15T10:00", normalized);
  }

  /** Long epoch for TIMESTAMP_NTZ is also normalized (same as Integer). */
  @Test
  public void testValidateRowTimestampNtzLongEpochNormalized() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("TS", createTimestampColumnSchema("TS", ColumnLogicalType.TIMESTAMP_NTZ));
    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("TS", 1705312800L);
    ValidationResult result = validator.validateRow(row);

    assertTrue(result.isValid());
    Object normalized = row.get("TS");
    assertInstanceOf(String.class, normalized, "Long epoch should be normalized to String");
    assertEquals("2024-01-15T10:00", normalized);
  }

  /** String timestamp for TIMESTAMP_NTZ is validated but returned unchanged. */
  @Test
  public void testValidateRowTimestampNtzStringPassthrough() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("TS", createTimestampColumnSchema("TS", ColumnLogicalType.TIMESTAMP_NTZ));
    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("TS", "2024-01-15T13:45:30");
    ValidationResult result = validator.validateRow(row);

    assertTrue(result.isValid());
    assertEquals("2024-01-15T13:45:30", row.get("TS"));
  }

  /** Integer epoch for TIMESTAMP_LTZ is normalized to ISO string with UTC offset. */
  @Test
  public void testValidateRowTimestampLtzIntegerEpochNormalized() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("TS", createTimestampColumnSchema("TS", ColumnLogicalType.TIMESTAMP_LTZ));
    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("TS", 1705312800);
    ValidationResult result = validator.validateRow(row);

    assertTrue(result.isValid());
    Object normalized = row.get("TS");
    assertInstanceOf(String.class, normalized, "Integer epoch should be normalized to String");
    assertEquals("2024-01-15T10:00Z", normalized);
  }

  /** Invalid string for TIMESTAMP_NTZ produces a type error. */
  @Test
  public void testValidateRowTimestampNtzInvalidStringRejects() {
    Map<String, ColumnSchema> schema = new HashMap<>();
    schema.put("TS", createTimestampColumnSchema("TS", ColumnLogicalType.TIMESTAMP_NTZ));
    RowValidator validator = newRowValidator(schema);

    Map<String, Object> row = new HashMap<>();
    row.put("TS", "not_a_timestamp");
    ValidationResult result = validator.validateRow(row);

    assertFalse(result.isValid());
    assertTrue(result.hasTypeError());
    assertEquals("TS", result.getColumnName());
  }

  // ================ Helper Methods ================

  /**
   * Constructs a {@link RowValidator} with TIME normalization enabled — the default for the vast
   * majority of tests, which do not exercise the normalizeTime flag. Centralizing the constructor
   * call keeps the diff small if the constructor signature changes again. Tests that specifically
   * exercise the flag call {@code new RowValidator(schema, normalizeTime)} directly.
   */
  private static RowValidator newRowValidator(Map<String, ColumnSchema> columnSchemaMap) {
    return new RowValidator(columnSchemaMap, /* normalizeTime= */ true);
  }

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

  private ColumnSchema createTimestampColumnSchema(String name, ColumnLogicalType logicalType) {
    return new ColumnSchema(
        name, logicalType, ColumnPhysicalType.SB8, true, null, 9, null, null, null);
  }
}
