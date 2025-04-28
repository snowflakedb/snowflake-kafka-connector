package com.snowflake.kafka.connector.internal.streaming.validation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class RowSchemaTest {

    @Nested
    class Constructor {
        private final List<ColumnMetadata> columnMetadata = new ArrayList<>();

        @BeforeEach
        void prepareColumnMetadata() {
            clearColumnMetadata();
        }

        void clearColumnMetadata() {
            columnMetadata.clear();
        }

        ColumnMetadata withNewColumnMetadataBase() {
            final ColumnMetadata columnMetadatum = new ColumnMetadata();
            columnMetadatum.setName("COL1");
            columnMetadatum.setPhysicalType("SB1");
            columnMetadatum.setLogicalType("FIXED");
            columnMetadatum.setNullable(false);
            columnMetadatum.setByteLength(14);
            columnMetadatum.setLength(11);
            columnMetadatum.setScale(0);
            columnMetadata.add(columnMetadatum);
            return columnMetadatum;
        }

        Executable createRowSchema(boolean isIceberg) {
            return () -> new RowSchema(isIceberg, columnMetadata);
        }

        @ParameterizedTest
        @ValueSource(booleans = {true, false})
        void testCollatedColumnUnsupported(final boolean isIceberg) {
            final ColumnMetadata collatedColumn = withNewColumnMetadataBase();
            collatedColumn.setName("COLCHAR");
            collatedColumn.setPhysicalType("LOB");
            collatedColumn.setNullable(true);
            collatedColumn.setLogicalType("TEXT");
            collatedColumn.setByteLength(14);
            collatedColumn.setLength(11);
            collatedColumn.setScale(0);
            collatedColumn.setCollation("en-ci");

            final RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class, createRowSchema(isIceberg));
            assertEquals("Data type not supported: Column COLCHAR with collation en-ci detected. Ingestion into collated columns is not supported", runtimeException.getMessage());
        }

        @Nested
        class NonsenseColumnMetadata {
            private ColumnMetadata columnMetadatum;

            @BeforeEach
            void prepareColumnMetadatumBase() {
                columnMetadatum = withNewColumnMetadataBase();
                columnMetadatum.setNullable(true);
                columnMetadatum.setByteLength(14);
                columnMetadatum.setLength(11);
                columnMetadatum.setScale(0);
                columnMetadatum.setPrecision(4);
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testNonsenseColumnUnsupported(final boolean isIceberg) {
                columnMetadatum.setName("testCol");
                columnMetadatum.setPhysicalType("Failure");
                columnMetadatum.setLogicalType("FIXED");

                final RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class, createRowSchema(isIceberg));
                assertEquals("Unknown data type for column: testCol. logical: FIXED, physical: Failure.", runtimeException.getMessage());
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testFixedLobColumnUnsupported(final boolean isIceberg) {
                columnMetadatum.setName("COL1");
                columnMetadatum.setPhysicalType("LOB");
                columnMetadatum.setLogicalType("FIXED");

                final RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class, createRowSchema(isIceberg));
                assertEquals("Unknown data type for column: COL1. logical: FIXED, physical: LOB.", runtimeException.getMessage());
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testTimestampNtzSb2ColumnUnsupported(final boolean isIceberg) {
                columnMetadatum.setName("COL1");
                columnMetadatum.setPhysicalType("SB2");
                columnMetadatum.setLogicalType("TIMESTAMP_NTZ");

                final RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class, createRowSchema(isIceberg));
                assertEquals("Unknown data type for column: COL1. logical: TIMESTAMP_NTZ(0), physical: SB2.", runtimeException.getMessage());
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testTimestampTzSb1ColumnUnsupported(final boolean isIceberg) {
                columnMetadatum.setName("COL1");
                columnMetadatum.setPhysicalType("SB1");
                columnMetadatum.setLogicalType("TIMESTAMP_TZ");

                final RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class, createRowSchema(isIceberg));
                assertEquals("Unknown data type for column: COL1. logical: TIMESTAMP_TZ(0), physical: SB1.", runtimeException.getMessage());
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testTimeSb16ColumnUnsupported(final boolean isIceberg) {
                columnMetadatum.setName("COL1");
                columnMetadatum.setPhysicalType("SB16");
                columnMetadatum.setLogicalType("TIME");

                final RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class, createRowSchema(isIceberg));
                assertEquals("Unknown data type for column: COL1. logical: TIME(0), physical: SB16.", runtimeException.getMessage());
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testInvalidLogicalTypeUnsupported(final boolean isIceberg) {
                columnMetadatum.setName("COLINVALIDLOGICAL");
                columnMetadatum.setPhysicalType("SB1");
                columnMetadatum.setNullable(false);
                columnMetadatum.setLogicalType("INVALID");
                columnMetadatum.setByteLength(14);
                columnMetadatum.setLength(11);
                columnMetadatum.setScale(0);

                final RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class, createRowSchema(isIceberg));
                assertEquals("Unknown data type for column: COLINVALIDLOGICAL. logical: INVALID, physical: SB1.", runtimeException.getMessage());
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testInvalidPhysicalTypeUnsupported(final boolean isIceberg) {
                columnMetadatum.setName("COLINVALIDPHYSICAL");
                columnMetadatum.setPhysicalType("INVALID");
                columnMetadatum.setNullable(false);
                columnMetadatum.setLogicalType("FIXED");
                columnMetadatum.setByteLength(14);
                columnMetadatum.setLength(11);
                columnMetadatum.setScale(0);

                final RuntimeException runtimeException = Assertions.assertThrows(RuntimeException.class, createRowSchema(isIceberg));
                assertEquals("Unknown data type for column: COLINVALIDPHYSICAL. logical: FIXED, physical: INVALID.", runtimeException.getMessage());
            }
        }
    }

    @Nested
    class Validations {

        @Nested
        class StringLength {

            List<ColumnMetadata> prepareStringColumnMetadata() {
                final ColumnMetadata columnMetadatum = new ColumnMetadata();
                columnMetadatum.setName("COLCHAR");
                columnMetadatum.setPhysicalType("LOB");
                columnMetadatum.setNullable(true);
                columnMetadatum.setLogicalType("TEXT");
                columnMetadatum.setByteLength(14);
                columnMetadatum.setLength(11);
                columnMetadatum.setScale(0);
                return Collections.singletonList(columnMetadatum);
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testStringLengthOk(final boolean isIceberg) {
                Assertions.assertNull(validateRowSchema(isIceberg, prepareStringColumnMetadata(), Map.of("COLCHAR", "12345678901")));
            }

            @Test
            void testStringLengthTooLong() {
                // iceberg does not provide support for max string size
                RowSchema.Error error = validateRowSchema(/* isIceberg= */ false, prepareStringColumnMetadata(), Map.of("COLCHAR", "12345678901234567890"));
                Assertions.assertNotNull(error);
            }
        }

        @Nested
        class HappyPath {

            static List<ColumnMetadata> createSchema(boolean isIceberg) {
                ColumnMetadata colTinyIntCase = new ColumnMetadata();
                colTinyIntCase.setName("\"colTinyInt\"");
                colTinyIntCase.setPhysicalType("SB1");
                colTinyIntCase.setNullable(true);
                colTinyIntCase.setLogicalType("FIXED");
                colTinyIntCase.setPrecision(2);
                colTinyIntCase.setScale(0);

                ColumnMetadata colTinyInt = new ColumnMetadata();
                colTinyInt.setName("COLTINYINT");
                colTinyInt.setPhysicalType("SB1");
                colTinyInt.setNullable(true);
                colTinyInt.setLogicalType("FIXED");
                colTinyInt.setPrecision(1);
                colTinyInt.setScale(0);

                ColumnMetadata colSmallInt = new ColumnMetadata();
                colSmallInt.setName("COLSMALLINT");
                colSmallInt.setPhysicalType("SB2");
                colSmallInt.setNullable(true);
                colSmallInt.setLogicalType("FIXED");
                colSmallInt.setPrecision(2);
                colSmallInt.setScale(0);

                ColumnMetadata colInt = new ColumnMetadata();
                colInt.setName("COLINT");
                colInt.setPhysicalType("SB4");
                colInt.setNullable(true);
                colInt.setLogicalType("FIXED");
                colInt.setPrecision(2);
                colInt.setScale(0);

                ColumnMetadata colBigInt = new ColumnMetadata();
                colBigInt.setName("COLBIGINT");
                colBigInt.setPhysicalType("SB8");
                colBigInt.setNullable(true);
                colBigInt.setLogicalType("FIXED");
                colBigInt.setPrecision(2);
                colBigInt.setScale(0);

                ColumnMetadata colDecimal = new ColumnMetadata();
                colDecimal.setName("COLDECIMAL");
                colDecimal.setPhysicalType("SB16");
                colDecimal.setNullable(true);
                colDecimal.setLogicalType("FIXED");
                colDecimal.setPrecision(38);
                colDecimal.setScale(2);

                ColumnMetadata colChar = new ColumnMetadata();
                colChar.setName("COLCHAR");
                colChar.setPhysicalType("LOB");
                colChar.setNullable(true);
                colChar.setLogicalType("TEXT");
                colChar.setByteLength(14);
                colChar.setLength(11);
                colChar.setScale(0);

                if (isIceberg) {
                    colTinyIntCase.setSourceIcebergDataType("\"decimal(2,0)\"");
                    colTinyInt.setSourceIcebergDataType("\"decimal(1,0)\"");
                    colSmallInt.setSourceIcebergDataType("\"decimal(2,0)\"");
                    colInt.setSourceIcebergDataType("\"int\"");
                    colBigInt.setSourceIcebergDataType("\"long\"");
                    colDecimal.setSourceIcebergDataType("\"decimal(38,2)\"");
                    colChar.setSourceIcebergDataType("\"string\"");
                }

                List<ColumnMetadata> columns =
                        Arrays.asList(
                                colTinyIntCase, colTinyInt, colSmallInt, colInt, colBigInt, colDecimal, colChar);
                for (int i = 0; i < columns.size(); i++) {
                    columns.get(i).setOrdinal(i + 1);
                }
                return columns;
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testValidateRow(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = createSchema(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("colTinyInt", (byte) 1);
                row.put("\"colTinyInt\"", (byte) 1);
                row.put("colSmallInt", (short) 2);
                row.put("colInt", 3);
                row.put("colBigInt", 4L);
                row.put("colDecimal", 1.23);
                row.put("colChar", "2");

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testValidateNulls(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = createSchema(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("colTinyInt", null);
                row.put("\"colTinyInt\"", null);
                row.put("colSmallInt", null);
                row.put("colInt", null);
                row.put("colBigInt", null);
                row.put("colDecimal", null);
                row.put("colChar", null);

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testDoubleQuotes(final boolean isIceberg) {
                ColumnMetadata colDoubleQuotes = new ColumnMetadata();
                colDoubleQuotes.setOrdinal(1);
                colDoubleQuotes.setName("\"colDoubleQuotes\"");
                colDoubleQuotes.setPhysicalType("SB16");
                colDoubleQuotes.setNullable(true);
                colDoubleQuotes.setLogicalType("FIXED");
                colDoubleQuotes.setPrecision(38);
                colDoubleQuotes.setScale(0);

                List<ColumnMetadata> columnMetadata = Collections.singletonList(colDoubleQuotes);
                Map<String, Object> row = Map.of("\"colDoubleQuotes\"", 1);

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }
        }

        @Nested
        class TimestampType {

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testExtraTimestamp(final boolean isIceberg) {
                ColumnMetadata colTimestampLtzSB16 = new ColumnMetadata();
                colTimestampLtzSB16.setOrdinal(1);
                colTimestampLtzSB16.setName("COLTIMESTAMPLTZ_SB16");
                colTimestampLtzSB16.setPhysicalType("SB16");
                colTimestampLtzSB16.setNullable(false);
                colTimestampLtzSB16.setLogicalType("TIMESTAMP_LTZ");
                colTimestampLtzSB16.setScale(6);

                List<ColumnMetadata> columnMetadata = Collections.singletonList(colTimestampLtzSB16);
                Map<String, Object> row = new HashMap<>();
                row.put("COLTIMESTAMPLTZ_SB8", "1621899220");
                row.put("COLTIMESTAMPLTZ_SB16", "1621899220.1234567");

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                Assertions.assertEquals(List.of("COLTIMESTAMPLTZ_SB8"), actual.extraColNames());
            }

            private static List<ColumnMetadata> getTimestampsRowColumnMetadata(boolean isIceberg) {
                ColumnMetadata colTimestampLtzSB8 = new ColumnMetadata();
                colTimestampLtzSB8.setOrdinal(1);
                colTimestampLtzSB8.setName("COLTIMESTAMPLTZ_SB8");
                colTimestampLtzSB8.setPhysicalType("SB8");
                colTimestampLtzSB8.setNullable(true);
                colTimestampLtzSB8.setLogicalType("TIMESTAMP_LTZ");
                colTimestampLtzSB8.setScale(0);

                ColumnMetadata colTimestampLtzSB16 = new ColumnMetadata();
                colTimestampLtzSB16.setOrdinal(2);
                colTimestampLtzSB16.setName("COLTIMESTAMPLTZ_SB16");
                colTimestampLtzSB16.setPhysicalType("SB16");
                colTimestampLtzSB16.setNullable(true);
                colTimestampLtzSB16.setLogicalType("TIMESTAMP_LTZ");
                colTimestampLtzSB16.setScale(9);

                ColumnMetadata colTimestampLtzSB16Scale6 = new ColumnMetadata();
                colTimestampLtzSB16Scale6.setOrdinal(3);
                colTimestampLtzSB16Scale6.setName("COLTIMESTAMPLTZ_SB16_SCALE6");
                colTimestampLtzSB16Scale6.setPhysicalType("SB16");
                colTimestampLtzSB16Scale6.setNullable(true);
                colTimestampLtzSB16Scale6.setLogicalType("TIMESTAMP_LTZ");
                colTimestampLtzSB16Scale6.setScale(6);

                if (isIceberg) {
                    colTimestampLtzSB8.setSourceIcebergDataType("\"timestamptz\"");
                    colTimestampLtzSB16.setSourceIcebergDataType("\"timestamptz\"");
                    colTimestampLtzSB16Scale6.setSourceIcebergDataType("\"timestamptz\"");
                }

                return Arrays.asList(colTimestampLtzSB8, colTimestampLtzSB16, colTimestampLtzSB16Scale6);
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testTimestamps0(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getTimestampsRowColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLTIMESTAMPLTZ_SB8", "1621899220");
                row.put("COLTIMESTAMPLTZ_SB16", "1621899220123456789");
                row.put("COLTIMESTAMPLTZ_SB16_SCALE6", "1621899220123456");

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testTimestamps1(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getTimestampsRowColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLTIMESTAMPLTZ_SB8", "1621899221");
                row.put("COLTIMESTAMPLTZ_SB16", "1621899220223456789");
                row.put("COLTIMESTAMPLTZ_SB16_SCALE6", "1621899220123457");

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testTimestamps2(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getTimestampsRowColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLTIMESTAMPLTZ_SB8", null);
                row.put("COLTIMESTAMPLTZ_SB16", null);
                row.put("COLTIMESTAMPLTZ_SB16_SCALE6", null);

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }
        }
        
        @Nested
        class DateType {
            
            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testDate(final boolean isIceberg) {
                ColumnMetadata colDate = new ColumnMetadata();
                colDate.setOrdinal(1);
                colDate.setName("COLDATE");
                colDate.setPhysicalType("SB8");
                colDate.setNullable(true);
                colDate.setLogicalType("DATE");
                colDate.setScale(0);

                Map<String, Object> row = new HashMap<>();
                row.put("COLDATE", String.valueOf(18772 * 24 * 60 * 60 * 1000L + 1));

                Assertions.assertNull(validateRowSchema(isIceberg, Collections.singletonList(colDate), row));
            }
        }
        
        @Nested
        class TimeType {

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testTime(final boolean isIceberg) {
                ColumnMetadata colTimeSB4 = new ColumnMetadata();
                colTimeSB4.setOrdinal(1);
                colTimeSB4.setName("COLTIMESB4");
                colTimeSB4.setPhysicalType("SB4");
                colTimeSB4.setNullable(true);
                colTimeSB4.setLogicalType("TIME");
                colTimeSB4.setScale(0);

                ColumnMetadata colTimeSB8 = new ColumnMetadata();
                colTimeSB8.setOrdinal(2);
                colTimeSB8.setName("COLTIMESB8");
                colTimeSB8.setPhysicalType("SB8");
                colTimeSB8.setNullable(true);
                colTimeSB8.setLogicalType("TIME");
                colTimeSB8.setScale(3);

                if (isIceberg) {
                    colTimeSB4.setSourceIcebergDataType("\"time\"");
                    colTimeSB8.setSourceIcebergDataType("\"time\"");
                }

                Map<String, Object> row = new HashMap<>();
                row.put("COLTIMESB4", "10:00:00");
                row.put("COLTIMESB8", "10:00:00.123");

                Assertions.assertNull(validateRowSchema(isIceberg, List.of(colTimeSB4, colTimeSB8), row));
            }
        }

        @Nested
        class MissingValue {

            List<ColumnMetadata> prepareNullableColumnMetadata() {
                ColumnMetadata colBoolean = new ColumnMetadata();
                colBoolean.setOrdinal(1);
                colBoolean.setName("COLBOOLEAN");
                colBoolean.setPhysicalType("SB1");
                colBoolean.setNullable(false);
                colBoolean.setLogicalType("BOOLEAN");
                colBoolean.setScale(0);
                return List.of(colBoolean);
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testNonNull(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = prepareNullableColumnMetadata();

                Map<String, Object> row = new HashMap<>();
                row.put("COLBOOLEAN", true);

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testNull(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = prepareNullableColumnMetadata();

                Map<String, Object> row = new HashMap<>();
                row.put("COLBOOLEAN", null);

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                Assertions.assertEquals(List.of("COLBOOLEAN"), actual.nullValueForNotNullColNames());
            }
        }

        @Nested
        class MissingColumn {

            List<ColumnMetadata> prepareNullableColumnMetadata() {
                ColumnMetadata colBoolean = new ColumnMetadata();
                colBoolean.setOrdinal(1);
                colBoolean.setName("COLBOOLEAN");
                colBoolean.setPhysicalType("SB1");
                colBoolean.setNullable(false);
                colBoolean.setLogicalType("BOOLEAN");
                colBoolean.setScale(0);

                ColumnMetadata colBoolean2 = new ColumnMetadata();
                colBoolean2.setOrdinal(2);
                colBoolean2.setName("COLBOOLEAN2");
                colBoolean2.setPhysicalType("SB1");
                colBoolean2.setNullable(true);
                colBoolean2.setLogicalType("BOOLEAN");
                colBoolean2.setScale(0);

                return List.of(colBoolean, colBoolean2);
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testNonNull(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = prepareNullableColumnMetadata();

                Map<String, Object> row = new HashMap<>();
                row.put("COLBOOLEAN", true);

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testNull(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = prepareNullableColumnMetadata();

                Map<String, Object> row = new HashMap<>();
                row.put("COLBOOLEAN2", true);

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                Assertions.assertEquals(List.of("COLBOOLEAN"), actual.missingNotNullColNames());
            }
        }

        @Nested
        class ExtraColumn {

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void test(final boolean isIceberg) {

                ColumnMetadata colBoolean = new ColumnMetadata();
                colBoolean.setOrdinal(1);
                colBoolean.setName("COLBOOLEAN1");
                colBoolean.setPhysicalType("SB1");
                colBoolean.setNullable(false);
                colBoolean.setLogicalType("BOOLEAN");
                colBoolean.setScale(0);

                Map<String, Object> row = new HashMap<>();
                row.put("COLBOOLEAN1", true);
                row.put("COLBOOLEAN2", true);
                row.put("COLBOOLEAN3", true);

                RowSchema.Error actual = validateRowSchema(isIceberg, List.of(colBoolean), row);
                Assertions.assertEquals(List.of("COLBOOLEAN3", "COLBOOLEAN2"), actual.extraColNames());
            }
        }

        @Nested
        class BooleanType {

            List<ColumnMetadata> getBooleanColumnMetadata(final boolean isIceberg) {
                ColumnMetadata colBoolean = new ColumnMetadata();
                colBoolean.setOrdinal(1);
                colBoolean.setName("COLBOOLEAN1");
                colBoolean.setPhysicalType("SB1");
                colBoolean.setNullable(true);
                colBoolean.setLogicalType("BOOLEAN");
                colBoolean.setScale(0);
                if (isIceberg) {
                    colBoolean.setSourceIcebergDataType("\"boolean\"");
                }
                return List.of(colBoolean);
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testString(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getBooleanColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLBOOLEAN1", "true");

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testBoolean(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getBooleanColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLBOOLEAN1", Boolean.FALSE);

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testPrimitive(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getBooleanColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLBOOLEAN1", true);

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testNull(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getBooleanColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLBOOLEAN1", null);

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testStringInvalid(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getBooleanColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLBOOLEAN1", "falze");

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                Assertions.assertEquals("The given row cannot be converted to the internal format due to invalid value: Value cannot be ingested into Snowflake column COLBOOLEAN1 of type BOOLEAN, rowIndex:0, reason: Not a valid boolean, see https://docs.snowflake.com/en/sql-reference/data-types-logical.html#conversion-to-boolean for the list of supported formats", actual.localizedMessage());
            }
        }

        @Nested
        class BinaryType {

            List<ColumnMetadata> getBinaryColumnMetadata(final boolean isIceberg) {
                ColumnMetadata colBinary = new ColumnMetadata();
                colBinary.setOrdinal(1);
                colBinary.setName("COLBINARY");
                colBinary.setPhysicalType("LOB");
                colBinary.setNullable(true);
                colBinary.setLogicalType("BINARY");
                colBinary.setLength(32);
                colBinary.setByteLength(256);
                colBinary.setScale(0);
                if (isIceberg) {
                    colBinary.setSourceIcebergDataType("\"binary\"");
                }
                return List.of(colBinary);
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testBinary0(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getBinaryColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLBINARY", "Hello World".getBytes(StandardCharsets.UTF_8));

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testBinary1(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getBinaryColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLBINARY", "Honk Honk".getBytes(StandardCharsets.UTF_8));

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testBinary2(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getBinaryColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLBINARY", null);

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }
        }

        @Nested
        class RealType {

            List<ColumnMetadata> getRealColumnMetadata() {
                ColumnMetadata colReal = new ColumnMetadata();
                colReal.setOrdinal(1);
                colReal.setName("COLREAL");
                colReal.setPhysicalType("SB16");
                colReal.setNullable(true);
                colReal.setLogicalType("REAL");
                colReal.setScale(0);
                return List.of(colReal);
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testBinary0(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getRealColumnMetadata();

                Map<String, Object> row = new HashMap<>();
                row.put("COLREAL", 123.456);

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testBinary1(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getRealColumnMetadata();

                Map<String, Object> row = new HashMap<>();
                row.put("COLREAL", 123.4567);

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testBinary2(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getRealColumnMetadata();

                Map<String, Object> row = new HashMap<>();
                row.put("COLREAL", null);

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }
        }

        @Nested
        class DecimalType {

            List<ColumnMetadata> getDecimalColumnMetadata() {
                ColumnMetadata colDecimal = new ColumnMetadata();
                colDecimal.setOrdinal(1);
                colDecimal.setName("COLDECIMAL");
                colDecimal.setPhysicalType("SB16");
                colDecimal.setNullable(true);
                colDecimal.setLogicalType("FIXED");
                colDecimal.setPrecision(38);
                colDecimal.setScale(0);
                return List.of(colDecimal);
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testDecimal0(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getDecimalColumnMetadata();

                Map<String, Object> row = new HashMap<>();
                row.put("COLDECIMAL", 1);

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testDecimal1(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getDecimalColumnMetadata();

                Map<String, Object> row = new HashMap<>();
                row.put("COLDECIMAL", "202");

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testDecimalNull(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getDecimalColumnMetadata();

                Map<String, Object> row = new HashMap<>();
                row.put("COLDECIMAL", null);

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testDecimalInvalidString(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getDecimalColumnMetadata();

                Map<String, Object> row = new HashMap<>();
                row.put("COLDECIMAL", "true");

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                Assertions.assertEquals("The given row cannot be converted to the internal format due to invalid value: Value cannot be ingested into Snowflake column COLDECIMAL of type NUMBER, rowIndex:0, reason: Not a valid number", actual.localizedMessage());
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testDecimalInvalidBoolean(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getDecimalColumnMetadata();

                Map<String, Object> row = new HashMap<>();
                row.put("COLDECIMAL", true);

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                Assertions.assertEquals("The given row cannot be converted to the internal format: Object of type java.lang.Boolean cannot be ingested into Snowflake column COLDECIMAL of type NUMBER, rowIndex:0. Allowed Java types: int, long, byte, short, float, double, BigDecimal, BigInteger, String", actual.localizedMessage());
            }
        }

        @Nested
        class VariantType {

            List<ColumnMetadata> getVariantColumnMetadata() {
                ColumnMetadata colVariant = new ColumnMetadata();
                colVariant.setOrdinal(1);
                colVariant.setName("COLVARIANT");
                colVariant.setPhysicalType("LOB");
                colVariant.setNullable(true);
                colVariant.setLogicalType("VARIANT");
                return List.of(colVariant);
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testVariantNull(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getVariantColumnMetadata();

                Map<String, Object> row = new HashMap<>();
                row.put("COLVARIANT", null);

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testVariantNullString(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getVariantColumnMetadata();

                Map<String, Object> row = new HashMap<>();
                row.put("COLVARIANT", "null");

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testVariantEmpty(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getVariantColumnMetadata();

                Map<String, Object> row = new HashMap<>();
                row.put("COLVARIANT", "");

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testVariantObject(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getVariantColumnMetadata();

                Map<String, Object> row = new HashMap<>();
                row.put("COLVARIANT", "{\"key\":1}");

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testVariantNumber(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getVariantColumnMetadata();

                Map<String, Object> row = new HashMap<>();
                row.put("COLVARIANT", 3);

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testVariantBoolean(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getVariantColumnMetadata();

                Map<String, Object> row = new HashMap<>();
                row.put("COLVARIANT", true);

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testVariantInvalidTimestamp(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getVariantColumnMetadata();

                Map<String, Object> row = new HashMap<>();
                row.put("COLVARIANT", java.sql.Timestamp.valueOf("2022-02-22 22:22:22"));

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                if (isIceberg) {
                    Assertions.assertEquals("The given row cannot be converted to the internal format: Object of type java.sql.Timestamp cannot be ingested into Snowflake column COLVARIANT of type STRING, rowIndex:0. Allowed Java types: String, Number, boolean, char", actual.localizedMessage());
                } else {
                    Assertions.assertEquals("The given row cannot be converted to the internal format: Object of type java.sql.Timestamp cannot be ingested into Snowflake column COLVARIANT of type VARIANT, rowIndex:0. Allowed Java types: String, Primitive data types and their arrays, java.time.*, List<T>, Map<String, T>, T[]", actual.localizedMessage());
                }
            }
        }

        @Nested
        class ObjectType {

            List<ColumnMetadata> getObjectColumnMetadata(final boolean isIceberg) {
                ColumnMetadata colObject = new ColumnMetadata();
                colObject.setOrdinal(1);
                colObject.setName("COLOBJECT");
                colObject.setPhysicalType("LOB");
                colObject.setNullable(true);
                colObject.setLogicalType("OBJECT");
                if (isIceberg) {
                    colObject.setSourceIcebergDataType("""
                            {
                              "type" : "struct",
                              "fields" : [ {
                                "id" : 3,
                                "name" : "foobar",
                                "required" : false,
                                "type" : "int"
                              }, {
                                "id" : 4,
                                "name" : "key",
                                "required" : false,
                                "type" : "int"
                              } ]
                            }""");
                }
                return List.of(colObject);
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testObjectNull(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getObjectColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLOBJECT", null);

                Assertions.assertNull(validateRowSchema(isIceberg, columnMetadata, row));
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testObjectNullString(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getObjectColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLOBJECT", "null");

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                if (isIceberg) {
                    Assertions.assertEquals("The given row cannot be converted to the internal format: Object of type java.lang.String cannot be ingested into Snowflake column COLOBJECT of type STRUCT, rowIndex:0. Allowed Java types: Map<String, Object>", actual.localizedMessage());
                } else {
                    Assertions.assertEquals("The given row cannot be converted to the internal format due to invalid value: Value cannot be ingested into Snowflake column COLOBJECT of type OBJECT, rowIndex:0, reason: Not an object", actual.localizedMessage());
                }
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testObjectEmpty(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getObjectColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLOBJECT", "");

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                if (isIceberg) {
                    Assertions.assertEquals("The given row cannot be converted to the internal format: Object of type java.lang.String cannot be ingested into Snowflake column COLOBJECT of type STRUCT, rowIndex:0. Allowed Java types: Map<String, Object>", actual.localizedMessage());
                } else {
                    Assertions.assertEquals("The given row cannot be converted to the internal format due to invalid value: Value cannot be ingested into Snowflake column COLOBJECT of type OBJECT, rowIndex:0, reason: Not an object", actual.localizedMessage());
                }
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testValidObjectString(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getObjectColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLOBJECT", "{\"key\":1}");

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                if (isIceberg) {
                    Assertions.assertEquals("The given row cannot be converted to the internal format: Object of type java.lang.String cannot be ingested into Snowflake column COLOBJECT of type STRUCT, rowIndex:0. Allowed Java types: Map<String, Object>", actual.localizedMessage());
                } else {
                    Assertions.assertNull(actual);
                }
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testValidObject(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getObjectColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLOBJECT", new HashMap<String, Object>() {
                    {
                        put("key", 1);
                    }
                });

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                Assertions.assertNull(actual);
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testObjectNumber(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getObjectColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLOBJECT", 3);

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                if (isIceberg) {
                    Assertions.assertEquals("The given row cannot be converted to the internal format: Object of type java.lang.Integer cannot be ingested into Snowflake column COLOBJECT of type STRUCT, rowIndex:0. Allowed Java types: Map<String, Object>", actual.localizedMessage());
                } else {
                    Assertions.assertEquals("The given row cannot be converted to the internal format due to invalid value: Value cannot be ingested into Snowflake column COLOBJECT of type OBJECT, rowIndex:0, reason: Not an object", actual.localizedMessage());
                }
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testObjectBoolean(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getObjectColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLOBJECT", true);

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                if (isIceberg) {
                    Assertions.assertEquals("The given row cannot be converted to the internal format: Object of type java.lang.Boolean cannot be ingested into Snowflake column COLOBJECT of type STRUCT, rowIndex:0. Allowed Java types: Map<String, Object>", actual.localizedMessage());
                } else {
                    Assertions.assertEquals("The given row cannot be converted to the internal format due to invalid value: Value cannot be ingested into Snowflake column COLOBJECT of type OBJECT, rowIndex:0, reason: Not an object", actual.localizedMessage());
                }
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testObjectWithDuplicateKeys(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getObjectColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLOBJECT", "{\"key\": 0, \"key\\u0000\": 1}");

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                if (isIceberg) {
                    Assertions.assertEquals("The given row cannot be converted to the internal format: Object of type java.lang.String cannot be ingested into Snowflake column COLOBJECT of type STRUCT, rowIndex:0. Allowed Java types: Map<String, Object>", actual.localizedMessage());
                } else {
                    Assertions.assertEquals("The given row cannot be converted to the internal format due to invalid value: Value cannot be ingested into Snowflake column COLOBJECT of type OBJECT, rowIndex:0, reason: Not a valid JSON: duplicate field key", actual.localizedMessage());
                }
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testObjectWithInvalidKey(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getObjectColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLOBJECT", new LinkedHashMap<String, Integer>() {
                    {
                        put("foo\uD800bar", 1);
                    }
                });

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                if (isIceberg) {
                    Assertions.assertNull(actual); // on par with SSV1, not checked for Iceberg
                } else {
                    Assertions.assertEquals("The given row cannot be converted to the internal format due to invalid value: Value cannot be ingested into Snowflake column COLOBJECT of type OBJECT, rowIndex:0, reason: Invalid Unicode string", actual.localizedMessage());
                }
            }
        }

        @Nested
        class ArrayType {
            // apparently SSV1 does automatically wrap individual values into arrays if they are not arrays themselves
            // see snowflake-ingest-java test: RowBufferTest.testE2EArray

            List<ColumnMetadata> getArrayColumnMetadata(final boolean isIceberg) {
                ColumnMetadata colObject = new ColumnMetadata();
                colObject.setOrdinal(1);
                colObject.setName("COLARRAY");
                colObject.setPhysicalType("LOB");
                colObject.setNullable(true);
                colObject.setLogicalType("ARRAY");
                if (isIceberg) {
                    colObject.setSourceIcebergDataType("""
                            {
                              "type": "list",
                              "element-id": 3,
                              "element-required": true,
                              "element": "int"
                            }
                            """);
                }
                return List.of(colObject);
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testArrayNull(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getArrayColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLARRAY", null);

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                Assertions.assertNull(actual);
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testArrayNullString(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getArrayColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLARRAY", "null");

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                if (isIceberg) {
                    Assertions.assertEquals("The given row cannot be converted to the internal format: Object of type java.lang.String cannot be ingested into Snowflake column COLARRAY of type LIST, rowIndex:0. Allowed Java types: Iterable", actual.localizedMessage());
                } else {
                    Assertions.assertNull(actual);
                }
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testArrayEmpty(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getArrayColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLARRAY", "");

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                if (isIceberg) {
                    Assertions.assertEquals("The given row cannot be converted to the internal format: Object of type java.lang.String cannot be ingested into Snowflake column COLARRAY of type LIST, rowIndex:0. Allowed Java types: Iterable", actual.localizedMessage());
                } else {
                    Assertions.assertNull(actual);
                }
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testArrayString(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getArrayColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLARRAY", "{\"key\":1}");

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                if (isIceberg) {
                    Assertions.assertEquals("The given row cannot be converted to the internal format: Object of type java.lang.String cannot be ingested into Snowflake column COLARRAY of type LIST, rowIndex:0. Allowed Java types: Iterable", actual.localizedMessage());
                } else {
                    Assertions.assertNull(actual);
                }
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testValidArray(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getArrayColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLARRAY", Arrays.asList(1, 2, 3));

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                Assertions.assertNull(actual);
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testArrayNumber(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getArrayColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLARRAY", 3);

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                if (isIceberg) {
                    Assertions.assertEquals("The given row cannot be converted to the internal format: Object of type java.lang.Integer cannot be ingested into Snowflake column COLARRAY of type LIST, rowIndex:0. Allowed Java types: Iterable", actual.localizedMessage());
                } else {
                    Assertions.assertNull(actual);
                }
            }

            @ParameterizedTest
            @ValueSource(booleans = {true, false})
            void testArrayBoolean(final boolean isIceberg) {
                List<ColumnMetadata> columnMetadata = getArrayColumnMetadata(isIceberg);

                Map<String, Object> row = new HashMap<>();
                row.put("COLARRAY", true);

                RowSchema.Error actual = validateRowSchema(isIceberg, columnMetadata, row);
                if (isIceberg) {
                    Assertions.assertEquals("The given row cannot be converted to the internal format: Object of type java.lang.Boolean cannot be ingested into Snowflake column COLARRAY of type LIST, rowIndex:0. Allowed Java types: Iterable", actual.localizedMessage());
                } else {
                    Assertions.assertNull(actual);
                }
            }
        }
    }

    RowSchema.Error validateRowSchema(boolean isIceberg, List<ColumnMetadata> columnMetadata, Map<String, Object> row) {
        return new RowSchema(isIceberg, columnMetadata).validate(row);
    }
}
