package com.snowflake.kafka.connector.internal.streaming.validation;

import net.snowflake.ingest.utils.IcebergDataTypeParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static net.snowflake.ingest.utils.Utils.concatDotPath;

public class RowSchema {
    private static final ResourceBundleManager errorResourceBundleManager =
            ResourceBundleManager.getSingleton(com.snowflake.openflow.runtime.processors.snowpipe.validation.ErrorCode.errorMessageResource);
    private final Set<String> nonNullableFieldNames = new HashSet<>();
    private final Map<String, PkgParquetColumn> fieldIndex = new HashMap<>();
    private final PkgSubColumnFinder subColumnFinder;
    private final boolean isIceberg;
    private final Map<String, ColumnProperties> columnProperties = new HashMap<>();

    /**
     * Set up the parquet schema.
     *
     * @param columns top level columns list of column metadata
     */
    public RowSchema(boolean isIceberg, List<ColumnMetadata> columns) {
        this.isIceberg = isIceberg;
        List<Type> parquetTypes = new ArrayList<>();
        int id = 1;

        for (ColumnMetadata column : columns) {
            /* Set up fields using top level column information */
            validateColumnCollation(column);
            PkgParquetTypeInfo typeInfo = PkgParquetTypeGenerator.generateColumnParquetTypeInfo(column, id);
            Type parquetType = typeInfo.parquetType();
            parquetTypes.add(parquetType);
            int columnIndex = parquetTypes.size() - 1;
            fieldIndex.put(column.getInternalName(), new PkgParquetColumn(column, columnIndex, parquetType));
            columnProperties.put(column.getName(), new ColumnProperties(column));

            if (!column.getNullable()) {
                addNonNullableFieldName(column.getInternalName());
            }

            id++;
        }

        String parquetMessageTypeName = isIceberg ? PARQUET_MESSAGE_TYPE_NAME : BDEC_PARQUET_MESSAGE_TYPE_NAME;
        MessageType schema = new MessageType(parquetMessageTypeName, parquetTypes);

        /*
         * Iceberg mode requires stats for all primitive columns and sub-columns, set them up here.
         *
         * There are two values that are used to identify a column in the stats map:
         *   1. ordinal - The ordinal is the index of the top level column in the schema.
         *   2. fieldId - The fieldId is the id of all sub-columns in the schema.
         *                It's indexed by the level and order of the column in the schema.
         *                Note that the fieldId is set to 0 for non-structured columns.
         *
         * For example, consider the following schema:
         *   F1 INT,
         *   F2 STRUCT(F21 STRUCT(F211 INT), F22 INT),
         *   F3 INT,
         *   F4 MAP(INT, MAP(INT, INT)),
         *   F5 INT,
         *   F6 ARRAY(INT),
         *   F7 INT
         *
         * The ordinal and fieldId  will look like this:
         *   F1:             ordinal=1, fieldId=1
         *   F2:             ordinal=2, fieldId=2
         *   F2.F21:         ordinal=2, fieldId=8
         *   F2.F21.F211:    ordinal=2, fieldId=13
         *   F2.F22:         ordinal=2, fieldId=9
         *   F3:             ordinal=3, fieldId=3
         *   F4:             ordinal=4, fieldId=4
         *   F4.key:         ordinal=4, fieldId=10
         *   F4.value:       ordinal=4, fieldId=11
         *   F4.value.key:   ordinal=4, fieldId=14
         *   F4.value.value: ordinal=4, fieldId=15
         *   F5:             ordinal=5, fieldId=5
         *   F6:             ordinal=6, fieldId=6
         *   F6.element:     ordinal=6, fieldId=12
         *   F7:             ordinal=7, fieldId=7
         *
         * The stats map will contain the following entries:
         *   F1:             ordinal=1, fieldId=0
         *   F2:             ordinal=2, fieldId=0
         *   F2.F21.F211:    ordinal=2, fieldId=13
         *   F2.F22:         ordinal=2, fieldId=9
         *   F3:             ordinal=3, fieldId=0
         *   F4.key:         ordinal=4, fieldId=10
         *   F4.value.key:   ordinal=4, fieldId=14
         *   F4.value.value: ordinal=4, fieldId=15
         *   F5:             ordinal=5, fieldId=0
         *   F6.element:     ordinal=6, fieldId=12
         *   F7:             ordinal=7, fieldId=0
         */
        if (isIceberg) {
            for (ColumnDescriptor columnDescriptor : schema.getColumns()) {
                String[] path = columnDescriptor.getPath();
                String columnDotPath = concatDotPath(path);
                PrimitiveType primitiveType = columnDescriptor.getPrimitiveType();

                if (path.length > 1
                        && schema
                        .getType(Arrays.copyOf(path, path.length - 1))
                        .isRepetition(Type.Repetition.REPEATED)) {
                    if (!primitiveType.getName().equals(IcebergDataTypeParser.ELEMENT)
                            && !primitiveType.getName().equals(IcebergDataTypeParser.KEY)
                            && !primitiveType.getName().equals(IcebergDataTypeParser.VALUE)) {
                        throw new PkgSFException(
                                ErrorCode.INTERNAL_ERROR,
                                String.format(
                                        "Invalid repeated column %s, column name must be %s, %s or %s",
                                        columnDotPath,
                                        IcebergDataTypeParser.ELEMENT,
                                        IcebergDataTypeParser.KEY,
                                        IcebergDataTypeParser.VALUE));
                    }
                }
            }
            subColumnFinder = new PkgSubColumnFinder(schema);
        } else {
            subColumnFinder = null;
        }
    }



    /**
     * Adds non-nullable field name. It is used to check if all non-nullable fields have been
     * provided.
     *
     * @param nonNullableFieldName non-nullable filed name
     */
    void addNonNullableFieldName(String nonNullableFieldName) {
        nonNullableFieldNames.add(nonNullableFieldName);
    }

    private static final String BDEC_PARQUET_MESSAGE_TYPE_NAME = "bdec";
    private static final String PARQUET_MESSAGE_TYPE_NAME = "schema";

    /** Throws an exception if the column has a collation defined. */
    void validateColumnCollation(ColumnMetadata column) {
        if (column.getCollation() != null) {
            throw new PkgSFException(
                    ErrorCode.UNSUPPORTED_DATA_TYPE,
                    String.format(
                            "Column %s with collation %s detected. Ingestion into collated columns is not"
                                    + " supported",
                            column.getName(), column.getCollation()));
        }
    }

    PkgParquetColumn get(String name) {
        return fieldIndex.get(name);
    }

    Set<String> getNonNullableFieldNames() {
        return nonNullableFieldNames;
    }

    PkgSubColumnFinder getSubColumnFinder() {
        return subColumnFinder;
    }

    boolean hasColumn(String name) {
        return fieldIndex.containsKey(name);
    }

    boolean isIceberg() {
        return isIceberg;
    }

    public Error validate(Map<String, Object> row) {
        PkgInsertError insertError = new PkgInsertError(row, 0);
        try {
            RowSchemaValidator.validateRowColumns(this, row, insertError, 0);
            RowSchemaValidator.validateRowValues(this, row, insertError, 0);
            return null;
        } catch (PkgSFException e) {
            return errorWith(insertError, e, e.getVendorCode(), e.getParams());
        } catch (Throwable th) {
            return errorWith(insertError, th, ErrorCode.INTERNAL_ERROR.getMessageCode(), th.getMessage());
        }
    }

    private Error errorWith(PkgInsertError insertError, Throwable th, String vendorCode, Object... params) {
        return new Error(insertError.getExtraColNames(), insertError.getMissingNotNullColNames(), insertError.getNullValueForNotNullColNames(), errorResourceBundleManager.getLocalizedMessage(vendorCode, params), vendorCode, th);
    }

    public Map<String, ColumnProperties> getColumnProperties() {
        return columnProperties;
    }

    public record Error(List<String> extraColNames, List<String> missingNotNullColNames,
                        List<String> nullValueForNotNullColNames, String localizedMessage, String vendorCode,
                        Throwable cause) {
    }
}
