package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Streams;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.records.RecordService;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import net.snowflake.ingest.streaming.internal.ColumnProperties;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.iceberg.types.Type;
import org.apache.kafka.connect.sink.SinkRecord;

class IcebergTableSchemaResolver {

  private final IcebergColumnTypeMapper columnTypeMapper;

  @VisibleForTesting
  IcebergTableSchemaResolver(IcebergColumnTypeMapper columnTypeMapper) {
    this.columnTypeMapper = columnTypeMapper;
  }

  public IcebergTableSchemaResolver() {
    this.columnTypeMapper = IcebergColumnTypeMapper.INSTANCE;
  }

  public IcebergTableSchema resolveIcebergSchemaFromChannel(
      Map<String, ColumnProperties> tableSchemaFromChannel, List<String> columnsToInclude) {
    // tableSchemaFromChannel nie maja ciapek
    // columnsToInclude maja ciapki
    // todo remember about the case with dots
    // todo potential error when cases are different - think easy to overcome
    List<ApacheIcebergColumnSchema> apacheIcebergColumnSchemas =
        tableSchemaFromChannel.entrySet().stream()
            .filter(
                (schemaFromChannelEntry) -> {
                  String quoteChannelColumnName =
                      Utils.quoteNameIfNeeded(schemaFromChannelEntry.getKey());
                  return columnsToInclude.contains(quoteChannelColumnName);
                })
            .map(
                (schemasFromChannelEntry) -> {
                  String columnName = schemasFromChannelEntry.getKey();
                  ColumnProperties columnProperty = schemasFromChannelEntry.getValue();
                  String plainIcebergSchema = getIcebergSchema(columnProperty);
                  Type schema = IcebergDataTypeParser.deserializeIcebergType(plainIcebergSchema);
                  return new ApacheIcebergColumnSchema(schema, columnName);
                })
            .collect(Collectors.toList());

    List<IcebergColumnTree> icebergColumnTrees =
        apacheIcebergColumnSchemas.stream()
            .map(IcebergColumnTree::new)
            .collect(Collectors.toList());

    return new IcebergTableSchema(icebergColumnTrees);
  }

  // todo remove it just when we can
  private static String getIcebergSchema(ColumnProperties columnProperties) {
    try {
      // TODO reflection should be replaced by proper builder.setIceberg(true) call in SNOW-1728002
      java.lang.reflect.Field field =
          FieldUtils.getField(ColumnProperties.class, "icebergColumnSchema");
      field.setAccessible(true);
      return (String) field.get(columnProperties);
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(
          "Couldn't set iceberg by accessing private field: " + "isIceberg", e);
    }
  }

  public IcebergTableSchema resolveIcebergSchema(SinkRecord record, List<String> columnsToInclude) {
    if (columnsToInclude == null || columnsToInclude.isEmpty()) {
      return IcebergTableSchema.Empty();
    }
    Set<String> columnNamesSet = new HashSet<>(columnsToInclude);

    if (hasSchema(record)) {
      return getTableSchemaFromRecordSCHEMAIceberg(record, columnNamesSet);
    } else {
      return getTableSchemaFromJsonIceberg(record, columnNamesSet);
    }
  }

  private boolean hasSchema(SinkRecord record) {
    return record.valueSchema() != null
        && record.valueSchema().fields() != null
        && !record.valueSchema().fields().isEmpty();
  }

  private IcebergTableSchema getTableSchemaFromJsonIceberg(
      SinkRecord record, Set<String> columnNamesSet) {
    JsonNode recordNode = RecordService.convertToJson(record.valueSchema(), record.value(), true);

    List<IcebergColumnTree> icebergColumnTrees =
        Streams.stream(recordNode.fields())
            .map(IcebergColumnJsonValuePair::from)
            .filter(pair -> columnNamesSet.contains(pair.getQuotedColumnName()))
            .map(IcebergColumnTree::new)
            .collect(Collectors.toList());
    return new IcebergTableSchema(icebergColumnTrees);
  }

  private IcebergTableSchema getTableSchemaFromRecordSCHEMAIceberg(
      SinkRecord record, Set<String> columnNamesSet) {
    // todo its second part
    JsonNode recordNode = RecordService.convertToJson(record.valueSchema(), record.value(), true);
    throw new IllegalArgumentException("not yet implemented SCHEMA path");
    // return IcebergTableSchema.Empty();
  }
}
