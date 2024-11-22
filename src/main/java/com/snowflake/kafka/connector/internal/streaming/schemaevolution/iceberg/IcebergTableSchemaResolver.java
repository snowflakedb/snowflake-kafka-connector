package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.snowflake.kafka.connector.records.RecordService;
import java.util.*;
import java.util.stream.Collectors;
import net.snowflake.ingest.streaming.internal.ColumnProperties;
import org.apache.iceberg.types.Type;
import org.apache.kafka.connect.sink.SinkRecord;

class IcebergTableSchemaResolver {

  /**
   * Retrieve IcebergSchema stored in a channel, then parse it into a tree. Filter out columns that
   * do not need to be modified.
   */
  public List<IcebergColumnTree> resolveIcebergSchemaFromChannel(
      Map<String, ColumnProperties> tableSchemaFromChannel, Set<String> columnsToEvolve) {

    List<ApacheIcebergColumnSchema> apacheIcebergColumnSchemas =
        tableSchemaFromChannel.entrySet().stream()
            .filter(
                (schemaFromChannelEntry) -> {
                  String columnNameFromChannel = schemaFromChannelEntry.getKey();
                  return columnsToEvolve.contains(columnNameFromChannel);
                })
            .map(this::mapApacheSchemaFromChannel)
            .collect(Collectors.toList());

    return apacheIcebergColumnSchemas.stream()
        .map(IcebergColumnTree::new)
        .collect(Collectors.toList());
  }

  public List<IcebergColumnTree> resolveIcebergSchemaFromRecord(
      SinkRecord record, Set<String> columnsToInclude) {
    if (columnsToInclude == null || columnsToInclude.isEmpty()) {
      return ImmutableList.of();
    }
    Set<String> columnNamesSet = new HashSet<>(columnsToInclude);

    if (hasSchema(record)) {
      // TODO not yet implemented
      return getTableSchemaFromRecordSchemaIceberg(record, columnNamesSet);
    } else {
      return getTableSchemaFromJsonIceberg(record, columnNamesSet);
    }
  }

  private ApacheIcebergColumnSchema mapApacheSchemaFromChannel(
      Map.Entry<String, ColumnProperties> schemaFromChannelEntry) {

    ColumnProperties columnProperty = schemaFromChannelEntry.getValue();
    String plainIcebergSchema = getIcebergSchema(columnProperty);

    Type schema = IcebergDataTypeParser.deserializeIcebergType(plainIcebergSchema);
    String columnName = schemaFromChannelEntry.getKey();
    return new ApacheIcebergColumnSchema(schema, columnName);
  }

  // todo remove in 1820155 when getIcebergSchema() method is made public
  private static String getIcebergSchema(ColumnProperties columnProperties) {
    try {
      java.lang.reflect.Field field =
          columnProperties.getClass().getDeclaredField("icebergColumnSchema");
      field.setAccessible(true);
      return (String) field.get(columnProperties);
    } catch (IllegalAccessException | NoSuchFieldException e) {
      throw new IllegalStateException(
          "Couldn't set iceberg by accessing private field: " + "isIceberg", e);
    }
  }

  private boolean hasSchema(SinkRecord record) {
    return record.valueSchema() != null
        && record.valueSchema().fields() != null
        && !record.valueSchema().fields().isEmpty();
  }

  private List<IcebergColumnTree> getTableSchemaFromJsonIceberg(
      SinkRecord record, Set<String> columnsToEvolve) {
    JsonNode recordNode = RecordService.convertToJson(record.valueSchema(), record.value(), true);

    return Streams.stream(recordNode.fields())
        .map(IcebergColumnJsonValuePair::from)
        .filter(pair -> columnsToEvolve.contains(pair.getColumnName().toUpperCase()))
        .map(IcebergColumnTree::new)
        .collect(Collectors.toList());
  }

  private List<IcebergColumnTree> getTableSchemaFromRecordSchemaIceberg(
      SinkRecord record, Set<String> columnNamesSet) {
    JsonNode recordNode = RecordService.convertToJson(record.valueSchema(), record.value(), true);
    throw new RuntimeException("NOT YET IMPLEMENTED! - SCHEMA path");
  }
}
