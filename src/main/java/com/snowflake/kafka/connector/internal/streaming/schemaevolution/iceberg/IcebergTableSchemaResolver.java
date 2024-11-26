package com.snowflake.kafka.connector.internal.streaming.schemaevolution.iceberg;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import com.snowflake.kafka.connector.records.RecordService;
import java.util.*;
import java.util.stream.Collectors;
import net.snowflake.ingest.streaming.internal.ColumnProperties;
import org.apache.iceberg.types.Type;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class IcebergTableSchemaResolver {

  private final IcebergColumnTypeMapper mapper = IcebergColumnTypeMapper.INSTANCE;

  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergTableSchemaResolver.class);

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
      SinkRecord record, Set<String> columnsToEvolve) {
    if (columnsToEvolve == null || columnsToEvolve.isEmpty()) {
      return ImmutableList.of();
    }
    if (hasSchema(record)) {
      return getTableSchemaFromRecordSchema(record, columnsToEvolve);
    } else {
      return getTableSchemaFromJson(record, columnsToEvolve);
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

  private List<IcebergColumnTree> getTableSchemaFromJson(
      SinkRecord record, Set<String> columnsToEvolve) {
    JsonNode recordNode = RecordService.convertToJson(record.valueSchema(), record.value(), true);

    return Streams.stream(recordNode.fields())
        .map(IcebergColumnJsonValuePair::from)
        .filter(pair -> columnsToEvolve.contains(pair.getColumnName().toUpperCase()))
        .map(IcebergColumnTree::new)
        .collect(Collectors.toList());
  }

  /**
   * Given a SinkRecord, get the schema information from it
   *
   * @param record the sink record that contains the schema and actual data
   * @return list of column represantation in a form of tree
   */
  private List<IcebergColumnTree> getTableSchemaFromRecordSchema(
      SinkRecord record, Set<String> columnsToEvolve) {

    Schema schema = record.valueSchema();

    if (schema != null && schema.fields() != null) {
      ArrayList<Field> foundColumns = new ArrayList<>();
      ArrayList<Field> notFoundColumns = new ArrayList<>();

      for (Field field : schema.fields()) {
        if (columnsToEvolve.contains(field.name().toUpperCase())) {
          foundColumns.add(field);
        } else {
          notFoundColumns.add(field);
        }
      }

      if (!notFoundColumns.isEmpty()) {
        throw SnowflakeErrors.ERROR_5022.getException(
            "Columns not found in schema: "
                + notFoundColumns.stream().map(Field::name).collect(Collectors.toList())
                + ", schemaColumns: "
                + schema.fields().stream().map(Field::name).collect(Collectors.toList()));
      }
      return foundColumns.stream().map(IcebergColumnTree::new).collect(Collectors.toList());
    }
    return ImmutableList.of();
  }
}
