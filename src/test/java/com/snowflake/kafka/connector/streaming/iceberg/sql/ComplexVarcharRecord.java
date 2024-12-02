package com.snowflake.kafka.connector.streaming.iceberg.sql;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.assertj.core.api.Assertions;

public class ComplexVarcharRecord {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(FAIL_ON_UNKNOWN_PROPERTIES, false);
  private final String varcharColumn;
  private final List<String> varcharListColumn;
  private final VarcharRecord objectColumn;

  public static final String INT_EXAMPLE =
      "{"
          + "  \"varchar_column\": 8,"
          + "  \"varchar_list_column\": [16, 24],"
          + "  \"object_column\": {"
          + "    \"varchar_column\": 32,"
          + "    \"varchar_list_column\": [64, 128]"
          + "  }"
          + "}";

  public static final String STRING_EXAMPLE =
      "{"
          + "  \"varchar_column\": \"eight\","
          + "  \"varchar_list_column\": [\"sixteen\", \"twenty-four\"],"
          + "  \"object_column\": {"
          + "    \"varchar_column\": \"thirty-two\","
          + "    \"varchar_list_column\": [\"sixty-four\", \"one-twenty-eight\"]"
          + "  }"
          + "}";

  public static final String DOUBLE_EXAMPLE =
      "{"
          + "  \"varchar_column\": 8.5,"
          + "  \"varchar_list_column\": [16.5, 24.5],"
          + "  \"object_column\": {"
          + "    \"varchar_column\": 32.5,"
          + "    \"varchar_list_column\": [64.5, 128.5]"
          + "  }"
          + "}";

  public static final String BOOLEAN_EXAMPLE =
      "{"
          + "  \"varchar_column\": true,"
          + "  \"varchar_list_column\": [false, true],"
          + "  \"object_column\": {"
          + "    \"varchar_column\": false,"
          + "    \"varchar_list_column\": [true, false]"
          + "  }"
          + "}";

  public static final String OBJECT_EXAMPLE =
      "{"
          + "  \"varchar_column\": {\"object\": 1},"
          + "  \"varchar_list_column\": [{\"object\": 1}],"
          + "  \"object_column\": {"
          + "    \"varchar_column\": {\"object\": 1},"
          + "    \"varchar_list_column\": [{\"object\": 1}]"
          + "  }"
          + "}";

  public static final String LIST_EXAMPLE =
      "{"
          + "  \"varchar_column\": [8],"
          + "  \"varchar_list_column\": [[16], [24]],"
          + "  \"object_column\": {"
          + "    \"varchar_column\": [32],"
          + "    \"varchar_list_column\": [[64], [128]]"
          + "  }"
          + "}";

  @JsonCreator
  public ComplexVarcharRecord(
      @JsonProperty("varchar_column") String varcharColumn,
      @JsonProperty("varchar_list_column") List<String> varcharListColumn,
      @JsonProperty("object_column") VarcharRecord objectColumn) {
    this.varcharColumn = varcharColumn;
    this.varcharListColumn = varcharListColumn;
    this.objectColumn = objectColumn;
  }

  public String getVarcharColumn() {
    return varcharColumn;
  }

  public List<String> getVarcharListColumn() {
    return varcharListColumn;
  }

  public VarcharRecord getObjectColumn() {
    return objectColumn;
  }

  public static List<MetadataRecord.RecordWithMetadata<ComplexVarcharRecord>> fromSchematizedResult(
      ResultSet resultSet) {
    List<MetadataRecord.RecordWithMetadata<ComplexVarcharRecord>> records = new ArrayList<>();
    try {
      while (resultSet.next()) {
        ComplexVarcharRecord complexVarcharRecord =
            new ComplexVarcharRecord(
                resultSet.getString("VARCHAR_COLUMN"),
                MAPPER.readValue(
                    resultSet.getString("VARCHAR_LIST_COLUMN"),
                    new TypeReference<List<String>>() {}),
                MAPPER.readValue(resultSet.getString("OBJECT_COLUMN"), VarcharRecord.class));
        MetadataRecord metadata = MetadataRecord.fromMetadataSingleRow(resultSet);
        records.add(MetadataRecord.RecordWithMetadata.of(metadata, complexVarcharRecord));
      }
    } catch (SQLException | IOException e) {
      Assertions.fail("Couldn't map ResultSet to PrimitiveJsonRecord");
    }
    return records;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ComplexVarcharRecord that = (ComplexVarcharRecord) o;
    return Objects.equals(varcharColumn, that.varcharColumn)
        && Objects.equals(varcharListColumn, that.varcharListColumn)
        && Objects.equals(objectColumn, that.objectColumn);
  }

  @Override
  public int hashCode() {
    return Objects.hash(varcharColumn, varcharListColumn, objectColumn);
  }

  @Override
  public String toString() {
    return "ComplexVarcharRecord{"
        + "varcharColumn='"
        + varcharColumn
        + '\''
        + ", varcharListColumn="
        + varcharListColumn
        + ", objectColumn="
        + objectColumn
        + '}';
  }

  public static class VarcharRecord {
    private final String varcharColumn;
    private final List<String> varcharListColumn;

    @JsonCreator
    public VarcharRecord(
        @JsonProperty("varchar_column") String varcharColumn,
        @JsonProperty("varchar_list_column") List<String> varcharListColumn) {
      this.varcharColumn = varcharColumn;
      this.varcharListColumn = varcharListColumn;
    }

    public String getVarcharColumn() {
      return varcharColumn;
    }

    public List<String> getVarcharListColumn() {
      return varcharListColumn;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      VarcharRecord example = (VarcharRecord) o;
      return Objects.equals(varcharColumn, example.varcharColumn)
          && Objects.equals(varcharListColumn, example.varcharListColumn);
    }

    @Override
    public int hashCode() {
      return Objects.hash(varcharColumn, varcharListColumn);
    }

    @Override
    public String toString() {
      return "VarcharRecord{"
          + "varcharColumn='"
          + varcharColumn
          + '\''
          + ", varcharListColumn="
          + varcharListColumn
          + '}';
    }
  }
}
