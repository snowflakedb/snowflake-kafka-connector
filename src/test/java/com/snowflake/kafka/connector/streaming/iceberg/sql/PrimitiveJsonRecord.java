package com.snowflake.kafka.connector.streaming.iceberg.sql;

import static net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.streaming.iceberg.sql.MetadataRecord.RecordWithMetadata;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.annotation.JsonCreator;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.annotation.JsonProperty;
import net.snowflake.client.jdbc.internal.fasterxml.jackson.databind.ObjectMapper;
import org.assertj.core.api.Assertions;

public class PrimitiveJsonRecord {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().configure(FAIL_ON_UNKNOWN_PROPERTIES, false);

  private final Long idInt8;

  private final Long idInt16;

  private final Long idInt32;

  private final Long idInt64;

  private final String description;

  private final Double ratingFloat32;

  private final Double ratingFloat64;

  private final Boolean approval;

  @JsonCreator
  public PrimitiveJsonRecord(
      @JsonProperty("id_int8") Long idInt8,
      @JsonProperty("id_int16") Long idInt16,
      @JsonProperty("id_int32") Long idInt32,
      // FIXME: there is currently some bug in Iceberg when storing int64 values
      //      @JsonProperty("id_int64") Long idInt64,
      @JsonProperty("description") String description,
      @JsonProperty("rating_float32") Double ratingFloat32,
      @JsonProperty("rating_float64") Double ratingFloat64,
      @JsonProperty("approval") Boolean approval) {
    this.idInt8 = idInt8;
    this.idInt16 = idInt16;
    this.idInt32 = idInt32;
    this.idInt64 = 64L;
    this.description = description;
    this.ratingFloat32 = ratingFloat32;
    this.ratingFloat64 = ratingFloat64;
    this.approval = approval;
  }

  public static List<RecordWithMetadata<PrimitiveJsonRecord>> fromSchematizedResult(
      ResultSet resultSet) {
    List<RecordWithMetadata<PrimitiveJsonRecord>> records = new ArrayList<>();
    try {
      while (resultSet.next()) {
        PrimitiveJsonRecord record =
            new PrimitiveJsonRecord(
                resultSet.getLong("ID_INT8"),
                resultSet.getLong("ID_INT16"),
                resultSet.getLong("ID_INT32"),
                // FIXME: there is currently some bug in Iceberg when storing int64 values
                //                resultSet.getLong("ID_INT64"),
                resultSet.getString("DESCRIPTION"),
                resultSet.getDouble("RATING_FLOAT32"),
                resultSet.getDouble("RATING_FLOAT64"),
                resultSet.getBoolean("APPROVAL"));
        MetadataRecord metadata = MetadataRecord.fromMetadataSingleRow(resultSet);
        records.add(RecordWithMetadata.of(metadata, record));
      }
    } catch (SQLException e) {
      Assertions.fail("Couldn't map ResultSet to PrimitiveJsonRecord");
    }
    return records;
  }

  public static List<RecordWithMetadata<PrimitiveJsonRecord>> fromRecordContentColumn(
      ResultSet resultSet) {
    List<RecordWithMetadata<PrimitiveJsonRecord>> records = new ArrayList<>();

    try {
      while (resultSet.next()) {
        String jsonString = resultSet.getString(Utils.TABLE_COLUMN_CONTENT);
        PrimitiveJsonRecord record = MAPPER.readValue(jsonString, PrimitiveJsonRecord.class);
        MetadataRecord metadata = MetadataRecord.fromMetadataSingleRow(resultSet);
        records.add(RecordWithMetadata.of(metadata, record));
      }
    } catch (SQLException | IOException e) {
      Assertions.fail("Couldn't map ResultSet to PrimitiveJsonRecord: " + e.getMessage());
    }
    return records;
  }

  public Long getIdInt8() {
    return idInt8;
  }

  public Long getIdInt16() {
    return idInt16;
  }

  public Long getIdInt32() {
    return idInt32;
  }

  public Long getIdInt64() {
    return idInt64;
  }

  public String getDescription() {
    return description;
  }

  public Double getRatingFloat32() {
    return ratingFloat32;
  }

  public Double getRatingFloat64() {
    return ratingFloat64;
  }

  public Boolean isApproval() {
    return approval;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PrimitiveJsonRecord that = (PrimitiveJsonRecord) o;
    return Objects.equals(idInt8, that.idInt8)
        && Objects.equals(idInt16, that.idInt16)
        && Objects.equals(idInt32, that.idInt32)
        && Objects.equals(idInt64, that.idInt64)
        && Objects.equals(description, that.description)
        && Objects.equals(ratingFloat32, that.ratingFloat32)
        && Objects.equals(ratingFloat64, that.ratingFloat64)
        && Objects.equals(approval, that.approval);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        idInt8, idInt16, idInt32, idInt64, description, ratingFloat32, ratingFloat64, approval);
  }

  @Override
  public String toString() {
    return "PrimitiveJsonRecord{"
        + "idInt8="
        + idInt8
        + ", idInt16="
        + idInt16
        + ", idInt32="
        + idInt32
        + ", idInt64="
        + idInt64
        + ", description='"
        + description
        + '\''
        + ", ratingFloat32="
        + ratingFloat32
        + ", ratingFloat64="
        + ratingFloat64
        + ", approval="
        + approval
        + '}';
  }
}
