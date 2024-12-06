package com.snowflake.kafka.connector.streaming.iceberg.sql;

import static com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.streaming.iceberg.sql.MetadataRecord.RecordWithMetadata;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.assertj.core.api.Assertions;

public class ComplexJsonRecord {

  public static final ComplexJsonRecord complexJsonRecordValueExample =
      new ComplexJsonRecord(
          8L,
          16L,
          32L,
          64L,
          "dogs are the best",
          0.5,
          0.25,
          true,
          ImmutableList.of(1, 2, 3),
          ImmutableList.of("a", "b", "c"),
          ImmutableList.of(true),
          ImmutableList.of(1, 4),
          ImmutableList.of(ImmutableList.of(7, 8, 9), ImmutableList.of(10, 11, 12)),
          PrimitiveJsonRecord.primitiveJsonRecordValueExample,
          PrimitiveJsonRecord.primitiveJsonRecordValueExample);

  public static final String complexJsonPayloadExample =
      "{"
          + "  \"id_int8\": 8,"
          + "  \"id_int16\": 16,"
          + "  \"id_int32\": 32,"
          + "  \"id_int64\": 64,"
          + "  \"description\": \"dogs are the best\","
          + "  \"rating_float32\": 0.5,"
          + "  \"rating_float64\": 0.25,"
          + "  \"approval\": true,"
          + "  \"array1\": [1, 2, 3],"
          + "  \"array2\": [\"a\", \"b\", \"c\"],"
          + "  \"array3\": [true],"
          + "  \"array4\": [1, 4],"
          + "  \"array5\": [[7, 8, 9], [10, 11, 12]],"
          + "  \"nestedRecord\": "
          + PrimitiveJsonRecord.primitiveJsonExample
          + ","
          + "  \"nestedRecord2\": "
          + PrimitiveJsonRecord.primitiveJsonExample
          + "}";

  public static final String complexJsonPayloadWithWrongValueTypeExample =
      "{"
          + "  \"id_int8\": 8,"
          + "  \"id_int16\": 16,"
          + "  \"id_int32\": 32,"
          + "  \"id_int64\": 64,"
          + "  \"description\": \"dogs are the best\","
          + "  \"rating_float32\": 0.5,"
          + "  \"rating_float64\": 0.25,"
          + "  \"approval\": true,"
          + "  \"array1\": [1, 2, 3],"
          + "  \"array2\": [\"a\", \"b\", \"c\"],"
          + "  \"array3\": [true],"
          + "  \"array4\": [1, 4],"
          + "  \"array5\": [[7, 8, 9], [10, 11, 12]],"
          + "  \"nestedRecord\": "
          + PrimitiveJsonRecord.primitiveJsonExample
          + ","
          + "  \"nestedRecord2\": 25"
          + "}";

  public static final String complexJsonWithSchemaExample =
      "{"
          + "  \"schema\": {"
          + "    \"type\": \"struct\","
          + "    \"fields\": ["
          + "      {"
          + "        \"field\": \"id_int8\","
          + "        \"type\": \"int8\""
          + "      },"
          + "      {"
          + "        \"field\": \"id_int16\","
          + "        \"type\": \"int16\""
          + "      },"
          + "      {"
          + "        \"field\": \"id_int32\","
          + "        \"type\": \"int32\""
          + "      },"
          + "      {"
          + "        \"field\": \"id_int64\","
          + "        \"type\": \"int64\""
          + "      },"
          + "      {"
          + "        \"field\": \"description\","
          + "        \"type\": \"string\""
          + "      },"
          + "      {"
          + "        \"field\": \"rating_float32\","
          + "        \"type\": \"float\""
          + "      },"
          + "      {"
          + "        \"field\": \"rating_float64\","
          + "        \"type\": \"double\""
          + "      },"
          + "      {"
          + "        \"field\": \"approval\","
          + "        \"type\": \"boolean\""
          + "      },"
          + "      {"
          + "        \"field\": \"array1\","
          + "          \"type\": \"array\","
          + "          \"items\": {"
          + "            \"type\": \"int32\""
          + "          }"
          + "      },"
          + "      {"
          + "        \"field\": \"array2\","
          + "          \"type\": \"array\","
          + "          \"items\": {"
          + "            \"type\": \"string\""
          + "          }"
          + "      },"
          + "      {"
          + "        \"field\": \"array3\","
          + "          \"type\": \"array\","
          + "          \"items\": {"
          + "            \"type\": \"boolean\""
          + "          }"
          + "      },"
          + "      {"
          + "        \"field\": \"array4\","
          + "          \"type\": \"array\","
          + "          \"items\": {"
          + "            \"type\": \"int32\""
          + "          },"
          + "          \"optional\": true"
          + "      },"
          + "      {"
          + "        \"field\": \"array5\","
          + "          \"type\": \"array\","
          + "          \"items\": {"
          + "            \"type\": \"array\","
          + "          \"items\": {"
          + "            \"type\": \"int32\""
          + "          }"
          + "          }"
          + "      },"
          + "      {"
          + "        \"field\": \"nestedRecord\","
          + "          \"type\": \"struct\","
          + "          \"fields\": ["
          + "            {"
          + "              \"field\": \"id_int8\","
          + "              \"type\": \"int8\""
          + "            },"
          + "            {"
          + "              \"field\": \"id_int16\","
          + "              \"type\": \"int16\""
          + "            },"
          + "            {"
          + "              \"field\": \"id_int32\","
          + "              \"type\": \"int32\""
          + "            },"
          + "            {"
          + "              \"field\": \"id_int64\","
          + "              \"type\": \"int64\""
          + "            },"
          + "            {"
          + "              \"field\": \"description\","
          + "              \"type\": \"string\""
          + "            },"
          + "            {"
          + "              \"field\": \"rating_float32\","
          + "              \"type\": \"float\""
          + "            },"
          + "            {"
          + "              \"field\": \"rating_float64\","
          + "              \"type\": \"double\""
          + "            },"
          + "            {"
          + "              \"field\": \"approval\","
          + "              \"type\": \"boolean\""
          + "            }"
          + "          ],"
          + "          \"optional\": true,"
          + "          \"name\": \"sf.kc.test\""
          + "      },"
          + "      {"
          + "        \"field\": \"nestedRecord2\","
          + "          \"type\": \"struct\","
          + "          \"fields\": ["
          + "            {"
          + "              \"field\": \"id_int8\","
          + "              \"type\": \"int8\""
          + "            },"
          + "            {"
          + "              \"field\": \"id_int16\","
          + "              \"type\": \"int16\""
          + "            },"
          + "            {"
          + "              \"field\": \"id_int32\","
          + "              \"type\": \"int32\""
          + "            },"
          + "            {"
          + "              \"field\": \"id_int64\","
          + "              \"type\": \"int64\""
          + "            },"
          + "            {"
          + "              \"field\": \"description\","
          + "              \"type\": \"string\""
          + "            },"
          + "            {"
          + "              \"field\": \"rating_float32\","
          + "              \"type\": \"float\""
          + "            },"
          + "            {"
          + "              \"field\": \"rating_float64\","
          + "              \"type\": \"double\""
          + "            },"
          + "            {"
          + "              \"field\": \"approval\","
          + "              \"type\": \"boolean\""
          + "            }"
          + "          ],"
          + "          \"optional\": true,"
          + "          \"name\": \"sf.kc.test\""
          + "      }"
          + "    ],"
          + "    \"optional\": false,"
          + "    \"name\": \"sf.kc.test\""
          + "  },"
          + "  \"payload\": "
          + complexJsonPayloadExample
          + "}";

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

  private final List<Integer> array1;
  private final List<String> array2;
  private final List<Boolean> array3;
  private final List<Integer> array4;
  private final List<List<Integer>> array5;

  private final PrimitiveJsonRecord nestedRecord;
  private final PrimitiveJsonRecord nestedRecord2;

  @JsonCreator
  public ComplexJsonRecord(
      @JsonProperty("id_int8") Long idInt8,
      @JsonProperty("id_int16") Long idInt16,
      @JsonProperty("id_int32") Long idInt32,
      @JsonProperty("id_int64") Long idInt64,
      @JsonProperty("description") String description,
      @JsonProperty("rating_float32") Double ratingFloat32,
      @JsonProperty("rating_float64") Double ratingFloat64,
      @JsonProperty("approval") Boolean approval,
      @JsonProperty("array1") List<Integer> array1,
      @JsonProperty("array2") List<String> array2,
      @JsonProperty("array3") List<Boolean> array3,
      @JsonProperty("array4") List<Integer> array4,
      @JsonProperty("array5") List<List<Integer>> array5,
      @JsonProperty("nestedRecord") PrimitiveJsonRecord nestedRecord,
      @JsonProperty("nestedRecord2") PrimitiveJsonRecord nestedRecord2) {
    this.idInt8 = idInt8;
    this.idInt16 = idInt16;
    this.idInt32 = idInt32;
    this.idInt64 = idInt64;
    this.description = description;
    this.ratingFloat32 = ratingFloat32;
    this.ratingFloat64 = ratingFloat64;
    this.approval = approval;
    this.array1 = array1;
    this.array2 = array2;
    this.array3 = array3;
    this.array4 = array4;
    this.array5 = array5;
    this.nestedRecord = nestedRecord;
    this.nestedRecord2 = nestedRecord2;
  }

  public static List<RecordWithMetadata<ComplexJsonRecord>> fromRecordContentColumn(
      ResultSet resultSet) {
    List<RecordWithMetadata<ComplexJsonRecord>> records = new ArrayList<>();

    try {
      while (resultSet.next()) {
        String jsonString = resultSet.getString(Utils.TABLE_COLUMN_CONTENT);
        ComplexJsonRecord record = MAPPER.readValue(jsonString, ComplexJsonRecord.class);
        MetadataRecord metadata = MetadataRecord.fromMetadataSingleRow(resultSet);
        records.add(RecordWithMetadata.of(metadata, record));
      }
    } catch (SQLException | IOException e) {
      Assertions.fail("Couldn't map ResultSet to ComplexJsonRecord: " + e.getMessage());
    }
    return records;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ComplexJsonRecord that = (ComplexJsonRecord) o;
    return Objects.equals(idInt8, that.idInt8)
        && Objects.equals(idInt16, that.idInt16)
        && Objects.equals(idInt32, that.idInt32)
        && Objects.equals(idInt64, that.idInt64)
        && Objects.equals(description, that.description)
        && Objects.equals(ratingFloat32, that.ratingFloat32)
        && Objects.equals(ratingFloat64, that.ratingFloat64)
        && Objects.equals(approval, that.approval)
        && Objects.equals(array1, that.array1)
        && Objects.equals(array2, that.array2)
        && Objects.equals(array3, that.array3)
        && Objects.equals(array4, that.array4)
        && Objects.equals(array5, that.array5)
        && Objects.equals(nestedRecord, that.nestedRecord)
        && Objects.equals(nestedRecord2, that.nestedRecord2);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        idInt8,
        idInt16,
        idInt32,
        idInt64,
        description,
        ratingFloat32,
        ratingFloat64,
        approval,
        array1,
        array2,
        array3,
        array4,
        array5,
        nestedRecord,
        nestedRecord2);
  }

  @Override
  public String toString() {
    return "ComplexJsonRecord{"
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
        + ", array1="
        + array1
        + ", array2="
        + array2
        + ", array3="
        + array3
        + ", array4="
        + array4
        + ", array5="
        + array5
        + ", nestedRecord="
        + nestedRecord
        + ", nestedRecord2="
        + nestedRecord2
        + '}';
  }
}
