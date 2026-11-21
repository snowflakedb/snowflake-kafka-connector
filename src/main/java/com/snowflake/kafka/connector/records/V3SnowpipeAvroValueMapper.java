package com.snowflake.kafka.connector.records;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

/**
 * Maps a generic Avro datum to plain Java values (maps, lists, strings, numbers, booleans, null)
 * that serialize to the same JSON as Avro's {@code GenericData.toString} rendering with decimal
 * conversion enabled.
 */
final class V3SnowpipeAvroValueMapper {

  private V3SnowpipeAvroValueMapper() {}

  /**
   * Maps {@code datum}, written with {@code schema}, to its JSON-compatible value:
   *
   * <ul>
   *   <li>unions resolve to the active branch value, without a branch wrapper
   *   <li>logical date, time, and timestamp types stay as their raw Avro {@code int} or {@code
   *       long}
   *   <li>decimals become a {@code double}, matching the precision of the JSON number Avro's
   *       rendering was parsed into
   *   <li>plain {@code bytes} become an ISO-8859-1 string and plain {@code fixed} a list of signed
   *       byte values
   *   <li>non-finite floating-point values become {@code "NaN"}, {@code "Infinity"}, or {@code
   *       "-Infinity"}
   * </ul>
   */
  static Object toValue(Schema schema, Object datum) {
    if (datum == null) {
      return null;
    }
    if (schema == null) {
      if (datum instanceof GenericContainer) {
        return toValue(((GenericContainer) datum).getSchema(), datum);
      }
      return datum instanceof CharSequence ? datum.toString() : datum;
    }

    Schema effective =
        schema.getType() == Schema.Type.UNION
            ? schema.getTypes().get(GenericData.get().resolveUnion(schema, datum))
            : schema;
    switch (effective.getType()) {
      case RECORD:
        return mapRecord((GenericRecord) datum, effective);
      case ARRAY:
        return mapArray((Collection<?>) datum, effective);
      case MAP:
        return mapMap((Map<?, ?>) datum, effective);
      case BYTES:
        return mapBytes((ByteBuffer) datum, effective);
      case FIXED:
        return mapFixed((GenericData.Fixed) datum, effective);
      case FLOAT:
        return Float.isFinite((Float) datum) ? datum : datum.toString();
      case DOUBLE:
        return Double.isFinite((Double) datum) ? datum : datum.toString();
      case INT:
      case LONG:
      case BOOLEAN:
        return datum;
      case NULL:
        return null;
      default:
        // STRING and ENUM
        return datum.toString();
    }
  }

  private static Map<String, Object> mapRecord(GenericRecord record, Schema schema) {
    Map<String, Object> result = new LinkedHashMap<>();
    for (Schema.Field field : schema.getFields()) {
      result.put(field.name(), toValue(field.schema(), record.get(field.pos())));
    }
    return result;
  }

  private static List<Object> mapArray(Collection<?> array, Schema schema) {
    List<Object> result = new ArrayList<>(array.size());
    for (Object element : array) {
      result.add(toValue(schema.getElementType(), element));
    }
    return result;
  }

  private static Map<String, Object> mapMap(Map<?, ?> map, Schema schema) {
    Map<String, Object> result = new LinkedHashMap<>();
    for (Map.Entry<?, ?> entry : map.entrySet()) {
      result.put(entry.getKey().toString(), toValue(schema.getValueType(), entry.getValue()));
    }
    return result;
  }

  private static Object mapBytes(ByteBuffer datum, Schema schema) {
    ByteBuffer buffer = datum.duplicate();
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType instanceof LogicalTypes.Decimal) {
      byte[] bytes = new byte[buffer.remaining()];
      buffer.get(bytes);
      return toDouble(bytes, (LogicalTypes.Decimal) logicalType);
    }
    return StandardCharsets.ISO_8859_1.decode(buffer).toString();
  }

  private static Object mapFixed(GenericData.Fixed datum, Schema schema) {
    LogicalType logicalType = schema.getLogicalType();
    if (logicalType instanceof LogicalTypes.Decimal) {
      return toDouble(datum.bytes(), (LogicalTypes.Decimal) logicalType);
    }
    List<Integer> bytes = new ArrayList<>(datum.bytes().length);
    for (byte value : datum.bytes()) {
      bytes.add((int) value);
    }
    return bytes;
  }

  private static double toDouble(byte[] unscaled, LogicalTypes.Decimal decimal) {
    return new BigDecimal(new BigInteger(unscaled), decimal.getScale()).doubleValue();
  }
}
