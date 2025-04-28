/*
 * Copyright (c) 2021-2024 Snowflake Computing Inc. All rights reserved.
 */

package com.snowflake.kafka.connector.internal.streaming.validation;

import static com.snowflake.kafka.connector.internal.streaming.validation.DuplicateKeyValidatingSerializer.stripTrailingNulls;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.function.Supplier;
import net.snowflake.client.jdbc.internal.snowflake.common.core.SnowflakeDateTimeFormat;
import net.snowflake.client.jdbc.internal.snowflake.common.util.Power10;
import net.snowflake.ingest.streaming.internal.TimestampWrapper;
import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.output.StringBuilderWriter;

/** Utility class for parsing and validating inputs based on Snowflake types */
class PkgDataValidationUtil {

  /**
   * Seconds limit used for integer-stored timestamp scale guessing. Value needs to be aligned with
   * the value from {@link SnowflakeDateTimeFormat#parse}
   */
  private static final long SECONDS_LIMIT_FOR_EPOCH = 31536000000L;

  /**
   * Milliseconds limit used for integer-stored timestamp scale guessing. Value needs to be aligned
   * with the value from {@link SnowflakeDateTimeFormat#parse}
   */
  private static final long MILLISECONDS_LIMIT_FOR_EPOCH = SECONDS_LIMIT_FOR_EPOCH * 1000L;

  /**
   * Microseconds limit used for integer-stored timestamp scale guessing. Value needs to be aligned
   * with the value from {@link SnowflakeDateTimeFormat#parse}
   */
  private static final long MICROSECONDS_LIMIT_FOR_EPOCH = SECONDS_LIMIT_FOR_EPOCH * 1000000L;

  public static final int BYTES_8_MB = 8 * 1024 * 1024;
  public static final int BYTES_16_MB = 2 * BYTES_8_MB;

  // TODO SNOW-664249: There is a few-byte mismatch between the value sent by the user and its
  // server-side representation. Validation leaves a small buffer for this difference.
  static final int MAX_SEMI_STRUCTURED_LENGTH = BYTES_16_MB - 64;

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private static final JsonFactory factory =
      new JsonFactory()
          // Handle duplicate fields in JSON objects by ourselves
          .configure(JsonGenerator.Feature.STRICT_DUPLICATE_DETECTION, false);

  // The version of Jackson we are using does not support serialization of date objects from the
  // java.time package. Here we define a module with custom java.time serializers. Additionally, we
  // define custom serializer for byte[] because the Jackson default is to serialize it as
  // base64-encoded string, and we would like to serialize it as JSON array of numbers.
  static {
    SimpleModule module = new SimpleModule();
    module.<byte[]>addSerializer(byte[].class, new ByteArraySerializer());
    module.addSerializer(ZonedDateTime.class, new ZonedDateTimeSerializer());
    module.addSerializer(LocalTime.class, new ToStringSerializer());
    module.addSerializer(OffsetTime.class, new ToStringSerializer());
    module.addSerializer(LocalDate.class, new ToStringSerializer());
    module.addSerializer(LocalDateTime.class, new ToStringSerializer());
    module.addSerializer(OffsetDateTime.class, new ToStringSerializer());
    module.addSerializer(DuplicateKeyValidatedObject.class, new DuplicateKeyValidatingSerializer());
    objectMapper.registerModule(module);
  }

  private static final ObjectWriter objectWriter = objectMapper.writer();

  // Caching the powers of 10 that are used for checking the range of numbers because computing them
  // on-demand is expensive.
  private static final BigDecimal[] POWER_10 = makePower10Table();

  private static BigDecimal[] makePower10Table() {
    BigDecimal[] power10 = new BigDecimal[Power10.sb16Size];
    for (int i = 0; i < Power10.sb16Size; i++) {
      power10[i] = new BigDecimal(Power10.sb16Table[i]);
    }
    return power10;
  }

  /**
   * Validates and parses input as JSON. All types in the object tree must be valid variant types,
   * see {@link PkgDataValidationUtil#isAllowedSemiStructuredType}.
   *
   * @param input Object to validate
   * @return JSON tree representing the input
   */
  private static JsonNode validateAndParseSemiStructuredAsJsonTree(
      String columnName, Object input, String snowflakeType, final long insertRowIndex) {
    if (input instanceof String) {
      String stringInput = (String) input;
      verifyValidUtf8(stringInput, columnName, snowflakeType, insertRowIndex);
      try {
        return objectMapper.readTree(stringInput);
      } catch (JsonProcessingException e) {
        throw valueFormatNotAllowedException(
            columnName, snowflakeType, "Not a valid JSON", insertRowIndex);
      }
    } else if (isAllowedSemiStructuredType(input)) {
      return objectMapper.valueToTree(input);
    }

    throw typeNotAllowedException(
        columnName,
        input.getClass(),
        snowflakeType,
        new String[] {
          "String",
          "Primitive data types and their arrays",
          "java.time.*",
          "List<T>",
          "Map<String, T>",
          "T[]"
        },
        insertRowIndex);
  }

  /**
   * Validates and parses input as JSON. All types in the object tree must be valid variant types,
   * see {@link PkgDataValidationUtil#isAllowedSemiStructuredType}.
   *
   * @param input Object to validate
   * @return Minified JSON string
   */
  private static String validateAndParseSemiStructured(
      String columnName, Object input, String snowflakeType, final long insertRowIndex) {
    if (input instanceof String) {
      final String stringInput = (String) input;
      verifyValidUtf8(stringInput, columnName, snowflakeType, insertRowIndex);
      final StringBuilderWriter resultWriter = new StringBuilderWriter(stringInput.length());
      Stack<DuplicateDetector<String>> fieldsByLevel = new Stack<>();
      try (final JsonParser parser = factory.createParser(stringInput);
          final JsonGenerator generator = factory.createGenerator(resultWriter)) {
        while (parser.nextToken() != null) {
          final JsonToken token = parser.currentToken();
          if (token.isNumeric()) {
            // If the current token is a number, we cannot just copy the current event because it
            // would write token the token from double (or big decimal), whose scientific notation
            // may have been altered during deserialization. We want to preserve the scientific
            // notation from the user input, so we write the current numer as text.
            generator.writeNumber(parser.getText());
          } else {
            // Validates duplicate JSON object fields
            if (token == JsonToken.START_OBJECT) {
              fieldsByLevel.push(new DuplicateDetector<>());
            }
            if (token == JsonToken.END_OBJECT) {
              fieldsByLevel.pop();
            }
            if (token == JsonToken.FIELD_NAME) {
              // We need to strip trailing nulls from the field name to match the behavior of the
              // server side json parser. See SNOW-1772196 for more details.
              String strippedFieldName = stripTrailingNulls(parser.currentName());
              if (fieldsByLevel.peek().isDuplicate(strippedFieldName)) {
                throw valueFormatNotAllowedException(
                    columnName,
                    snowflakeType,
                    String.format("Not a valid JSON: duplicate field %s", strippedFieldName),
                    insertRowIndex);
              }
            }
            generator.copyCurrentEvent(parser);
          }
        }
      } catch (JsonParseException e) {
        throw valueFormatNotAllowedException(
            columnName, snowflakeType, "Not a valid JSON", insertRowIndex);
      } catch (IOException e) {
        if (e.getMessage().contains("Duplicate field")) {
          throw valueFormatNotAllowedException(
              columnName, snowflakeType, "Not a valid JSON: duplicate field", insertRowIndex);
        }
        throw new PkgSFException(
            e,
            ErrorCode.IO_ERROR,
            String.format(
                "Cannot create JSON Parser or JSON generator for column %s of type %s, rowIndex:%d",
                columnName, snowflakeType, insertRowIndex));
      }
      // We return the minified string from the result writer
      return resultWriter.toString();
    } else if (isAllowedSemiStructuredType(input)) {
      try {
        String result = objectWriter.writeValueAsString(new DuplicateKeyValidatedObject(input));
        verifyValidUtf8(result, columnName, snowflakeType, insertRowIndex);
        return result;
      } catch (JsonProcessingException e) {
        throw valueFormatNotAllowedException(
            columnName, snowflakeType, e.getMessage(), insertRowIndex);
      }
    }

    throw typeNotAllowedException(
        columnName,
        input.getClass(),
        snowflakeType,
        new String[] {
          "String",
          "Primitive data types and their arrays",
          "java.time.*",
          "List<T>",
          "Map<String, T>",
          "T[]"
        },
        insertRowIndex);
  }

  /**
   * Validates and parses input as JSON. All types in the object tree must be valid variant types,
   * see {@link PkgDataValidationUtil#isAllowedSemiStructuredType}.
   *
   * @param input Object to validate
   * @param insertRowIndex
   * @return JSON string representing the input
   */
  static String validateAndParseVariant(String columnName, Object input, long insertRowIndex) {
    JsonNode node =
        validateAndParseSemiStructuredAsJsonTree(columnName, input, "VARIANT", insertRowIndex);

    // Missing nodes are not valid json, ingest them as NULL instead
    if (node.isMissingNode()) {
      return null;
    }

    String output = node.toString();
    int stringLength = output.getBytes(StandardCharsets.UTF_8).length;
    if (stringLength > MAX_SEMI_STRUCTURED_LENGTH) {
      throw valueFormatNotAllowedException(
          columnName,
          "VARIANT",
          String.format(
              "Variant too long: length=%d maxLength=%d", stringLength, MAX_SEMI_STRUCTURED_LENGTH),
          insertRowIndex);
    }
    return output;
  }

  /**
   * Validates and parses input as JSON. All types in the object tree must be valid variant types,
   * see {@link PkgDataValidationUtil#isAllowedSemiStructuredType}.
   *
   * @param input Object to validate
   * @param insertRowIndex
   * @return JSON string representing the input
   */
  static String validateAndParseVariantNew(String columnName, Object input, long insertRowIndex) {
    final String result =
        validateAndParseSemiStructured(columnName, input, "VARIANT", insertRowIndex);

    // Empty json strings are ingested as nulls
    if (result.isEmpty()) {
      return null;
    }
    int stringLength = result.getBytes(StandardCharsets.UTF_8).length;
    if (stringLength > MAX_SEMI_STRUCTURED_LENGTH) {
      throw valueFormatNotAllowedException(
          columnName,
          "VARIANT",
          String.format(
              "Variant too long: length=%d maxLength=%d", stringLength, MAX_SEMI_STRUCTURED_LENGTH),
          insertRowIndex);
    }
    return result;
  }

  /**
   * Validates that passed object is allowed data type for semi-structured columns (i.e. VARIANT,
   * ARRAY, OBJECT). For non-trivial types like maps, arrays or lists, it recursively traverses the
   * object tree and validates that all types in the tree are also allowed. Allowed Java types:
   *
   * <ul>
   *   <li>primitive types (int, long, boolean, ...)
   *   <li>String
   *   <li>BigInteger
   *   <li>BigDecimal
   *   <li>LocalTime
   *   <li>OffsetTime
   *   <li>LocalDate
   *   <li>LocalDateTime
   *   <li>OffsetDateTime
   *   <li>ZonedDateTime
   *   <li>Map<String, T> where T is an allowed semi-structured type
   *   <li>List<T> where T is an allowed semi-structured type
   *   <li>primitive arrays (char[], int[], ...)
   *   <li>T[] where T is an allowed semi-structured type
   * </ul>
   *
   * @param o Object to validate
   * @return If the passed object is allowed for ingestion into semi-structured column
   */
  static boolean isAllowedSemiStructuredType(Object o) {
    // Allow null
    if (o == null) {
      return true;
    }

    // Allow string
    if (o instanceof String) {
      return true;
    }

    // Allow all primitive Java data types
    if (o instanceof Long
        || o instanceof Integer
        || o instanceof Short
        || o instanceof Byte
        || o instanceof Float
        || o instanceof Double
        || o instanceof Boolean
        || o instanceof Character) {
      return true;
    }

    // Allow BigInteger and BigDecimal
    if (o instanceof BigInteger || o instanceof BigDecimal) {
      return true;
    }

    // Allow supported types from java.time package
    if (o instanceof LocalTime
        || o instanceof OffsetTime
        || o instanceof LocalDate
        || o instanceof LocalDateTime
        || o instanceof ZonedDateTime
        || o instanceof OffsetDateTime) {
      return true;
    }

    // Map<String, T> is allowed, as long as T is also a supported semi-structured type
    if (o instanceof Map) {
      boolean allKeysAreStrings =
          ((Map<?, ?>) o).keySet().stream().allMatch(x -> x instanceof String);
      if (!allKeysAreStrings) {
        return false;
      }
      boolean allValuesAreAllowed =
          ((Map<?, ?>) o)
              .values().stream().allMatch(PkgDataValidationUtil::isAllowedSemiStructuredType);
      return allValuesAreAllowed;
    }

    // Allow arrays of primitive data types
    if (o instanceof byte[]
        || o instanceof short[]
        || o instanceof int[]
        || o instanceof long[]
        || o instanceof float[]
        || o instanceof double[]
        || o instanceof boolean[]
        || o instanceof char[]) {
      return true;
    }

    // Allow arrays of allowed semi-structured objects
    if (o.getClass().isArray()) {
      return Arrays.stream((Object[]) o)
          .allMatch(PkgDataValidationUtil::isAllowedSemiStructuredType);
    }

    // Allow lists consisting of allowed semi-structured objects
    if (o instanceof List) {
      return ((List<?>) o).stream().allMatch(PkgDataValidationUtil::isAllowedSemiStructuredType);
    }

    // If nothing matches, reject the input
    return false;
  }

  /**
   * Validates and parses JSON array. Non-array types are converted into single-element arrays. All
   * types in the array tree must be valid variant types, see {@link
   * PkgDataValidationUtil#isAllowedSemiStructuredType}.
   *
   * @param input Object to validate
   * @param insertRowIndex
   * @return JSON array representing the input
   */
  static String validateAndParseArray(String columnName, Object input, long insertRowIndex) {
    JsonNode jsonNode =
        validateAndParseSemiStructuredAsJsonTree(columnName, input, "ARRAY", insertRowIndex);

    // Non-array values are ingested as single-element arrays, mimicking the Worksheets behavior
    if (!jsonNode.isArray()) {
      jsonNode = objectMapper.createArrayNode().add(jsonNode);
    }

    String output = jsonNode.toString();
    // Throw an exception if the size is too large
    int stringLength = output.getBytes(StandardCharsets.UTF_8).length;
    if (stringLength > MAX_SEMI_STRUCTURED_LENGTH) {
      throw valueFormatNotAllowedException(
          columnName,
          "ARRAY",
          String.format(
              "Array too large. length=%d maxLength=%d", stringLength, MAX_SEMI_STRUCTURED_LENGTH),
          insertRowIndex);
    }
    return output;
  }

  /**
   * Validates and parses JSON array. Non-array types are converted into single-element arrays. All
   * types in the array tree must be valid variant types, see {@link
   * PkgDataValidationUtil#isAllowedSemiStructuredType}.
   *
   * @param input Object to validate
   * @param insertRowIndex
   * @return JSON array representing the input
   */
  static String validateAndParseArrayNew(String columnName, Object input, long insertRowIndex) {
    String result = validateAndParseSemiStructured(columnName, input, "ARRAY", insertRowIndex);
    if (result.isEmpty()) {
      // Empty input is ingested as an array of null
      result =
          JsonToken.START_ARRAY.asString()
              + JsonToken.VALUE_NULL.asString()
              + JsonToken.END_ARRAY.asString();
    } else if (!result.startsWith(JsonToken.START_ARRAY.asString())) {
      // Non-array values are ingested as single-element arrays, mimicking the Worksheets behavior
      result = JsonToken.START_ARRAY.asString() + result + JsonToken.END_ARRAY.asString();
    }

    // Throw an exception if the size is too large
    int stringLength = result.getBytes(StandardCharsets.UTF_8).length;
    if (stringLength > MAX_SEMI_STRUCTURED_LENGTH) {
      throw valueFormatNotAllowedException(
          columnName,
          "ARRAY",
          String.format(
              "Array too large. length=%d maxLength=%d", stringLength, MAX_SEMI_STRUCTURED_LENGTH),
          insertRowIndex);
    }
    return result;
  }

  /**
   * Validates and parses JSON object. Input is rejected if the value does not represent JSON object
   * (e.g. String '{}' or Map<String, T>). All types in the object tree must be valid variant types,
   * see {@link PkgDataValidationUtil#isAllowedSemiStructuredType}.
   *
   * @param input Object to validate
   * @param insertRowIndex
   * @return JSON object representing the input
   */
  static String validateAndParseObject(String columnName, Object input, long insertRowIndex) {
    JsonNode jsonNode =
        validateAndParseSemiStructuredAsJsonTree(columnName, input, "OBJECT", insertRowIndex);
    if (!jsonNode.isObject()) {
      throw valueFormatNotAllowedException(columnName, "OBJECT", "Not an object", insertRowIndex);
    }

    String output = jsonNode.toString();
    // Throw an exception if the size is too large
    int stringLength = output.getBytes(StandardCharsets.UTF_8).length;
    if (stringLength > MAX_SEMI_STRUCTURED_LENGTH) {
      throw valueFormatNotAllowedException(
          columnName,
          "OBJECT",
          String.format(
              "Object too large. length=%d maxLength=%d", stringLength, MAX_SEMI_STRUCTURED_LENGTH),
          insertRowIndex);
    }
    return output;
  }

  /**
   * Validates and parses JSON object. Input is rejected if the value does not represent JSON object
   * (e.g. String '{}' or Map<String, T>). All types in the object tree must be valid variant types,
   * see {@link PkgDataValidationUtil#isAllowedSemiStructuredType}.
   *
   * @param input Object to validate
   * @param insertRowIndex
   * @return JSON object representing the input
   */
  static String validateAndParseObjectNew(String columnName, Object input, long insertRowIndex) {
    final String result =
        validateAndParseSemiStructured(columnName, input, "OBJECT", insertRowIndex);
    if (!result.startsWith(JsonToken.START_OBJECT.asString())) {
      throw valueFormatNotAllowedException(columnName, "OBJECT", "Not an object", insertRowIndex);
    }
    // Throw an exception if the size is too large
    int stringLength = result.getBytes(StandardCharsets.UTF_8).length;
    if (stringLength > MAX_SEMI_STRUCTURED_LENGTH) {
      throw valueFormatNotAllowedException(
          columnName,
          "OBJECT",
          String.format(
              "Object too large. length=%d maxLength=%d", stringLength, MAX_SEMI_STRUCTURED_LENGTH),
          insertRowIndex);
    }
    return result;
  }

  /**
   * Converts user input to offset date time, which is the canonical representation of dates and
   * timestamps.
   */
  private static OffsetDateTime inputToOffsetDateTime(
      String columnName,
      String typeName,
      Object input,
      ZoneId defaultTimezone,
      final long insertRowIndex) {
    if (input instanceof OffsetDateTime) {
      return (OffsetDateTime) input;
    }

    if (input instanceof ZonedDateTime) {
      return ((ZonedDateTime) input).toOffsetDateTime();
    }

    if (input instanceof LocalDateTime) {
      return ((LocalDateTime) input).atZone(defaultTimezone).toOffsetDateTime();
    }

    if (input instanceof LocalDate) {
      return ((LocalDate) input).atStartOfDay().atZone(defaultTimezone).toOffsetDateTime();
    }

    if (input instanceof Instant) {
      // Just like integer-stored timestamps, instants are always interpreted in UTC
      return ((Instant) input).atZone(ZoneOffset.UTC).toOffsetDateTime();
    }

    if (input instanceof String) {
      String stringInput = ((String) input).trim();
      {
        // First, try to parse ZonedDateTime
        ZonedDateTime zoned = catchParsingError(() -> ZonedDateTime.parse(stringInput));
        if (zoned != null) {
          return zoned.toOffsetDateTime();
        }
      }

      {
        // Next, try to parse OffsetDateTime
        OffsetDateTime offset = catchParsingError(() -> OffsetDateTime.parse(stringInput));
        if (offset != null) {
          return offset;
        }
      }

      {
        // Alternatively, try to parse LocalDateTime
        LocalDateTime localDateTime = catchParsingError(() -> LocalDateTime.parse(stringInput));
        if (localDateTime != null) {
          return localDateTime.atZone(defaultTimezone).toOffsetDateTime();
        }
      }

      {
        // Alternatively, try to parse LocalDate
        LocalDate localDate = catchParsingError(() -> LocalDate.parse(stringInput));
        if (localDate != null) {
          return localDate.atStartOfDay().atZone(defaultTimezone).toOffsetDateTime();
        }
      }

      {
        // Alternatively, try to parse integer-stored timestamp
        // Just like in Snowflake, integer-stored timestamps are always in UTC
        Instant instant = catchParsingError(() -> parseInstantGuessScale(stringInput));
        if (instant != null) {
          return instant.atOffset(ZoneOffset.UTC);
        }
      }

      // Couldn't parse anything, throw an exception
      throw valueFormatNotAllowedException(
          columnName,
          typeName,
          "Not a valid value, see"
              + " https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview"
              + " for the list of supported formats",
          insertRowIndex);
    }

    // Type is not supported, throw an exception
    throw typeNotAllowedException(
        columnName,
        input.getClass(),
        typeName,
        new String[] {"String", "LocalDate", "LocalDateTime", "ZonedDateTime", "OffsetDateTime"},
        insertRowIndex);
  }

  private static <T> T catchParsingError(Supplier<T> op) {
    try {
      return op.get();
    } catch (DateTimeParseException | NumberFormatException e) {
      return null;
    }
  }

  /**
   * Validates and parses input for TIMESTAMP_NTZ, TIMESTAMP_LTZ and TIMEATAMP_TZ Snowflake types.
   * Allowed Java types:
   *
   * <ul>
   *   <li>String
   *   <li>LocalDate
   *   <li>LocalDateTime
   *   <li>OffsetDateTime
   *   <li>ZonedDateTime
   * </ul>
   *
   * @param columnName Column name, used in validation error messages
   * @param input String date in valid format, seconds past the epoch or java.time.* object. Accepts
   *     fractional seconds with precision up to the column's scale
   * @param scale decimal scale of timestamp 16 byte integer
   * @param defaultTimezone Input, which does not carry timezone information is going to be
   *     interpreted in the default timezone.
   * @param trimTimezone Whether timezone information should be removed from the resulting date,
   *     should be true for TIMESTAMP_NTZ columns.
   * @param insertRowIndex
   * @return TimestampWrapper
   */
  static TimestampWrapper validateAndParseTimestamp(
      String columnName,
      Object input,
      int scale,
      ZoneId defaultTimezone,
      boolean trimTimezone,
      long insertRowIndex) {
    OffsetDateTime offsetDateTime =
        inputToOffsetDateTime(columnName, "TIMESTAMP", input, defaultTimezone, insertRowIndex);

    if (trimTimezone) {
      offsetDateTime = offsetDateTime.withOffsetSameLocal(ZoneOffset.UTC);
    }
    if (offsetDateTime.getYear() < 1 || offsetDateTime.getYear() > 9999) {
      throw new PkgSFException(
          ErrorCode.INVALID_VALUE_ROW,
          String.format(
              "Timestamp out of representable inclusive range of years between 1 and 9999,"
                  + " rowIndex:%d, column:%s, value:%s",
              insertRowIndex, columnName, offsetDateTime));
    }
    return new TimestampWrapper(offsetDateTime, scale);
  }

  /**
   * Converts input to string, validates that length is less than max allowed string size
   * https://docs.snowflake.com/en/sql-reference/data-types-text.html#varchar. Allowed data types:
   *
   * <ul>
   *   <li>String
   *   <li>Number
   *   <li>boolean
   *   <li>char
   * </ul>
   *
   * @param input Object to validate and parse to String
   * @param maxLengthOptional Maximum allowed length of the output String, if empty then uses
   *     maximum allowed by Snowflake
   *     (https://docs.snowflake.com/en/sql-reference/data-types-text.html#varchar)
   * @param insertRowIndex
   */
  static String validateAndParseString(
      String columnName, Object input, Optional<Integer> maxLengthOptional, long insertRowIndex) {
    String output;
    if (input instanceof String) {
      output = (String) input;
      verifyValidUtf8(output, columnName, "STRING", insertRowIndex);
    } else if (input instanceof Number) {
      output = new BigDecimal(input.toString()).stripTrailingZeros().toPlainString();
    } else if (input instanceof Boolean || input instanceof Character) {
      output = input.toString();
    } else {
      throw typeNotAllowedException(
          columnName,
          input.getClass(),
          "STRING",
          new String[] {"String", "Number", "boolean", "char"},
          insertRowIndex);
    }
    byte[] utf8Bytes = output.getBytes(StandardCharsets.UTF_8);

    // Strings can never be larger than 16MB
    if (utf8Bytes.length > BYTES_16_MB) {
      throw valueFormatNotAllowedException(
          columnName,
          "STRING",
          String.format(
              "String too long: length=%d bytes maxLength=%d bytes", utf8Bytes.length, BYTES_16_MB),
          insertRowIndex);
    }

    // If max allowed length is specified (e.g. VARCHAR(10)), the number of unicode characters must
    // not exceed this value
    maxLengthOptional.ifPresent(
        maxAllowedCharacters -> {
          int actualCharacters = PkgBinaryStringUtils.unicodeCharactersCount(output);
          if (actualCharacters > maxAllowedCharacters) {
            throw valueFormatNotAllowedException(
                columnName,
                "STRING",
                String.format(
                    "String too long: length=%d characters maxLength=%d characters",
                    actualCharacters, maxAllowedCharacters),
                insertRowIndex);
          }
        });
    return output;
  }

  /**
   * Returns a BigDecimal representation of the input. Strings of the form "1.23E4" will be treated
   * as being written in * scientific notation (e.g. 1.23 * 10^4). Does not perform any size
   * validation. Allowed Java types:
   * <li>byte, short, int, long
   * <li>float, double
   * <li>BigInteger, BigDecimal
   * <li>String
   */
  static BigDecimal validateAndParseBigDecimal(
      String columnName, Object input, long insertRowIndex) {
    if (input instanceof BigDecimal) {
      return (BigDecimal) input;
    } else if (input instanceof BigInteger) {
      return new BigDecimal((BigInteger) input);
    } else if (input instanceof Byte
        || input instanceof Short
        || input instanceof Integer
        || input instanceof Long) {
      return BigDecimal.valueOf(((Number) input).longValue());
    } else if (input instanceof Float || input instanceof Double) {
      try {
        return BigDecimal.valueOf(((Number) input).doubleValue());
      } catch (NumberFormatException e) {
        /* NaN and infinity are not allowed */
        throw valueFormatNotAllowedException(
            columnName, "NUMBER", "Not a valid number", insertRowIndex);
      }
    } else if (input instanceof String) {
      try {
        final String stringInput = ((String) input).trim();
        return new BigDecimal(stringInput);
      } catch (NumberFormatException e) {
        throw valueFormatNotAllowedException(
            columnName, "NUMBER", "Not a valid number", insertRowIndex);
      }
    } else {
      throw typeNotAllowedException(
          columnName,
          input.getClass(),
          "NUMBER",
          new String[] {
            "int", "long", "byte", "short", "float", "double", "BigDecimal", "BigInteger", "String"
          },
          insertRowIndex);
    }
  }

  /**
   * Returns the number of days between the epoch and the passed date. Allowed Java types:
   *
   * <ul>
   *   <li>String
   *   <li>{@link LocalDate}
   *   <li>{@link LocalDateTime}
   *   <li>{@link OffsetDateTime}
   *   <li>{@link ZonedDateTime}
   *   <li>{@link Instant}
   * </ul>
   */
  static int validateAndParseDate(String columnName, Object input, long insertRowIndex) {
    OffsetDateTime offsetDateTime =
        inputToOffsetDateTime(columnName, "DATE", input, ZoneOffset.UTC, insertRowIndex);

    if (offsetDateTime.getYear() < -9999 || offsetDateTime.getYear() > 9999) {
      throw new PkgSFException(
          ErrorCode.INVALID_VALUE_ROW,
          String.format(
              "Date out of representable inclusive range of years between -9999 and 9999,"
                  + " rowIndex:%d, column:%s, value:%s",
              insertRowIndex, columnName, offsetDateTime));
    }

    return Math.toIntExact(offsetDateTime.toLocalDate().toEpochDay());
  }

  /**
   * Validates input for data type BINARY. Allowed Java types:
   *
   * <ul>
   *   <li>byte[]
   *   <li>String (hex-encoded)
   * </ul>
   *
   * @param input Array to validate
   * @param maxLengthOptional Max array length, defaults to 8MB, which is the max allowed length for
   *     BINARY column
   * @param insertRowIndex
   * @return Validated array
   */
  static byte[] validateAndParseBinary(
      String columnName, Object input, Optional<Integer> maxLengthOptional, long insertRowIndex) {
    byte[] output;
    if (input instanceof byte[]) {
      // byte[] is a mutable object, we need to create a defensive copy to protect against
      // concurrent modifications of the array, which could lead to mismatch between data
      // and metadata
      byte[] originalInputArray = (byte[]) input;
      output = new byte[originalInputArray.length];
      System.arraycopy(originalInputArray, 0, output, 0, originalInputArray.length);
    } else if (input instanceof String) {
      try {
        String stringInput = ((String) input).trim();
        output = Hex.decodeHex(stringInput);
      } catch (DecoderException e) {
        throw valueFormatNotAllowedException(
            columnName, "BINARY", "Not a valid hex string", insertRowIndex);
      }
    } else {
      throw typeNotAllowedException(
          columnName,
          input.getClass(),
          "BINARY",
          new String[] {"byte[]", "String"},
          insertRowIndex);
    }

    int maxLength = maxLengthOptional.orElse(BYTES_8_MB);
    if (output.length > maxLength) {
      throw valueFormatNotAllowedException(
          columnName,
          "BINARY",
          String.format("Binary too long: length=%d maxLength=%d", output.length, maxLength),
          insertRowIndex);
    }
    return output;
  }

  /**
   * Returns the number of units since 00:00, depending on the scale (scale=0: seconds, scale=3:
   * milliseconds, scale=9: nanoseconds). Allowed Java types:
   *
   * <ul>
   *   <li>String
   *   <li>{@link LocalTime}
   *   <li>{@link OffsetTime}
   * </ul>
   */
  static BigInteger validateAndParseTime(
      String columnName, Object input, int scale, long insertRowIndex) {
    if (input instanceof LocalTime) {
      LocalTime localTime = (LocalTime) input;
      return BigInteger.valueOf(localTime.toNanoOfDay()).divide(Power10.sb16Table[9 - scale]);
    } else if (input instanceof OffsetTime) {
      return validateAndParseTime(
          columnName, ((OffsetTime) input).toLocalTime(), scale, insertRowIndex);
    } else if (input instanceof String) {
      String stringInput = ((String) input).trim();
      {
        // First, try to parse LocalTime
        LocalTime localTime = catchParsingError(() -> LocalTime.parse(stringInput));
        if (localTime != null) {
          return validateAndParseTime(columnName, localTime, scale, insertRowIndex);
        }
      }

      {
        // Alternatively, try to parse OffsetTime
        OffsetTime offsetTime = catchParsingError((() -> OffsetTime.parse(stringInput)));
        if (offsetTime != null) {
          return validateAndParseTime(columnName, offsetTime.toLocalTime(), scale, insertRowIndex);
        }
      }

      {
        // Alternatively, try to parse integer-stored time
        Instant parsedInstant = catchParsingError(() -> parseInstantGuessScale(stringInput));
        if (parsedInstant != null) {
          return validateAndParseTime(
              columnName,
              LocalDateTime.ofInstant(parsedInstant, ZoneOffset.UTC).toLocalTime(),
              scale,
              insertRowIndex);
        }
      }

      throw valueFormatNotAllowedException(
          columnName,
          "TIME",
          "Not a valid time, see"
              + " https://docs.snowflake.com/en/user-guide/data-load-snowpipe-streaming-overview"
              + " for the list of supported formats",
          insertRowIndex);

    } else {
      throw typeNotAllowedException(
          columnName,
          input.getClass(),
          "TIME",
          new String[] {"String", "LocalTime", "OffsetTime"},
          insertRowIndex);
    }
  }

  /**
   * Attempts to parse integer-stored date from string input. Tries to guess the scale according to
   * the rules documented at
   * https://docs.snowflake.com/en/user-guide/date-time-input-output.html#auto-detection-of-integer-stored-date-time-and-timestamp-values.
   *
   * @param input String to parse, must represent a valid long
   * @return Instant representing the input
   * @throws NumberFormatException If the input in not a valid long
   */
  private static Instant parseInstantGuessScale(String input) {
    BigInteger epochNanos;
    try {
      long val = Long.parseLong(input);

      if (val > -SECONDS_LIMIT_FOR_EPOCH && val < SECONDS_LIMIT_FOR_EPOCH) {
        epochNanos = BigInteger.valueOf(val).multiply(Power10.sb16Table[9]);
      } else if (val > -MILLISECONDS_LIMIT_FOR_EPOCH && val < MILLISECONDS_LIMIT_FOR_EPOCH) {
        epochNanos = BigInteger.valueOf(val).multiply(Power10.sb16Table[6]);
      } else if (val > -MICROSECONDS_LIMIT_FOR_EPOCH && val < MICROSECONDS_LIMIT_FOR_EPOCH) {
        epochNanos = BigInteger.valueOf(val).multiply(Power10.sb16Table[3]);
      } else {
        epochNanos = BigInteger.valueOf(val);
      }
    } catch (NumberFormatException e) {
      // The input is bigger than max long value, treat it as nano-seconds directly
      epochNanos = new BigInteger(input);
    }

    return Instant.ofEpochSecond(
        epochNanos.divide(Power10.sb16Table[9]).longValue(),
        epochNanos.remainder(Power10.sb16Table[9]).longValue());
  }

  /**
   * Converts input to double value. Allowed Java types:
   *
   * <ul>
   *   <li>Number
   *   <li>String
   * </ul>
   *
   * @param input
   * @param insertRowIndex
   */
  static double validateAndParseReal(String columnName, Object input, long insertRowIndex) {
    if (input instanceof Float) {
      return Double.parseDouble(input.toString());
    } else if (input instanceof Number) {
      return ((Number) input).doubleValue();
    } else if (input instanceof String) {
      String stringInput = ((String) input).trim();
      try {
        return Double.parseDouble(stringInput);
      } catch (NumberFormatException err) {
        stringInput = stringInput.toLowerCase();
        switch (stringInput) {
          case "nan":
            return Double.NaN;
          case "inf":
            return Double.POSITIVE_INFINITY;
          case "-inf":
            return Double.NEGATIVE_INFINITY;
          default:
            throw valueFormatNotAllowedException(
                columnName, "REAL", "Not a valid decimal number", insertRowIndex);
        }
      }
    }
    throw typeNotAllowedException(
        columnName, input.getClass(), "REAL", new String[] {"Number", "String"}, insertRowIndex);
  }

  /**
   * Validates and parses input Iceberg INT column. Allowed Java types:
   *
   * <ul>
   *   <li>Number
   *   <li>String
   * </ul>
   *
   * @param columnName Column name, used in validation error messages
   * @param input Object to validate and parse
   * @param insertRowIndex Row index for error reporting
   * @return Parsed integer
   */
  static int validateAndParseIcebergInt(String columnName, Object input, long insertRowIndex) {
    BigDecimal roundedValue =
        validateAndParseBigDecimal(columnName, input, insertRowIndex)
            .setScale(0, RoundingMode.HALF_UP);
    try {
      return roundedValue.intValueExact();
    } catch (ArithmeticException e) {
      /* overflow */
      throw new PkgSFException(
          ErrorCode.INVALID_VALUE_ROW,
          String.format(
              "Number out of representable inclusive range of integers between %d and %d,"
                  + " rowIndex:%d, column:%s, value:%s",
              Integer.MIN_VALUE, Integer.MAX_VALUE, insertRowIndex, columnName, input));
    }
  }

  /**
   * Validates and parses input Iceberg LONG column. Allowed Java types:
   *
   * <ul>
   *   <li>Number
   *   <li>String
   * </ul>
   *
   * @param columnName Column name, used in validation error messages
   * @param input Object to validate and parse
   * @param insertRowIndex Row index for error reporting
   * @return Parsed long
   */
  static long validateAndParseIcebergLong(String columnName, Object input, long insertRowIndex) {
    BigDecimal roundedValue =
        validateAndParseBigDecimal(columnName, input, insertRowIndex)
            .setScale(0, RoundingMode.HALF_UP);
    try {
      return roundedValue.longValueExact();
    } catch (ArithmeticException e) {
      /* overflow */
      throw new PkgSFException(
          ErrorCode.INVALID_VALUE_ROW,
          String.format(
              "Number out of representable inclusive range of integers between %d and %d,"
                  + " rowIndex:%d, column:%s, value:%s",
              Long.MIN_VALUE, Long.MAX_VALUE, insertRowIndex, columnName, input));
    }
  }

  /**
   * Validate and parse input to integer output, 1=true, 0=false. String values converted to boolean
   * according to https://docs.snowflake.com/en/sql-reference/functions/to_boolean.html#usage-notes
   * Allowed Java types:
   *
   * <ul>
   *   <li>boolean
   *   <li>String
   *   <li>java.lang.Number
   * </ul>
   *
   * @param input Input to be converted
   * @return 1 or 0 where 1=true, 0=false
   */
  static int validateAndParseBoolean(String columnName, Object input, long insertRowIndex) {
    if (input instanceof Boolean) {
      return (boolean) input ? 1 : 0;
    } else if (input instanceof Number) {
      return new BigDecimal(input.toString()).compareTo(BigDecimal.ZERO) == 0 ? 0 : 1;
    } else if (input instanceof String) {
      return convertStringToBoolean(columnName, (String) input, insertRowIndex) ? 1 : 0;
    }

    throw typeNotAllowedException(
        columnName,
        input.getClass(),
        "BOOLEAN",
        new String[] {"boolean", "Number", "String"},
        insertRowIndex);
  }

  /**
   * Validate and cast Iceberg struct column to Map<String, Object>. Allowed Java type:
   *
   * <ul>
   *   <li>Map<String, Object>
   * </ul>
   *
   * @param columnName Column name, used in validation error messages
   * @param input Object to validate and parse
   * @param insertRowIndex Row index for error reporting
   * @return Object cast to Map
   */
  static Map<String, ?> validateAndParseIcebergStruct(
      String columnName, Object input, long insertRowIndex) {
    if (!(input instanceof Map)) {
      throw typeNotAllowedException(
          columnName,
          input.getClass(),
          "STRUCT",
          new String[] {"Map<String, Object>"},
          insertRowIndex);
    }
    for (Object key : ((Map<?, ?>) input).keySet()) {
      if (!(key instanceof String)) {
        throw new PkgSFException(
            ErrorCode.INVALID_FORMAT_ROW,
            String.format(
                "Field name of struct typed column must be of type String, but found %s."
                    + " rowIndex:%d, column:%s",
                key.getClass().getName(), insertRowIndex, columnName));
      }
    }

    return (Map<String, ?>) input;
  }

  /**
   * Validate and parse Iceberg list column to an Iterable. Allowed Java type:
   *
   * <ul>
   *   <li>Iterable
   * </ul>
   *
   * @param columnName Column name, used in validation error messages
   * @param input Object to validate and parse
   * @param insertRowIndex Row index for error reporting
   * @return Object cast to Iterable
   */
  static Iterable<?> validateAndParseIcebergList(
      String columnName, Object input, long insertRowIndex) {
    if (!(input instanceof Iterable)) {
      throw typeNotAllowedException(
          columnName, input.getClass(), "LIST", new String[] {"Iterable"}, insertRowIndex);
    }
    return (Iterable<?>) input;
  }

  /**
   * Validate and parse Iceberg map column to a map. Allowed Java type:
   *
   * <ul>
   *   <li>Map<Object, Object>
   * </ul>
   *
   * @param columnName Column name, used in validation error messages
   * @param input Object to validate and parse
   * @param insertRowIndex Row index for error reporting
   * @return Object cast to Map
   */
  static Map<?, ?> validateAndParseIcebergMap(
      String columnName, Object input, long insertRowIndex) {
    if (!(input instanceof Map)) {
      throw typeNotAllowedException(
          columnName, input.getClass(), "MAP", new String[] {"Map"}, insertRowIndex);
    }
    return (Map<?, ?>) input;
  }

  static void checkValueInRange(
      String columnName,
      BigDecimal bigDecimalValue,
      int scale,
      int precision,
      final long insertRowIndex) {
    BigDecimal comparand =
        (precision >= scale) && (precision - scale) < POWER_10.length
            ? POWER_10[precision - scale]
            : BigDecimal.TEN.pow(precision - scale);
    if (bigDecimalValue.abs().compareTo(comparand) >= 0) {
      throw new PkgSFException(
          ErrorCode.INVALID_FORMAT_ROW,
          String.format(
              "Number out of representable exclusive range of (-1e%s..1e%s), rowIndex:%d,"
                  + " column:%s, value:%s",
              precision - scale, precision - scale, insertRowIndex, columnName, bigDecimalValue));
    }
  }

  static void checkFixedLengthByteArray(
      String columnName, byte[] bytes, int length, final long insertRowIndex) {
    if (bytes.length != length) {
      throw new PkgSFException(
          ErrorCode.INVALID_VALUE_ROW,
          String.format(
              "Binary length mismatch: expected:%d, actual:%d, rowIndex:%d, column:%s",
              length, bytes.length, insertRowIndex, columnName));
    }
  }

  static Set<String> allowedBooleanStringsLowerCased =
      new HashSet<>(
          Arrays.asList("1", "0", "yes", "no", "y", "n", "t", "f", "true", "false", "on", "off"));

  private static boolean convertStringToBoolean(
      String columnName, String value, final long insertRowIndex) {
    String normalizedInput = value.toLowerCase().trim();
    if (!allowedBooleanStringsLowerCased.contains(normalizedInput)) {
      throw valueFormatNotAllowedException(
          columnName,
          "BOOLEAN",
          "Not a valid boolean, see"
              + " https://docs.snowflake.com/en/sql-reference/data-types-logical.html#conversion-to-boolean"
              + " for the list of supported formats",
          insertRowIndex);
    }
    return "1".equals(normalizedInput)
        || "yes".equals(normalizedInput)
        || "y".equals(normalizedInput)
        || "t".equals(normalizedInput)
        || "true".equals(normalizedInput)
        || "on".equals(normalizedInput);
  }

  /**
   * Create exception that a Java type cannot be ingested into a specific Snowflake column type
   *
   * @param javaType Java type failing the validation
   * @param snowflakeType Target Snowflake column type
   * @param allowedJavaTypes Java types supported for the Java type
   */
  private static PkgSFException typeNotAllowedException(
      String columnName,
      Class<?> javaType,
      String snowflakeType,
      String[] allowedJavaTypes,
      final long insertRowIndex) {
    return new PkgSFException(
        ErrorCode.INVALID_FORMAT_ROW,
        String.format(
            "Object of type %s cannot be ingested into Snowflake column %s of type %s, rowIndex:%d",
            javaType.getName(), columnName, snowflakeType, insertRowIndex),
        String.format(
            String.format("Allowed Java types: %s", String.join(", ", allowedJavaTypes))));
  }

  /**
   * Create exception when the Java type is correct, but the value is invalid (e.g. boolean cannot
   * be parsed from a string)
   *
   * <p>Note: Do not log actual Object Value
   *
   * @param columnName Column Name
   * @param snowflakeType Snowflake column type
   * @param reason Reason why value format is not allowed.
   * @param rowIndex Index of the Input row primarily for debugging purposes.
   * @return SFException is thrown
   */
  private static PkgSFException valueFormatNotAllowedException(
      String columnName, String snowflakeType, String reason, final long rowIndex) {
    return new PkgSFException(
        ErrorCode.INVALID_VALUE_ROW,
        String.format(
            "Value cannot be ingested into Snowflake column %s of type %s, rowIndex:%d, reason: %s",
            columnName, snowflakeType, rowIndex, reason));
  }

  /**
   * Validates that a string is valid UTF-8 string. It catches situations like unmatched high/low
   * UTF-16 surrogate, for example.
   */
  private static void verifyValidUtf8(
      String input, String columnName, String dataType, final long insertRowIndex) {
    String roundTripStr =
        new String(input.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
    if (!input.equals(roundTripStr)) {
      throw valueFormatNotAllowedException(
          columnName, dataType, "Invalid Unicode string", insertRowIndex);
    }
  }
}
