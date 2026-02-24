# Table Name Sanitization Toggle Design

**Date:** 2026-02-23
**Issue:** SNOW-3029864
**Author:** Behzad Mikaili + Claude Opus 4.6

## Problem Statement

The Snowflake Kafka Connector v3/v4 has limitations preventing users from using case-sensitive or special-character table names:

1. **Config validation rejects quoted identifiers:** `topic2table.map` with `myTopic:"My-Table"` fails validation
2. **No case preservation:** Auto-generated table names rely on Snowflake's implicit uppercasing
3. **Forced sanitization:** Users cannot opt out of character replacement for auto-generated names

This prevents integration with tables that use mixed case or special characters (e.g., `"My-Table"`, `"data-2024-01"`).

## Current Behavior (Master)

### Validation Phase (Config Parsing)
- `parseTopicToTableMap()` validates table names using `isValidSnowflakeTableName()`
- Regex: `^[_a-zA-Z][_$a-zA-Z0-9]+$`
- **Rejects:** Quoted identifiers, dashes, spaces, numbers at start
- **Accepts:** Only unquoted valid identifiers (case preserved but not enforced)

### Name Generation Phase (Runtime)
- `generateValidNameFromMap(topic, map)` determines table name
- **Map entries:** Returned as-is (no sanitization)
- **Auto-generated names:**
  - Valid identifiers: pass through unchanged
  - Invalid identifiers: sanitize (replace special chars with `_`, append hash)
- **No explicit uppercasing** - relies on Snowflake SQL frontend

### Example Flows

| Input | Map Entry? | Current Behavior | Actual Table Name |
|-------|-----------|------------------|-------------------|
| `myTopic:MyTable` | Yes | Pass validation, return `MyTable` | `MYTABLE` (SF uppercases) |
| `myTopic:"My-Table"` | Yes | **FAIL validation** ERROR_0021 | N/A |
| `my-topic` (auto) | No | Sanitize → `my_topic_123456` | `MY_TOPIC_123456` (SF uppercases) |
| `MyTopic` (auto) | No | Valid, pass through → `MyTopic` | `MYTOPIC` (SF uppercases) |

## Proposed Solution

### Architecture

Add `snowflake.enable.table.name.sanitization` config flag (default `true`) to control auto-generated name processing with explicit uppercasing.

**Key changes:**
1. Relax `isValidSnowflakeTableName` regex to accept quoted identifier pattern `"[^"]+"`
2. Add sanitization toggle that affects only auto-generated names
3. Add explicit uppercasing to sanitization path (don't rely on Snowflake)
4. Ensure all DDL uses `identifier(?)` JDBC parameterization

### Behavior Matrix

| Scenario | Sanitization=True (v3 compatible) | Sanitization=False (new) |
|----------|----------------------------------|--------------------------|
| Map: `myTopic:"My-Table"` | Pass validation, return `"My-Table"` | Pass validation, return `"My-Table"` |
| Map: `myTopic:MyTable` | Pass validation, return `MyTable` → uppercase to `MYTABLE` | Pass validation, return `MyTable` (Snowflake uppercases) |
| Auto: `MyTopic` | Valid, uppercase → `MYTOPIC` | Valid, pass through → `MyTopic` (Snowflake uppercases) |
| Auto: `my-topic` | Sanitize + uppercase → `MY_TOPIC_123456` | Pass through → `my-topic` (Snowflake fails with SQL error) |

**Important notes:**
- Map entries that pass validation are always returned as-is (existing behavior)
- Sanitization flag only controls auto-generated name processing
- When sanitization=false, invalid unquoted identifiers fail at Snowflake (user responsibility)
- Quoted identifiers in map entries bypass Snowflake uppercasing

### Data Flow

```
Config Parsing (One-time):
  parseTopicToTableMap(config)
    → for each mapping:
      → isValidSnowflakeTableName(tableName)
        → Accept if: ^"[^"]+"$ OR ^[_a-zA-Z][_$a-zA-Z0-9]+$
        → Reject with ERROR_0021 if invalid

Runtime (Per-record):
  generateValidNameFromMap(topic, map, sanitizationEnabled)
    → if (map.contains(topic)):
        return GeneratedName.fromMap(map.get(topic))  // Bypass
    → if (map.matchesRegex(topic)):
        return GeneratedName.fromMap(matchedValue)  // Bypass
    → // Auto-generated name path:
    → if (isValidSnowflakeObjectIdentifier(topic)):
        if (sanitizationEnabled):
          return GeneratedName.generated(topic.toUpperCase(Locale.ROOT))
        else:
          return GeneratedName.generated(topic)
    → // Invalid identifier path:
    → if (sanitizationEnabled):
        sanitized = replaceSpecialChars(topic) + "_" + hash
        return GeneratedName.generated(sanitized.toUpperCase(Locale.ROOT))
    → else:
        return GeneratedName.generated(topic)  // Let Snowflake fail
```

### Implementation Details

**File: `Utils.java`**
- Update `isValidSnowflakeTableName()` regex:
  ```java
  return tableName.matches(
    "^(\"[^\"]+\"|([_a-zA-Z]{1}[_$a-zA-Z0-9]+\\.){0,2}[_a-zA-Z]{1}[_$a-zA-Z0-9]+)$");
  ```
- Add `boolean enableSanitization` parameter to:
  - `getTableName(topic, map, enableSanitization)`
  - `generateTableName(topic, map, enableSanitization)`
  - `generateValidName(topic, map, enableSanitization)`
  - `generateValidNameFromMap(topic, map, enableSanitization)`
- Modify sanitization logic:
  - Valid identifiers: uppercase if sanitization enabled
  - Invalid identifiers: sanitize + uppercase if enabled, pass through if disabled
- Map entries always bypass sanitization (existing behavior)

**File: `Constants.java`**
```java
public static final String SNOWFLAKE_ENABLE_TABLE_NAME_SANITIZATION =
    "snowflake.enable.table.name.sanitization";
public static final boolean SNOWFLAKE_ENABLE_TABLE_NAME_SANITIZATION_DEFAULT = true;
```

**File: `ConnectorConfigDefinition.java`**
- Add config definition with BOOLEAN type, default true, LOW importance
- Description: "When enabled, auto-generated table names are sanitized and uppercased for v3 compatibility. When disabled, topic names are passed through as-is (use topic2table.map with quoted identifiers for special characters)."

**File: `SnowflakeSinkServiceV2.java`**
- Read `enableSanitization` flag from config in constructor
- Pass flag to all `Utils.getTableName()` calls

**DDL Parameterization:**
- Audit all CREATE/ALTER/DROP statements
- Ensure `identifier(?)` is used for table names everywhere
- JDBC handles quoting/escaping automatically

## Backward Compatibility

**Default behavior (sanitization=true):** Fully compatible with v3.2.x for existing configs
- Auto-generated names are sanitized + uppercased (new uppercasing is explicit but produces same result)
- Map entries with valid identifiers work unchanged (but now uppercased explicitly)

**Breaking change:** Map entries with unquoted mixed-case identifiers
- v3.2.x: `myTopic:MyTable` → `MyTable` → Snowflake uppercases to `MYTABLE`
- New: `myTopic:MyTable` → uppercase to `MYTABLE` explicitly
- Result is the same (`MYTABLE` table) but uppercasing happens earlier

**New capability (sanitization=false):**
- Users can now use quoted identifiers: `myTopic:"My-Table"` ✓
- Users can pass through invalid identifiers (at their own risk)

## Testing Strategy

### Unit Tests (`UtilsTest.java`)
1. `testValidSnowflakeTableNameAcceptsQuotedIdentifiers()`
   - Verify regex accepts `"My-Table"`, `"data-2024"`, `"123table"`
   - Verify regex still accepts valid unquoted identifiers
   - Verify regex rejects malformed quoted identifiers

2. `testSanitizationEnabledUppercasesValid()`
   - `getTableName("MyTopic", emptyMap, true)` → `"MYTOPIC"`
   - `getTableName("myTopic", emptyMap, true)` → `"MYTOPIC"`

3. `testSanitizationEnabledSanitizesAndUppercases()`
   - `getTableName("my-topic", emptyMap, true)` → `"MY_TOPIC_<hash>"`

4. `testSanitizationDisabledPassesThrough()`
   - `getTableName("MyTopic", emptyMap, false)` → `"MyTopic"`
   - `getTableName("my-topic", emptyMap, false)` → `"my-topic"`

5. `testMapEntriesBypassSanitization()`
   - Map: `myTopic:"My-Table"`
   - `getTableName("myTopic", map, true)` → `"My-Table"`
   - `getTableName("myTopic", map, false)` → `"My-Table"`

### Integration Tests (`SnowflakeSinkTaskForStreamingIT.java`)
1. `testSanitizationEnabledAutoGenerated()`
   - Topic: `my-topic-123`
   - Config: `sanitization=true`
   - Verify table `MY_TOPIC_123_<hash>` exists
   - Verify data ingested correctly

2. `testSanitizationDisabledQuotedMap()`
   - Topic: `myTopic`
   - Map: `myTopic:"My-Test-Table"`
   - Config: `sanitization=false`
   - Verify table `"My-Test-Table"` exists (case preserved)
   - Verify data ingested correctly

3. `testSanitizationDisabledInvalidTopicFails()`
   - Topic: `123-invalid`
   - Config: `sanitization=false`, no map
   - Verify connector errors with Snowflake SQL error (invalid identifier)

## Success Criteria

1. ✅ Users can specify quoted identifiers in `topic2table.map`
2. ✅ Sanitization toggle controls auto-generated name behavior
3. ✅ Default behavior is v3 compatible (sanitization=true)
4. ✅ All DDL uses `identifier(?)` parameterization
5. ✅ Unit tests cover all validation + sanitization paths
6. ✅ Integration tests verify end-to-end table creation

## Future Considerations

- **Embedded quotes:** Current design assumes Kafka topic names don't contain `"` characters (valid assumption based on Kafka naming rules)
- **Schema/database qualification:** Current regex accepts `db.schema.table` patterns - may need refinement for quoted qualified names
- **Metrics:** Consider adding telemetry for sanitization bypass usage
