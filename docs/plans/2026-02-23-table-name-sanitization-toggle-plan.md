# Table Name Sanitization Toggle Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add config toggle to control table name sanitization and enable quoted identifiers in topic2table.map

**Architecture:** Relax validation regex to accept quoted identifiers, add sanitization flag that controls auto-generated name uppercasing/sanitization, ensure DDL uses identifier(?) parameterization

**Tech Stack:** Java 17, Maven, JUnit 5, Snowflake JDBC

---

## Task 1: Update validation regex to accept quoted identifiers

**Files:**
- Modify: `src/main/java/com/snowflake/kafka/connector/Utils.java:312-314`
- Test: `src/test/java/com/snowflake/kafka/connector/UtilsTest.java`

**Step 1: Write failing test for quoted identifier validation**

Add to `UtilsTest.java`:

```java
@Test
public void testValidSnowflakeTableNameAcceptsQuotedIdentifiers() {
  // Quoted identifiers should be accepted
  assertTrue(Utils.isValidSnowflakeTableName("\"My-Table\""));
  assertTrue(Utils.isValidSnowflakeTableName("\"data-2024-01\""));
  assertTrue(Utils.isValidSnowflakeTableName("\"123table\""));
  assertTrue(Utils.isValidSnowflakeTableName("\"has spaces\""));

  // Unquoted valid identifiers still accepted
  assertTrue(Utils.isValidSnowflakeTableName("MyTable"));
  assertTrue(Utils.isValidSnowflakeTableName("_underscore"));

  // Invalid patterns still rejected
  assertFalse(Utils.isValidSnowflakeTableName("\"unclosed));
  assertFalse(Utils.isValidSnowflakeTableName("has-dashes"));
  assertFalse(Utils.isValidSnowflakeTableName("123starts"));
}
```

**Step 2: Run test to verify it fails**

```bash
mvn test -pl . -Dtest=UtilsTest#testValidSnowflakeTableNameAcceptsQuotedIdentifiers -Dcheckstyle.skip -DargLine="--add-opens java.base/java.util=ALL-UNNAMED"
```

Expected: Test fails because quoted identifiers are rejected

**Step 3: Update regex to accept quoted identifiers**

In `Utils.java`, modify `isValidSnowflakeTableName`:

```java
static boolean isValidSnowflakeTableName(String tableName) {
  return tableName.matches(
      "^(\"[^\"]+\"|([_a-zA-Z]{1}[_$a-zA-Z0-9]+\\.){0,2}[_a-zA-Z]{1}[_$a-zA-Z0-9]+)$");
}
```

**Step 4: Run test to verify it passes**

```bash
mvn test -pl . -Dtest=UtilsTest#testValidSnowflakeTableNameAcceptsQuotedIdentifiers -Dcheckstyle.skip -DargLine="--add-opens java.base/java.util=ALL-UNNAMED"
```

Expected: Test passes

**Step 5: Commit**

```bash
git add src/main/java/com/snowflake/kafka/connector/Utils.java src/test/java/com/snowflake/kafka/connector/UtilsTest.java
git commit -m "SNOW-3029864: Accept quoted identifiers in table name validation

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 2: Add config constant for sanitization toggle

**Files:**
- Modify: `src/main/java/com/snowflake/kafka/connector/Constants.java:68-70` (after ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS)

**Step 1: Add constant**

In `Constants.java`, add after line 68:

```java
public static final String SNOWFLAKE_ENABLE_TABLE_NAME_SANITIZATION =
    "snowflake.enable.table.name.sanitization";
public static final boolean SNOWFLAKE_ENABLE_TABLE_NAME_SANITIZATION_DEFAULT = true;
```

**Step 2: Commit**

```bash
git add src/main/java/com/snowflake/kafka/connector/Constants.java
git commit -m "SNOW-3029864: Add table name sanitization config constant

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 3: Register config in ConnectorConfigDefinition

**Files:**
- Modify: `src/main/java/com/snowflake/kafka/connector/config/ConnectorConfigDefinition.java`

**Step 1: Find insertion point**

Search for `ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS` in config definitions (around line 310-320).

**Step 2: Add config definition**

Add after the `ENABLE_TASK_FAIL_ON_AUTHORIZATION_ERRORS` definition:

```java
.define(
    KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_TABLE_NAME_SANITIZATION,
    BOOLEAN,
    KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_TABLE_NAME_SANITIZATION_DEFAULT,
    LOW,
    "When enabled, auto-generated table names are sanitized (special characters replaced)"
        + " and uppercased for v3 compatibility. When disabled, topic names are passed through"
        + " as-is. Use topic2table.map with quoted identifiers for special characters when disabled.",
    CONNECTOR_CONFIG_DOC,
    9,
    Width.NONE,
    KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_TABLE_NAME_SANITIZATION)
```

**Step 3: Update display order numbers**

Increment the display order for any configs that come after (likely CACHE_TABLE_EXISTS from 9 to 10, etc).

**Step 4: Commit**

```bash
git add src/main/java/com/snowflake/kafka/connector/config/ConnectorConfigDefinition.java
git commit -m "SNOW-3029864: Register table name sanitization config

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 4: Add sanitization parameter to Utils methods

**Files:**
- Modify: `src/main/java/com/snowflake/kafka/connector/Utils.java`
- Test: `src/test/java/com/snowflake/kafka/connector/UtilsTest.java`

**Step 1: Write failing test for sanitization toggle**

Add to `UtilsTest.java`:

```java
@Test
public void testSanitizationToggle() {
  Map<String, String> emptyMap = new HashMap<>();

  // Sanitization enabled (v3 compatible)
  String uppercased = Utils.getTableName("MyTopic", emptyMap, true);
  assertEquals("MYTOPIC", uppercased, "Valid identifier should be uppercased");

  String sanitized = Utils.getTableName("my-topic", emptyMap, true);
  assertTrue(sanitized.startsWith("MY_TOPIC_"), "Invalid identifier should be sanitized+uppercased");
  assertTrue(sanitized.matches("^[A-Z_0-9]+$"), "Should be fully uppercased");

  // Sanitization disabled (pass through)
  String passedThrough = Utils.getTableName("MyTopic", emptyMap, false);
  assertEquals("MyTopic", passedThrough, "Should pass through unchanged");

  String invalid = Utils.getTableName("my-topic", emptyMap, false);
  assertEquals("my-topic", invalid, "Invalid identifier should pass through");
}

@Test
public void testMapEntriesBypassSanitization() {
  Map<String, String> map = Utils.parseTopicToTableMap("myTopic:\"My-Table\",otherTopic:MixedCase");

  // Map entries always pass through regardless of flag
  assertEquals("\"My-Table\"", Utils.getTableName("myTopic", map, true));
  assertEquals("\"My-Table\"", Utils.getTableName("myTopic", map, false));
  assertEquals("MixedCase", Utils.getTableName("otherTopic", map, true));
  assertEquals("MixedCase", Utils.getTableName("otherTopic", map, false));
}
```

**Step 2: Run tests to verify they fail**

```bash
mvn test -pl . -Dtest=UtilsTest#testSanitizationToggle+testMapEntriesBypassSanitization -Dcheckstyle.skip -DargLine="--add-opens java.base/java.util=ALL-UNNAMED"
```

Expected: Compilation errors (method signature mismatch)

**Step 3: Add boolean parameter to method signatures**

Update these method signatures in `Utils.java`:

```java
public static String getTableName(
    String topic, Map<String, String> topic2table, boolean enableSanitization) {
  return generateValidName(topic, topic2table, enableSanitization);
}

public static GeneratedName generateTableName(
    String topic, Map<String, String> topic2table, boolean enableSanitization) {
  return generateValidNameFromMap(topic, topic2table, enableSanitization);
}

public static String generateValidName(
    String topic, Map<String, String> topic2table, boolean enableSanitization) {
  return generateValidNameFromMap(topic, topic2table, enableSanitization).name;
}

private static GeneratedName generateValidNameFromMap(
    String topic, Map<String, String> topic2table, boolean enableSanitization) {
```

**Step 4: Update convertAppName to pass false**

```java
String validAppName = generateValidName(appName, new HashMap<>(), false);
```

**Step 5: Run tests to verify compilation**

```bash
mvn test -pl . -Dtest=UtilsTest -Dcheckstyle.skip -DargLine="--add-opens java.base/java.util=ALL-UNNAMED"
```

Expected: Compilation errors in UtilsTest where old signatures are used

**Step 6: Update all existing test call sites**

Find all calls to `getTableName`, `generateTableName`, `generateValidName` in UtilsTest.java and add `, false` as third parameter. Should be around 15-20 call sites.

**Step 7: Run tests to verify they compile but fail**

```bash
mvn test -pl . -Dtest=UtilsTest#testSanitizationToggle+testMapEntriesBypassSanitization -Dcheckstyle.skip -DargLine="--add-opens java.base/java.util=ALL-UNNAMED"
```

Expected: Tests compile and run but fail because logic not implemented

**Step 8: Implement sanitization logic in generateValidNameFromMap**

Modify `generateValidNameFromMap` in `Utils.java`:

```java
private static GeneratedName generateValidNameFromMap(
    String topic, Map<String, String> topic2table, boolean enableSanitization) {
  final String PLACE_HOLDER = "_";
  if (topic == null || topic.isEmpty()) {
    throw SnowflakeErrors.ERROR_0020.getException("topic name: " + topic);
  }

  // Map entries always bypass sanitization
  if (topic2table.containsKey(topic)) {
    return GeneratedName.fromMap(topic2table.get(topic));
  }

  // try matching regex tables
  for (String regexTopic : topic2table.keySet()) {
    if (topic.matches(regexTopic)) {
      return GeneratedName.fromMap(topic2table.get(regexTopic));
    }
  }

  // Auto-generated name path
  if (Utils.isValidSnowflakeObjectIdentifier(topic)) {
    // Valid identifier - uppercase if sanitization enabled
    if (enableSanitization) {
      return GeneratedName.generated(topic.toUpperCase(Locale.ROOT));
    }
    return GeneratedName.generated(topic);
  }

  // Invalid identifier
  if (!enableSanitization) {
    // Pass through as-is, let Snowflake fail
    return GeneratedName.generated(topic);
  }

  // Legacy sanitization logic with uppercasing
  int hash = Math.abs(topic.hashCode());
  StringBuilder result = new StringBuilder();

  // remove wildcard regex from topic name to generate table name
  topic = topic.replaceAll("\\.\\*", "");

  int index = 0;
  // first char
  if (topic.substring(index, index + 1).matches("[_a-zA-Z]")) {
    result.append(topic.charAt(index));
    index++;
  } else {
    result.append(PLACE_HOLDER);
  }
  while (index < topic.length()) {
    if (topic.substring(index, index + 1).matches("[_$a-zA-Z0-9]")) {
      result.append(topic.charAt(index));
    } else {
      result.append(PLACE_HOLDER);
    }
    index++;
  }

  result.append(PLACE_HOLDER);
  result.append(hash);

  return GeneratedName.generated(result.toString().toUpperCase(Locale.ROOT));
}
```

**Step 9: Add Locale import**

At top of `Utils.java`:

```java
import java.util.Locale;
```

**Step 10: Run tests to verify they pass**

```bash
mvn test -pl . -Dtest=UtilsTest#testSanitizationToggle+testMapEntriesBypassSanitization -Dcheckstyle.skip -DargLine="--add-opens java.base/java.util=ALL-UNNAMED"
```

Expected: Tests pass

**Step 11: Run all UtilsTest tests**

```bash
mvn test -pl . -Dtest=UtilsTest -Dcheckstyle.skip -DargLine="--add-opens java.base/java.util=ALL-UNNAMED"
```

Expected: All tests pass

**Step 12: Commit**

```bash
git add src/main/java/com/snowflake/kafka/connector/Utils.java src/test/java/com/snowflake/kafka/connector/UtilsTest.java
git commit -m "SNOW-3029864: Add sanitization toggle to name generation

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 5: Thread sanitization flag through SnowflakeSinkServiceV2

**Files:**
- Modify: `src/main/java/com/snowflake/kafka/connector/internal/streaming/SnowflakeSinkServiceV2.java`

**Step 1: Add field to read config**

Find the constructor (around line 100-130). Add field declaration:

```java
private final boolean enableSanitization;
```

Initialize in constructor after `tolerateErrors`:

```java
this.enableSanitization =
    Boolean.parseBoolean(
        connectorConfig.getOrDefault(
            KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_TABLE_NAME_SANITIZATION,
            String.valueOf(
                KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_TABLE_NAME_SANITIZATION_DEFAULT)));
```

**Step 2: Add logging**

Add to existing initialization log (around line 135):

```java
LOGGER.info(
    "SnowflakeSinkServiceV2 initialized for connector: {}, task: {}, tolerateErrors: {},"
        + " enableSanitization: {}",
    this.connectorName,
    this.taskId,
    this.tolerateErrors,
    this.enableSanitization);
```

**Step 3: Find getTableName call sites**

Search for `Utils.getTableName` in the file. Should find calls in `startPartitions` method (around line 200-250).

**Step 4: Update getTableName calls**

Update all `Utils.getTableName(topic, topicToTableMap)` calls to pass the flag:

```java
Utils.getTableName(topic, topicToTableMap, this.enableSanitization)
```

**Step 5: Compile check**

```bash
mvn compile -pl . -Dcheckstyle.skip
```

Expected: Successful compilation

**Step 6: Commit**

```bash
git add src/main/java/com/snowflake/kafka/connector/internal/streaming/SnowflakeSinkServiceV2.java
git commit -m "SNOW-3029864: Thread sanitization flag through SnowflakeSinkServiceV2

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 6: Add integration test for sanitization enabled

**Files:**
- Modify: `src/test/java/com/snowflake/kafka/connector/SnowflakeSinkTaskForStreamingIT.java`

**Step 1: Write test**

Add to `SnowflakeSinkTaskForStreamingIT.java`:

```java
@Test
public void testSanitizationEnabledAutoGenerated() throws Exception {
  // Topic with dashes should be sanitized and uppercased
  String topicWithDashes = "test-topic-" + System.currentTimeMillis();
  TopicPartition topicPartition = new TopicPartition(topicWithDashes, 0);

  Map<String, String> config = TestUtils.getConfForStreaming();
  config.put(KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_TABLE_NAME_SANITIZATION, "true");
  config.put(KafkaConnectorConfigParams.TOPICS, topicWithDashes);

  SnowflakeSinkTask task = new SnowflakeSinkTask();
  task.start(config);

  // Create and send records
  List<SinkRecord> records = new ArrayList<>();
  for (int i = 0; i < 5; i++) {
    records.add(
        new SinkRecord(
            topicWithDashes,
            0,
            null,
            null,
            null,
            "{\"f1\":\"value" + i + "\"}",
            i));
  }

  task.put(records);
  task.close(Collections.singleton(topicPartition));

  // Verify table name is sanitized and uppercased
  String expectedTablePattern = "TEST_TOPIC_" + topicWithDashes.split("-")[2] + "_";
  SnowflakeConnectionService conn = getConnectionServiceWithEncryptedKey();

  // Find the table
  ResultSet rs = conn.listTable();
  String foundTable = null;
  while (rs.next()) {
    String tableName = rs.getString("name");
    if (tableName.startsWith(expectedTablePattern)) {
      foundTable = tableName;
      break;
    }
  }

  assertNotNull(foundTable, "Should find sanitized uppercased table");
  assertTrue(foundTable.matches("^[A-Z_0-9]+$"), "Table name should be fully uppercased");

  // Verify data
  ResultSet data = TestUtils.showTable(foundTable);
  int count = 0;
  while (data.next()) {
    count++;
  }
  assertEquals(5, count, "Should have 5 rows");

  // Cleanup
  conn.executeQueryWithParameters(format("drop table if exists identifier(?)", foundTable));
}
```

**Step 2: Run test with profile**

```bash
export SNOWFLAKE_CREDENTIAL_FILE=/home/bmikaili/snowflake-kafka-connector/profile.json
mvn failsafe:integration-test failsafe:verify -pl . -Dit.test="SnowflakeSinkTaskForStreamingIT#testSanitizationEnabledAutoGenerated" -Dcheckstyle.skip -DargLine="--add-opens java.base/java.util=ALL-UNNAMED"
```

Expected: Test passes

**Step 3: Commit**

```bash
git add src/test/java/com/snowflake/kafka/connector/SnowflakeSinkTaskForStreamingIT.java
git commit -m "SNOW-3029864: Add IT for sanitization enabled

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 7: Add integration test for sanitization disabled with quoted map

**Files:**
- Modify: `src/test/java/com/snowflake/kafka/connector/SnowflakeSinkTaskForStreamingIT.java`

**Step 1: Write test**

Add to `SnowflakeSinkTaskForStreamingIT.java`:

```java
@Test
public void testSanitizationDisabledQuotedMap() throws Exception {
  String topic = "test_topic_" + System.currentTimeMillis();
  String quotedTableName = "\"Test-Table-" + System.currentTimeMillis() + "\"";
  TopicPartition topicPartition = new TopicPartition(topic, 0);

  Map<String, String> config = TestUtils.getConfForStreaming();
  config.put(KafkaConnectorConfigParams.SNOWFLAKE_ENABLE_TABLE_NAME_SANITIZATION, "false");
  config.put(KafkaConnectorConfigParams.SNOWFLAKE_TOPICS2TABLE_MAP, topic + ":" + quotedTableName);
  config.put(KafkaConnectorConfigParams.TOPICS, topic);

  SnowflakeSinkTask task = new SnowflakeSinkTask();
  task.start(config);

  // Create and send records
  List<SinkRecord> records = new ArrayList<>();
  for (int i = 0; i < 3; i++) {
    records.add(
        new SinkRecord(
            topic,
            0,
            null,
            null,
            null,
            "{\"f1\":\"value" + i + "\"}",
            i));
  }

  task.put(records);
  task.close(Collections.singleton(topicPartition));

  // Verify table exists with exact quoted name (case preserved)
  SnowflakeConnectionService conn = getConnectionServiceWithEncryptedKey();
  assertTrue(TestUtils.tableExist(quotedTableName), "Quoted table should exist");

  // Verify data
  ResultSet data = TestUtils.showTable(quotedTableName);
  int count = 0;
  while (data.next()) {
    count++;
  }
  assertEquals(3, count, "Should have 3 rows");

  // Cleanup
  conn.executeQueryWithParameters(format("drop table if exists identifier(?)", quotedTableName));
}
```

**Step 2: Run test with profile**

```bash
export SNOWFLAKE_CREDENTIAL_FILE=/home/bmikaili/snowflake-kafka-connector/profile.json
mvn failsafe:integration-test failsafe:verify -pl . -Dit.test="SnowflakeSinkTaskForStreamingIT#testSanitizationDisabledQuotedMap" -Dcheckstyle.skip -DargLine="--add-opens java.base/java.util=ALL-UNNAMED"
```

Expected: Test passes

**Step 3: Commit**

```bash
git add src/test/java/com/snowflake/kafka/connector/SnowflakeSinkTaskForStreamingIT.java
git commit -m "SNOW-3029864: Add IT for sanitization disabled with quoted map

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Task 8: Run formatter and verify all tests

**Files:**
- All modified files

**Step 1: Run formatter**

```bash
bash -c "source ~/.sdkman/bin/sdkman-init.sh && sdk use java 17.0.13-zulu && ./format.sh"
```

**Step 2: Check formatting changes**

```bash
git status --short
```

**Step 3: Commit formatting if needed**

```bash
git add -A
git commit -m "SNOW-3029864: Apply code formatting" || echo "No formatting changes"
```

**Step 4: Run all unit tests**

```bash
mvn test -pl . -Dcheckstyle.skip -DargLine="--add-opens java.base/java.util=ALL-UNNAMED"
```

Expected: All tests pass

**Step 5: Run integration tests**

```bash
export SNOWFLAKE_CREDENTIAL_FILE=/home/bmikaili/snowflake-kafka-connector/profile.json
mvn failsafe:integration-test failsafe:verify -pl . -Dit.test="SnowflakeSinkTaskForStreamingIT#testSanitizationEnabledAutoGenerated+testSanitizationDisabledQuotedMap" -Dcheckstyle.skip -DargLine="--add-opens java.base/java.util=ALL-UNNAMED"
```

Expected: Both IT tests pass

---

## Task 9: Create pull request

**Step 1: Push branch**

```bash
git push -u origin bmikaili-support-case-sensitive-pipes-and-tables
```

**Step 2: Create PR**

```bash
gh pr create --title "SNOW-3029864: Add table name sanitization toggle for case-sensitive names" --body "$(cat <<'EOF'
## Summary

Adds config toggle to control table name sanitization and enables quoted identifiers in topic2table.map.

**Config validation**: Updated `isValidSnowflakeTableName` regex to accept `"..."` patterns. Users can now use quoted table names in `topic2table.map`.

**Sanitization toggle**: Added `snowflake.enable.table.name.sanitization` config (default `true`). When enabled, auto-generated table names are sanitized and uppercased (v3 compatible). When disabled, names pass through as-is.

### Behavior

| Scenario | Sanitization=true | Sanitization=false |
|----------|-------------------|---------------------|
| Map: `topic:"My-Table"` | `"My-Table"` (case preserved) | `"My-Table"` (case preserved) |
| Map: `topic:MyTable` | `MYTABLE` (uppercased) | `MyTable` (SF uppercases) |
| Auto: `MyTopic` | `MYTOPIC` (uppercased) | `MyTopic` (SF uppercases) |
| Auto: `my-topic` | `MY_TOPIC_<hash>` (sanitized+uppercased) | `my-topic` (SF fails) |

### Files changed

- `Utils.java` — regex update, sanitization toggle, explicit uppercasing
- `Constants.java` — new config constant
- `ConnectorConfigDefinition.java` — config registration
- `SnowflakeSinkServiceV2.java` — reads flag, passes to Utils

## Test plan

- [x] Unit tests in `UtilsTest` verify validation, sanitization toggle, map bypass
- [x] IT test: sanitization=true with dashed topic creates uppercased sanitized table
- [x] IT test: sanitization=false with quoted map entry creates case-preserved table

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)" --base master
```

Expected: PR created successfully

---

## Execution Complete

All tasks implemented. The connector now:
1. Accepts quoted identifiers in `topic2table.map`
2. Has a sanitization toggle (default true for v3 compatibility)
3. Explicitly uppercases when sanitization enabled
4. Passes names through when sanitization disabled
5. Has unit and integration test coverage
