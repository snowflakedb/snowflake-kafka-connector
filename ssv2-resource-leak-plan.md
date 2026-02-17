# SSv2 Resource Leak Analysis & Implementation Plan

## Overview

This document identifies 11 resource leak paths in the Snowflake Streaming v2 (SSv2) SDK integration, where `SnowflakeStreamingIngestClient`, `SnowflakeStreamingIngestChannel`, and `SnowflakeConnectionService` resources may not be properly closed under exception conditions.

- 2 HIGH severity client leak paths
- 7 MEDIUM severity channel/connection leak paths
- 2 LOW-MEDIUM severity cleanup issues

---

## Issue Catalog

### Issue #1 — Client Leak in `SnowflakeSinkServiceV2.stop()` (HIGH)

**File:** `src/main/java/com/snowflake/kafka/connector/internal/streaming/SnowflakeSinkServiceV2.java:426-437`

**Problem:** If `waitForAllChannelsToCommitData()` throws (timeout, network error, `CompletionException`), `StreamingClientManager.closeTaskClients()` is never called, leaking all SSv2 clients for this task.

```java
@Override
public void stop() {
    waitForAllChannelsToCommitData();  // <-- NO EXCEPTION HANDLING
    StreamingClientManager.closeTaskClients(connectorName, taskId);  // <-- NEVER REACHED ON EXCEPTION
}
```

**Impact:** `SnowflakeStreamingIngestClient` instances remain open indefinitely. Memory leak (clients hold buffers and metadata). Network connections remain open. Server-side resources not released.

---

### Issue #2 — Partial Client Cleanup in `StreamingClientManager.closeTaskClients()` (HIGH)

**File:** `src/main/java/com/snowflake/kafka/connector/internal/streaming/v2/StreamingClientManager.java:173-197`

**Problem:** If `client.close()` at line 191 throws an exception, the `removeIf` iterator terminates immediately. Remaining clients in the map are never closed. Map state becomes inconsistent (some tasks removed, clients not closed).

```java
synchronized void closeTaskClients(final String taskId) {
    pipeToTasks.entrySet().removeIf(entry -> {
        if (tasks.remove(taskId) && tasks.isEmpty()) {
            SnowflakeStreamingIngestClient client = clients.remove(pipeName);
            if (client != null) {
                client.close();  // <-- NO TRY-CATCH, ABORTS ITERATION ON EXCEPTION
            }
            return true;
        }
        return false;
    });
}
```

**Impact:** Multiple `SnowflakeStreamingIngestClient` instances leak when closing one client fails. Accumulates over time as tasks restart.

---

### Issue #3 — Channel Leak During Fallback Recovery (MEDIUM)

**File:** `src/main/java/com/snowflake/kafka/connector/internal/streaming/v2/SnowpipeStreamingPartitionChannel.java:376-377`

**Problem:** If `channel.close()` throws, `openChannelForTable()` is never called. The channel reference becomes invalid but the task continues to use it.

```java
private long streamingApiFallbackSupplier(...) {
    if (!channel.isClosed()) {
        channel.close();  // <-- NO TRY-CATCH, CAN THROW SFException
    }
    SnowflakeStreamingIngestChannel newChannel = openChannelForTable(channelName);  // <-- NEVER REACHED
    // ...
}
```

**Impact:** Channel becomes permanently unusable. Data ingestion stops for this partition. Requires task restart to recover.

---

### Issue #4 — Channel Leak in `openChannelForTable()` (MEDIUM)

**File:** `src/main/java/com/snowflake/kafka/connector/internal/streaming/v2/SnowpipeStreamingPartitionChannel.java:511-512`

**Problem:** If `this.channel.close()` throws, `streamingIngestClient.openChannel()` is never called. The old channel reference remains but is now invalid.

```java
private SnowflakeStreamingIngestChannel openChannelForTable(final String channelName) {
    final SnowflakeStreamingIngestClient streamingIngestClient = StreamingClientManager.getClient(...);
    if (channelIsOpen()) {
        this.channel.close();  // <-- NO TRY-CATCH
    }
    final OpenChannelResult result = streamingIngestClient.openChannel(channelName, null);  // <-- NEVER REACHED
    // ...
}
```

**Impact:** Channel stuck in invalid state. All subsequent operations on the channel fail. Partition ingestion stops.

---

### Issue #5 — `closeChannelWrapped()` Only Catches `SFException` (LOW-MEDIUM)

**File:** `src/main/java/com/snowflake/kafka/connector/internal/streaming/v2/SnowpipeStreamingPartitionChannel.java:543-552`

**Problem:** Only catches `SFException`. If `channel.close()` throws any other exception (`RuntimeException`, `NullPointerException`, etc.), it propagates uncaught. Telemetry in `onCloseChannelSuccess()` never executes.

```java
private CompletableFuture<Void> closeChannelWrapped() {
    try {
        return CompletableFuture.runAsync(() -> channel.close());
    } catch (SFException e) {  // <-- ONLY SFException, NOT Exception
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(e);
        return future;
    }
}
```

**Impact:** Silent failures in async close operations. Telemetry data not sent.

---

### Issue #6 — Channels Not Removed from Map on Async Close Failure (MEDIUM)

**File:** `src/main/java/com/snowflake/kafka/connector/internal/streaming/SnowflakeSinkServiceV2.java:420-422`

**Problem:** If `closeChannelAsync()` fails, the `thenAccept()` callback is never invoked. Channel remains in `partitionsToChannel` map indefinitely.

```java
return topicPartitionChannel
    .closeChannelAsync()
    .thenAccept(__ -> partitionsToChannel.remove(key));  // <-- ONLY RUNS ON SUCCESS
```

**Impact:** Closed or invalid channels accumulate in the map. Stale references prevent garbage collection. Map grows over time during rebalancing.

---

### Issue #7 — `testConnection` Never Closed in `validate()` (MEDIUM)

**File:** `src/main/java/com/snowflake/kafka/connector/SnowflakeStreamingSinkConnector.java:228-311`

**Problem:** The `validate()` method creates a `SnowflakeConnectionService` at line 230 via `SnowflakeConnectionServiceFactory.builder()...build()` but never calls `testConnection.close()` on any code path — neither on success nor on any of the exception handlers.

```java
SnowflakeConnectionService testConnection;
try {
    testConnection = SnowflakeConnectionServiceFactory.builder().setProperties(connectorConfigs).build();
} catch (SnowflakeKafkaConnectorException e) {
    // ... handles errors but never closes testConnection
    return result;
}

testConnection.databaseExists(...);  // uses connection
testConnection.schemaExists(...);    // uses connection
// <-- testConnection NEVER CLOSED
return result;
```

**Impact:** Every call to `validate()` leaks a JDBC connection. Kafka Connect calls `validate()` during connector configuration — this happens on every config change.

---

### Issue #8 — Connector-Level `conn` Never Closed in `stop()` (MEDIUM)

**File:** `src/main/java/com/snowflake/kafka/connector/SnowflakeStreamingSinkConnector.java:57,115,134-142`

**Problem:** The connector's `start()` creates `conn` at line 115 but `stop()` only reports telemetry — it never calls `conn.close()`.

```java
@Override
public void stop() {
    setupComplete = false;
    if (telemetryClient != null) {
        telemetryClient.reportKafkaConnectStop(connectorStartTime);
    }
    // <-- NO conn.close()
}
```

**Impact:** Leaks a JDBC connection per connector lifecycle. Each connector restart leaks another connection.

---

### Issue #9 — Task-Level `conn` Never Closed in `stop()` (MEDIUM)

**File:** `src/main/java/com/snowflake/kafka/connector/SnowflakeSinkTask.java:73,186,230-245`

**Problem:** `SnowflakeSinkTask.start()` creates a `SnowflakeConnectionService` at line 186 and stores it in `this.conn`. `stop()` calls `this.sink.stop()` but never calls `this.conn.close()`. The `SnowflakeSinkServiceV2` doesn't own or close `conn` either — it only uses it.

```java
@Override
public void stop() {
    if (this.telemetryReporter != null) {
        this.telemetryReporter.stop();
    }
    if (this.sink != null) {
        this.sink.stop();
    }
    // <-- NO this.conn.close()
}
```

**Impact:** Each task restart leaks a JDBC connection. If task is rebalanced, connection leaks. Connection pools eventually exhaust.

---

### Issue #10 — Channel Overwrite Without Cleanup in `createStreamingChannelForTopicPartition()` (MEDIUM)

**File:** `src/main/java/com/snowflake/kafka/connector/internal/streaming/SnowflakeSinkServiceV2.java:200-238`

**Problem:** The method comment explicitly states: *"This is essentially a blind write to partitionsToChannel."* Line 237 does `partitionsToChannel.put(channelName, partitionChannel)` — if a channel already exists for that key, the old `SnowpipeStreamingPartitionChannel` (and its underlying `SnowflakeStreamingIngestChannel`) is silently replaced and never closed.

This happens during:
- `insert()` at line 292 when `isChannelClosed()` is true (lazy recreation)
- `startPartitions()` called twice for the same partitions (rebalance edge cases)

```java
// Line 200 comment: "This is essentially a blind write to partitionsToChannel.
// i.e. we do not check if it is presented or not."
private void createStreamingChannelForTopicPartition(...) {
    final SnowpipeStreamingPartitionChannel partitionChannel = new SnowpipeStreamingPartitionChannel(...);
    partitionsToChannel.put(channelName, partitionChannel);  // <-- BLIND PUT, OLD CHANNEL LEAKED
}
```

**Impact:** Old channel's underlying `SnowflakeStreamingIngestChannel` remains open. Orphaned channels accumulate server-side resources.

---

### Issue #11 — Static `connectors` Map Never Pruned (LOW-MEDIUM)

**File:** `src/main/java/com/snowflake/kafka/connector/internal/streaming/v2/StreamingClientManager.java:40`

**Problem:** The `connectors` ConcurrentHashMap is static and entries are never removed. `closeTaskClients()` closes individual clients but the `ConnectorIngestClients` wrapper persists in the map even when all its clients and tasks are gone.

```java
private static final Map<String, ConnectorIngestClients> connectors = new ConcurrentHashMap<>();
// ^^ STATIC, NEVER CLEANED UP
```

**Impact:** In long-running Kafka Connect clusters with connector restarts, this accumulates empty `ConnectorIngestClients` objects indefinitely.

---

## Summary Table

| #  | Issue | Severity | File | Resource Type | Trigger |
|----|-------|----------|------|---------------|---------|
| 1  | `stop()` without finally | HIGH | SnowflakeSinkServiceV2.java:432 | Client | Exception in waitForAllChannelsToCommitData() |
| 2  | Partial client cleanup | HIGH | StreamingClientManager.java:191 | Client | Exception in client.close() |
| 3  | Fallback recovery failure | MEDIUM | SnowpipeStreamingPartitionChannel.java:377 | Channel | Exception in channel.close() |
| 4  | openChannelForTable() failure | MEDIUM | SnowpipeStreamingPartitionChannel.java:512 | Channel | Exception in channel.close() |
| 5  | Async close exception | LOW-MED | SnowpipeStreamingPartitionChannel.java:545 | Channel | Non-SFException during close |
| 6  | Map not cleaned on failure | MEDIUM | SnowflakeSinkServiceV2.java:420 | Memory | Exception in closeChannelAsync() |
| 7  | testConnection not closed | MEDIUM | SnowflakeStreamingSinkConnector.java:230 | JDBC | Every validate() call |
| 8  | Connector conn not closed | MEDIUM | SnowflakeStreamingSinkConnector.java:135 | JDBC | Every connector stop() |
| 9  | Task conn not closed | MEDIUM | SnowflakeSinkTask.java:230 | JDBC | Every task stop() |
| 10 | Channel blind overwrite | MEDIUM | SnowflakeSinkServiceV2.java:237 | Channel | Rebalance or lazy recreation |
| 11 | Static map never pruned | LOW-MED | StreamingClientManager.java:40 | Memory | Connector restarts over time |

---

## Design Decision: Manual Fixes Over Generic Abstractions

We evaluated whether generic patterns (utility classes, composite closeables) could systematically prevent these leaks. The conclusion is: **use manual fixes with one interface-level change.**

### What we considered

| Approach | Verdict | Reasoning |
|---|---|---|
| `SnowflakeConnectionService extends AutoCloseable` | **Do it** | Zero-cost, source-compatible (already has `close()`). Enables try-with-resources for short-lived connections and IDE/linter warnings. |
| `SafeClose` utility class | Skip | Replaces a 5-line try-catch-log with a one-liner, but only 4 call sites. Not enough to justify a new class. The manual pattern is standard Java every developer reads instantly. |
| `ResourceCleaner` / composite closeable | Skip | Reimplements try-finally with extra steps. The `stop()` methods have 2-3 cleanup actions each — a nested try-finally is completely clear without needing to learn a custom utility's registration/execution semantics. |

### Why generic solutions don't fit most of these issues

The resource lifecycles in this codebase fall into categories that resist try-with-resources:

- **Long-lived resources** (task/connector `conn`) span `start()` to `stop()` across method boundaries. No single scope owns them.
- **Shared resources with reference counting** (`StreamingClientManager` clients) are used by multiple tasks. Closing happens when the last task stops, not at scope exit.
- **Async close** (`TopicPartitionChannel.closeChannelAsync()`) returns `CompletableFuture` — fundamentally incompatible with try-with-resources.
- **Close-and-reopen** (channel recovery) is a state transition, not a scoped lifecycle.

### What we ARE doing

1. **`SnowflakeConnectionService extends AutoCloseable`** — one-token interface change, enables try-with-resources where lifecycle permits (issue #7), and provides IDE warnings going forward.
2. **Manual try-catch/try-finally for everything else** — standard Java idioms that every developer understands immediately.

---

## Implementation Plan

### Phase 0: Interface Change

#### `SnowflakeConnectionService extends AutoCloseable`

**File:** `SnowflakeConnectionService.java`

Add `extends AutoCloseable` to the interface. This is source-compatible since the interface already declares `void close()` which matches `AutoCloseable.close()`.

```java
public interface SnowflakeConnectionService extends AutoCloseable {
```

This enables try-with-resources for issue #7 and provides IDE warnings when connections are not closed.

---

### Phase 1: HIGH Severity (Issues #1, #2)

#### Issue #1 — Add try-finally to `SnowflakeSinkServiceV2.stop()`

**File:** `SnowflakeSinkServiceV2.java`

Wrap `waitForAllChannelsToCommitData()` in try-catch with `StreamingClientManager.closeTaskClients()` in a `finally` block:

```java
@Override
public void stop() {
    LOGGER.info("Stopping SnowflakeSinkServiceV2 for connector: {}, task: {}",
        this.connectorName, this.taskId);
    try {
        waitForAllChannelsToCommitData();
    } catch (Exception e) {
        LOGGER.error("Error waiting for channels to commit data during stop: {}", e.getMessage(), e);
    } finally {
        try {
            StreamingClientManager.closeTaskClients(connectorName, taskId);
        } catch (Exception e) {
            LOGGER.error("Error closing task clients: {}", e.getMessage(), e);
        }
    }
}
```

#### Issue #2 — Wrap `client.close()` in try-catch inside `closeTaskClients()`

**File:** `StreamingClientManager.java`

Inside the `removeIf` lambda, wrap `client.close()` in try-catch. Log errors but continue iterating:

```java
synchronized void closeTaskClients(final String taskId) {
    LOGGER.info("Releasing clients for task {} in connector {}", taskId, connectorName);
    List<Exception> closeErrors = new ArrayList<>();
    pipeToTasks.entrySet().removeIf(entry -> {
        String pipeName = entry.getKey();
        Set<String> tasks = entry.getValue();
        if (tasks.remove(taskId) && tasks.isEmpty()) {
            SnowflakeStreamingIngestClient client = clients.remove(pipeName);
            if (client != null) {
                try {
                    LOGGER.info("Closing client for pipe {} in connector {} (last task stopped)",
                        pipeName, connectorName);
                    client.close();
                } catch (Exception e) {
                    LOGGER.error("Failed to close client for pipe {}: {}", pipeName, e.getMessage(), e);
                    closeErrors.add(e);
                }
            }
            return true;
        }
        return false;
    });
    if (!closeErrors.isEmpty()) {
        LOGGER.error("Encountered {} errors while closing clients for task {}", closeErrors.size(), taskId);
    }
}
```

---

### Phase 2: MEDIUM Severity — Channel Close Safety (Issues #3, #4, #5)

#### Issue #3 — Try-catch around `channel.close()` in `streamingApiFallbackSupplier()`

**File:** `SnowpipeStreamingPartitionChannel.java`

```java
if (!channel.isClosed()) {
    try {
        channel.close();
    } catch (Exception e) {
        LOGGER.warn("Failed to close old channel during recovery: {}", e.getMessage(), e);
    }
}
```

#### Issue #4 — Try-catch around `this.channel.close()` in `openChannelForTable()`

**File:** `SnowpipeStreamingPartitionChannel.java`

```java
if (channelIsOpen()) {
    try {
        this.channel.close();
    } catch (Exception e) {
        LOGGER.warn("Failed to close existing channel before reopening: {}", e.getMessage(), e);
    }
}
```

#### Issue #5 — Catch all exceptions in `closeChannelWrapped()`

**File:** `SnowpipeStreamingPartitionChannel.java`

Change `catch (SFException e)` to `catch (Exception e)`:

```java
private CompletableFuture<Void> closeChannelWrapped() {
    try {
        return CompletableFuture.runAsync(() -> channel.close());
    } catch (Exception e) {
        LOGGER.warn("Exception while initiating channel close: {}", e.getMessage(), e);
        CompletableFuture<Void> future = new CompletableFuture<>();
        future.completeExceptionally(e);
        return future;
    }
}
```

---

### Phase 3: MEDIUM Severity — Map and Connection Cleanup (Issues #6, #7, #8, #9, #10)

#### Issue #6 — Use `whenComplete()` instead of `thenAccept()` in `closeTopicPartition()`

**File:** `SnowflakeSinkServiceV2.java`

```java
return topicPartitionChannel
    .closeChannelAsync()
    .whenComplete((result, exception) -> {
        partitionsToChannel.remove(key);
        if (exception != null) {
            LOGGER.error("Error closing channel for key {}: {}", key, exception.getMessage(), exception);
        }
    });
```

#### Issue #7 — Close `testConnection` in `validate()` using try-with-resources

**File:** `SnowflakeStreamingSinkConnector.java`

Now that `SnowflakeConnectionService extends AutoCloseable` (Phase 0), use try-with-resources:

```java
try (SnowflakeConnectionService testConnection =
        SnowflakeConnectionServiceFactory.builder().setProperties(connectorConfigs).build()) {
    testConnection.databaseExists(...);
    testConnection.schemaExists(...);
} catch (SnowflakeKafkaConnectorException e) {
    // ... existing error handling ...
}
```

#### Issue #8 — Close connector-level `conn` in `stop()`

**File:** `SnowflakeStreamingSinkConnector.java`

```java
@Override
public void stop() {
    LOGGER.info("SnowflakeStreamingSinkConnector connector stopping...");
    setupComplete = false;
    if (telemetryClient != null) {
        telemetryClient.reportKafkaConnectStop(connectorStartTime);
    }
    if (conn != null) {
        try {
            conn.close();
        } catch (Exception e) {
            LOGGER.warn("Failed to close connector connection: {}", e.getMessage(), e);
        }
    }
}
```

#### Issue #9 — Close task-level `conn` in `stop()`

**File:** `SnowflakeSinkTask.java`

```java
@Override
public void stop() {
    DYNAMIC_LOGGER.info("stopping task {}", this.taskConfigId);
    if (this.telemetryReporter != null) {
        this.telemetryReporter.stop();
    }
    if (this.sink != null) {
        this.sink.stop();
    }
    if (this.conn != null) {
        try {
            this.conn.close();
        } catch (Exception e) {
            DYNAMIC_LOGGER.warn("Failed to close connection: {}", e.getMessage(), e);
        }
    }
    DYNAMIC_LOGGER.info("task stopped, total task runtime: {} milliseconds",
        getDurationFromStartMs(this.taskStartTime));
}
```

#### Issue #10 — Close old channel before overwrite in `createStreamingChannelForTopicPartition()`

**File:** `SnowflakeSinkServiceV2.java`

Before the `put()`, close any existing channel:

```java
private void createStreamingChannelForTopicPartition(...) {
    final String channelName = makeChannelName(...);
    // ...
    TopicPartitionChannel existing = partitionsToChannel.get(channelName);
    if (existing != null) {
        LOGGER.warn("Replacing existing channel: {}", channelName);
        try {
            existing.closeChannelAsync();
        } catch (Exception e) {
            LOGGER.warn("Failed to close existing channel {}: {}", channelName, e.getMessage(), e);
        }
    }
    // ... create new channel ...
    partitionsToChannel.put(channelName, partitionChannel);
}
```

---

### Phase 4: LOW Severity (Issue #11)

#### Issue #11 — Prune empty entries from static `connectors` map

**File:** `StreamingClientManager.java`

After `clients.closeTaskClients(taskId)`, check if the `ConnectorIngestClients` is empty and remove:

```java
public static void closeTaskClients(final String connectorName, final String taskId) {
    synchronized (connectors) {
        ConnectorIngestClients clients = connectors.get(connectorName);
        if (clients != null) {
            clients.closeTaskClients(taskId);
            if (clients.pipeToTasks.isEmpty() && clients.clients.isEmpty()) {
                connectors.remove(connectorName);
                LOGGER.info("Removed empty client manager for connector: {}", connectorName);
            }
        } else {
            LOGGER.warn("Attempted to release task {} for unknown connector: {}", taskId, connectorName);
        }
    }
}
```

---

### Phase 5: Testing

Write unit tests for each fix:

1. **Issue #1 test:** Verify `closeTaskClients()` is called even when `waitForAllChannelsToCommitData()` throws.
2. **Issue #2 test:** Verify all clients are attempted for close even when one throws.
3. **Issues #3, #4 test:** Verify new channel is opened even when old channel close fails.
4. **Issue #5 test:** Verify non-`SFException` is wrapped in failed `CompletableFuture`.
5. **Issue #6 test:** Verify `partitionsToChannel` is cleaned up even when `closeChannelAsync()` fails.
6. **Issue #7 test:** Verify `testConnection` is closed after `validate()`.
7. **Issues #8, #9 test:** Verify `conn.close()` is called in both connector and task `stop()`.
8. **Issue #10 test:** Verify old channel is closed when `createStreamingChannelForTopicPartition()` replaces an existing entry.
9. **Issue #11 test:** Verify connector entries are removed from `StreamingClientManager.connectors` when all clients are closed.

Existing test fakes (`FakeSnowflakeStreamingIngestClient`, `FakeSnowflakeStreamingIngestChannel`) and `StreamingClientManagerIT` can be extended by configuring fakes to throw on `close()`.

---

### Execution Order

Phase 0 first (interface change), then Phases 1-4 in parallel since they touch different files/methods. Phase 5 (testing) should follow each fix. Commit per-phase so each fix is reviewable independently.

### Files Modified

| File | Issues |
|------|--------|
| `SnowflakeConnectionService.java` | Phase 0 (AutoCloseable) |
| `SnowflakeSinkServiceV2.java` | #1, #6, #10 |
| `StreamingClientManager.java` | #2, #11 |
| `SnowpipeStreamingPartitionChannel.java` | #3, #4, #5 |
| `SnowflakeStreamingSinkConnector.java` | #7, #8 |
| `SnowflakeSinkTask.java` | #9 |
