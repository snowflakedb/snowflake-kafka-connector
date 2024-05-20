package net.snowflake.ingest.streaming;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import net.snowflake.ingest.streaming.internal.ColumnProperties;

/**
 * Fake implementation of {@link SnowflakeStreamingIngestChannel} which stores state in memory.
 * Should work as a drop in replacement for the original one. The implementation is able to keep
 * state between calls: so inserting rows through {@link SnowflakeStreamingIngestChannel#insertRow}
 * will update the {@link SnowflakeStreamingIngestChannel#getLatestCommittedOffsetToken()}.
 *
 * <p>The only method that does is not supported is {@link
 * SnowflakeStreamingIngestChannel#getTableSchema} Thread safety is based on {@link ReentrantLock}
 */
public class FakeSnowflakeStreamingIngestChannel implements SnowflakeStreamingIngestChannel {

  private final String name;
  private final String fullyQualifiedName;
  private final String dbName;
  private final String schemaName;
  private final String tableName;
  private final String fullyQualifiedTableName;
  private boolean closed;
  private String offsetToken;

  /** Reference to the client that owns this channel */
  private final SnowflakeStreamingIngestClient owningClient;
  /** Lock used to protect the buffers from concurrent read/write */
  private final Lock bufferLock;

  private List<Map<String, Object>> rows = new LinkedList<>();

  public FakeSnowflakeStreamingIngestChannel(
      SnowflakeStreamingIngestClient owningClient,
      String name,
      String dbName,
      String schemaName,
      String tableName) {
    this.owningClient = owningClient;
    Objects.requireNonNull(name);
    Objects.requireNonNull(dbName);
    Objects.requireNonNull(schemaName);
    Objects.requireNonNull(tableName);
    this.name = name;
    this.dbName = dbName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.fullyQualifiedName = String.format("%s.%s.%s.%s", dbName, schemaName, tableName, name);
    this.fullyQualifiedTableName = String.format("%s.%s.%s", dbName, schemaName, tableName);
    this.bufferLock = new ReentrantLock();
  }

  @Override
  public String getFullyQualifiedName() {
    return fullyQualifiedName;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getDBName() {
    return dbName;
  }

  @Override
  public String getSchemaName() {
    return schemaName;
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public String getFullyQualifiedTableName() {
    return fullyQualifiedTableName;
  }

  @Override
  public boolean isValid() {
    return true;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public CompletableFuture<Void> close() {
    return close(false);
  }

  @Override
  public CompletableFuture<Void> close(boolean drop) {
    closed = true;
    if (drop) {
      owningClient.dropChannel(
          DropChannelRequest.builder(name)
              .setTableName(tableName)
              .setDBName(dbName)
              .setSchemaName(schemaName)
              .build());
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public InsertValidationResponse insertRow(Map<String, Object> row, String offsetToken) {
    return this.insertRows(Lists.newArrayList(row), null, offsetToken);
  }

  @Override
  public InsertValidationResponse insertRows(
      Iterable<Map<String, Object>> rows, String startOffsetToken, String endOffsetToken) {
    final List<Map<String, Object>> rowsCopy = new LinkedList<>();
    rows.forEach(r -> rowsCopy.add(new LinkedHashMap<>(r)));
    bufferLock.lock();
    try {
      this.rows.addAll(rowsCopy);
      this.offsetToken = endOffsetToken;
    } finally {
      bufferLock.unlock();
    }
    return new InsertValidationResponse();
  }

  @Override
  public InsertValidationResponse insertRows(
      Iterable<Map<String, Object>> rows, String offsetToken) {
    return this.insertRows(rows, null, offsetToken);
  }

  @Override
  public String getLatestCommittedOffsetToken() {
    bufferLock.lock();
    try {
      return offsetToken;
    } finally {
      bufferLock.unlock();
    }
  }

  @Override
  public Map<String, ColumnProperties> getTableSchema() {
    throw new UnsupportedOperationException("Method is unsupported in fake communication channel");
  }

  List<Map<String, Object>> getRows() {
    return ImmutableList.copyOf(this.rows);
  }
}
