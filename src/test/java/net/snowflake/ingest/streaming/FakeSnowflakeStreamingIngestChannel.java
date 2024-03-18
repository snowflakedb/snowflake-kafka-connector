package net.snowflake.ingest.streaming;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import net.snowflake.ingest.streaming.internal.ColumnProperties;

/**
 * Fake implementation of {@link SnowflakeStreamingIngestChannel} which stores state in memory.
 * Should work as a drop in replacement for the original one. The implementation is able to keep
 * state between calls: so inserting rows through {@link SnowflakeStreamingIngestChannel#insertRow}
 * will update the {@link SnowflakeStreamingIngestChannel#getLatestCommittedOffsetToken()}.
 *
 * <p>The only method that does is not supported is {@link
 * SnowflakeStreamingIngestChannel#getTableSchema}
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

  private List<Map<String, Object>> rows = new LinkedList<>();

  public FakeSnowflakeStreamingIngestChannel(
      String name, String dbName, String schemaName, String tableName) {
    this.name = name;
    this.dbName = dbName;
    this.schemaName = schemaName;
    this.tableName = tableName;
    this.fullyQualifiedName = String.format("%s.%s.%s.%s", dbName, schemaName, tableName, name);
    this.fullyQualifiedTableName = String.format("%s.%s.%s", dbName, schemaName, tableName);
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
    return false;
  }

  @Override
  public CompletableFuture<Void> close() {
    closed = true;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> close(boolean drop) {
    closed = true;
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public InsertValidationResponse insertRow(Map<String, Object> row, String offsetToken) {
    rows.add(row);
    this.offsetToken = offsetToken;
    return new InsertValidationResponse();
  }

  @Override
  public InsertValidationResponse insertRows(
      Iterable<Map<String, Object>> rows, String startOffsetToken, String endOffsetToken) {
    rows.forEach(r -> this.rows.add(new LinkedHashMap<>(r)));
    this.offsetToken = endOffsetToken;
    return new InsertValidationResponse();
  }

  @Override
  public InsertValidationResponse insertRows(
      Iterable<Map<String, Object>> rows, String offsetToken) {
    rows.forEach(r -> this.rows.add(new LinkedHashMap<>(r)));
    this.offsetToken = offsetToken;
    return new InsertValidationResponse();
  }

  @Override
  public String getLatestCommittedOffsetToken() {
    return offsetToken;
  }

  @Override
  public Map<String, ColumnProperties> getTableSchema() {
    throw new UnsupportedOperationException("Method not implemented in fake");
  }
}
