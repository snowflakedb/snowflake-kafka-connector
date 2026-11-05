package com.snowflake.kafka.connector.internal.streaming.v2.evolveschema;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/** 200 response of the SSv2 {@code :evolve-schema} pipe endpoint. */
@JsonIgnoreProperties(ignoreUnknown = true)
public final class EvolveSchemaResponse {

  private final List<String> columnsAdded;
  private final List<String> columnsAlreadyPresent;
  private final List<String> columnsTypeChanged;
  private final List<String> columnsMadeNullable;
  private final boolean pipeShutdown;

  @JsonCreator
  public EvolveSchemaResponse(
      @JsonProperty("columns_added") List<String> columnsAdded,
      @JsonProperty("columns_already_present") List<String> columnsAlreadyPresent,
      @JsonProperty("columns_type_changed") List<String> columnsTypeChanged,
      @JsonProperty("columns_made_nullable") List<String> columnsMadeNullable,
      @JsonProperty("pipe_shutdown") boolean pipeShutdown) {
    this.columnsAdded = orEmpty(columnsAdded);
    this.columnsAlreadyPresent = orEmpty(columnsAlreadyPresent);
    this.columnsTypeChanged = orEmpty(columnsTypeChanged);
    this.columnsMadeNullable = orEmpty(columnsMadeNullable);
    this.pipeShutdown = pipeShutdown;
  }

  /** Columns added to the table by this call. */
  public List<String> getColumnsAdded() {
    return columnsAdded;
  }

  /** Schema fields whose column already existed with a compatible type. */
  public List<String> getColumnsAlreadyPresent() {
    return columnsAlreadyPresent;
  }

  /** Existing columns whose type this call widened. */
  public List<String> getColumnsTypeChanged() {
    return columnsTypeChanged;
  }

  /** Existing NOT NULL columns this call made nullable. */
  public List<String> getColumnsMadeNullable() {
    return columnsMadeNullable;
  }

  /** Whether GS restarted the pipe so ingestion picks up the new table schema. */
  public boolean isPipeShutdown() {
    return pipeShutdown;
  }

  private static List<String> orEmpty(List<String> list) {
    return list == null ? List.of() : List.copyOf(list);
  }

  @Override
  public String toString() {
    return "EvolveSchemaResponse{columnsAdded="
        + columnsAdded
        + ", columnsAlreadyPresent="
        + columnsAlreadyPresent
        + ", columnsTypeChanged="
        + columnsTypeChanged
        + ", columnsMadeNullable="
        + columnsMadeNullable
        + ", pipeShutdown="
        + pipeShutdown
        + "}";
  }
}
