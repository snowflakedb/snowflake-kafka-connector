package com.snowflake.kafka.connector.internal.streaming.v2.evolveschema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.snowflake.ingest.streaming.EvolveSchemaResult;
import com.snowflake.ingest.streaming.SFException;
import com.snowflake.ingest.streaming.SchemaType;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

class PipeSchemaEvolverTest {

  private final SnowflakeStreamingIngestClient clientP = clientReturning();
  private final SnowflakeStreamingIngestClient clientQ = clientReturning();
  private final AtomicLong nanoTime = new AtomicLong(1_000);
  private final PipeSchemaEvolver evolver = new PipeSchemaEvolver(nanoTime::get);

  @Test
  void reportsWhetherTheTableWasEvolved() {
    Schema a = struct("a");

    assertThat(evolver.evolveIfNeeded(clientP, "P", "C", 0, a, a))
        .isEqualTo(PipeSchemaEvolver.Result.EVOLVED);
    assertThat(evolver.evolveIfNeeded(clientP, "P", "C", 1, a, a))
        .isEqualTo(PipeSchemaEvolver.Result.NOT_NEEDED);
  }

  @Test
  void notEnabledTurnsTheEvolverOffForEveryPipeUntilTheRetryInterval() {
    Schema a = struct("a");
    Schema b = struct("b");
    when(clientP.evolveSchema(any(), anyString(), anyBoolean()))
        .thenThrow(
            new SFException(
                "EvolveSchemaNotEnabled", "off", 403, "Forbidden", "ERR_EVOLVE_SCHEMA_NOT_ENABLED"))
        .thenReturn(mock(EvolveSchemaResult.class));

    assertThat(evolver.evolveIfNeeded(clientP, "P", "C", 0, a, a))
        .isEqualTo(PipeSchemaEvolver.Result.NOT_ENABLED);
    nanoTime.addAndGet(PipeSchemaEvolver.NOT_ENABLED_RETRY_INTERVAL.toNanos() - 1);
    assertThat(evolver.evolveIfNeeded(clientP, "P", "C", 1, b, b))
        .isEqualTo(PipeSchemaEvolver.Result.NOT_NEEDED);
    assertThat(evolver.evolveIfNeeded(clientQ, "Q", "C", 0, a, a))
        .isEqualTo(PipeSchemaEvolver.Result.NOT_NEEDED);
    verify(clientP, times(1)).evolveSchema(any(), anyString(), anyBoolean());
    verify(clientQ, never()).evolveSchema(any(), anyString(), anyBoolean());

    nanoTime.addAndGet(1);
    assertThat(evolver.evolveIfNeeded(clientP, "P", "C", 2, a, a))
        .isEqualTo(PipeSchemaEvolver.Result.EVOLVED);
  }

  @Test
  void otherSdkErrorPropagatesAndKeepsTheEvolverOn() {
    Schema a = struct("a");
    when(clientP.evolveSchema(any(), anyString(), anyBoolean()))
        .thenThrow(new SFException("SfApiAuthError", "forbidden", 401, "Unauthorized"))
        .thenReturn(mock(EvolveSchemaResult.class));

    assertThatExceptionOfType(SFException.class)
        .isThrownBy(() -> evolver.evolveIfNeeded(clientP, "P", "C", 0, a, a));
    assertThat(evolver.evolveIfNeeded(clientP, "P", "C", 1, a, a))
        .isEqualTo(PipeSchemaEvolver.Result.EVOLVED);
  }

  @Test
  void schemaRejectedForItselfIsNotSentAgain() {
    Schema a = struct("a");
    SFException invalid =
        new SFException(
            "SfApiUserError", "declined", 400, "Bad Request", "ERR_EVOLVE_SCHEMA_INVALID_SCHEMA");
    when(clientP.evolveSchema(any(), anyString(), anyBoolean())).thenThrow(invalid);

    assertThatExceptionOfType(SFException.class)
        .isThrownBy(() -> evolver.evolveIfNeeded(clientP, "P", "C", 0, a, a));
    assertThatExceptionOfType(SFException.class)
        .isThrownBy(() -> evolver.evolveIfNeeded(clientP, "P", "C", 1, a, a))
        .isSameAs(invalid);

    verify(clientP, times(1)).evolveSchema(any(), anyString(), anyBoolean());
  }

  @Test
  void tableLevelRejectionIsSentAgain() {
    Schema a = struct("a");
    when(clientP.evolveSchema(any(), anyString(), anyBoolean()))
        .thenThrow(
            new SFException(
                "SfApiUserError",
                "declined",
                400,
                "Bad Request",
                "ERR_EVOLVE_SCHEMA_MISSING_PRIVILEGE"))
        .thenReturn(mock(EvolveSchemaResult.class));

    assertThatExceptionOfType(SFException.class)
        .isThrownBy(() -> evolver.evolveIfNeeded(clientP, "P", "C", 0, a, a));
    // After the grant, the next record of the schema evolves the table.
    assertThat(evolver.evolveIfNeeded(clientP, "P", "C", 1, a, a))
        .isEqualTo(PipeSchemaEvolver.Result.EVOLVED);
  }

  @Test
  void forgetPipeSendsARejectedSchemaAgain() {
    Schema a = struct("a");
    when(clientP.evolveSchema(any(), anyString(), anyBoolean()))
        .thenThrow(
            new SFException(
                "SfApiUserError",
                "declined",
                400,
                "Bad Request",
                "ERR_CHANNEL_SCHEMA_EVOLUTION_TOO_MANY_COLUNS"))
        .thenReturn(mock(EvolveSchemaResult.class));
    assertThatExceptionOfType(SFException.class)
        .isThrownBy(() -> evolver.evolveIfNeeded(clientP, "P", "C", 0, a, a));

    evolver.forgetPipe("P");

    assertThat(evolver.evolveIfNeeded(clientP, "P", "C", 1, a, a))
        .isEqualTo(PipeSchemaEvolver.Result.EVOLVED);
  }

  @Test
  void skipsNullAndNonStructSchemas() {
    evolver.evolveIfNeeded(clientP, "P", "C", 0, null, null);
    evolver.evolveIfNeeded(clientP, "P", "C", 0, Schema.STRING_SCHEMA, Schema.STRING_SCHEMA);

    verify(clientP, never()).evolveSchema(any(), anyString(), anyBoolean());
  }

  @Test
  void sendsColumnSchemaAsKafkaConnectWithoutNormalizingNames() {
    Schema value = struct("id");
    Schema column = struct("ID");

    evolver.evolveIfNeeded(clientP, "P", "C", 42, value, column);

    verify(clientP)
        .evolveSchema(SchemaType.KAFKA_CONNECT, ConnectSchemaJson.toJson(column).toString(), false);
  }

  @Test
  void callsOncePerSchemaPerPipe() {
    Schema a = struct("a");
    Schema b = struct("b");

    evolver.evolveIfNeeded(clientP, "P", "C0", 0, a, a);
    evolver.evolveIfNeeded(clientP, "P", "C0", 1, a, a);
    evolver.evolveIfNeeded(clientP, "P", "C0", 2, struct("a"), a);
    evolver.evolveIfNeeded(clientP, "P", "C1", 0, a, a);
    evolver.evolveIfNeeded(clientP, "P", "C0", 3, b, b);
    evolver.evolveIfNeeded(clientP, "P", "C0", 4, a, a);
    evolver.evolveIfNeeded(clientQ, "Q", "C2", 0, a, a);

    verify(clientP, times(2)).evolveSchema(any(), anyString(), anyBoolean());
    verify(clientQ, times(1)).evolveSchema(any(), anyString(), anyBoolean());
  }

  @Test
  void failedCallIsRetriedOnNextRecord() {
    Schema a = struct("a");
    when(clientP.evolveSchema(any(), anyString(), anyBoolean()))
        .thenThrow(new SFException("SfApiUserError", "declined", 400, "Bad Request"))
        .thenReturn(mock(EvolveSchemaResult.class));

    assertThatExceptionOfType(SFException.class)
        .isThrownBy(() -> evolver.evolveIfNeeded(clientP, "P", "C", 0, a, a));
    evolver.evolveIfNeeded(clientP, "P", "C", 0, a, a);

    verify(clientP, times(2)).evolveSchema(any(), anyString(), anyBoolean());
  }

  @Test
  void forgetPipeEvolvesItsSchemasAgain() {
    Schema a = struct("a");
    evolver.evolveIfNeeded(clientP, "P", "C", 0, a, a);
    evolver.evolveIfNeeded(clientQ, "Q", "C", 0, a, a);

    evolver.forgetPipe("P");
    evolver.evolveIfNeeded(clientP, "P", "C", 1, a, a);
    evolver.evolveIfNeeded(clientQ, "Q", "C", 1, a, a);

    verify(clientP, times(2)).evolveSchema(any(), anyString(), anyBoolean());
    verify(clientQ, times(1)).evolveSchema(any(), anyString(), anyBoolean());
  }

  @Test
  void forgetsSchemasWhenCapIsReached() {
    Schema first = struct("f0");
    evolver.evolveIfNeeded(clientP, "P", "C", 0, first, first);
    for (int i = 1; i <= PipeSchemaEvolver.MAX_SCHEMAS_PER_PIPE; i++) {
      Schema s = struct("f" + i);
      evolver.evolveIfNeeded(clientP, "P", "C", i, s, s);
    }
    evolver.evolveIfNeeded(clientP, "P", "C", 0, first, first);

    verify(clientP, times(PipeSchemaEvolver.MAX_SCHEMAS_PER_PIPE + 2))
        .evolveSchema(any(), anyString(), anyBoolean());
  }

  private static SnowflakeStreamingIngestClient clientReturning() {
    SnowflakeStreamingIngestClient client = mock(SnowflakeStreamingIngestClient.class);
    when(client.evolveSchema(any(), anyString(), anyBoolean()))
        .thenReturn(mock(EvolveSchemaResult.class));
    return client;
  }

  private static Schema struct(String field) {
    return SchemaBuilder.struct().field(field, Schema.INT64_SCHEMA).build();
  }
}
