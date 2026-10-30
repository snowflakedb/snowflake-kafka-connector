package com.snowflake.kafka.connector.internal.streaming.v2.evolveschema;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

class PipeSchemaEvolverTest {

  private final FakeClient client = new FakeClient();
  private final PipeSchemaEvolver evolver = new PipeSchemaEvolver(client, "DB", "SC");

  @Test
  void skipsNullAndNonStructSchemas() {
    evolver.evolveIfNeeded("P", "C", 0, null, null);
    evolver.evolveIfNeeded("P", "C", 0, Schema.STRING_SCHEMA, Schema.STRING_SCHEMA);

    assertThat(client.calls).isEmpty();
  }

  @Test
  void sendsColumnSchemaWithChannelAndOffset() {
    Schema value = struct("id");
    Schema column = struct("ID");

    evolver.evolveIfNeeded("P", "C", 42, value, column);

    assertThat(client.calls).hasSize(1);
    Call call = client.calls.get(0);
    assertThat(call.database).isEqualTo("DB");
    assertThat(call.schema).isEqualTo("SC");
    assertThat(call.pipe).isEqualTo("P");
    assertThat(call.request.getSchemaType())
        .isEqualTo(EvolveSchemaRequest.SCHEMA_TYPE_KAFKA_CONNECT);
    assertThat(call.request.getSchema()).isEqualTo(ConnectSchemaJson.toJson(column));
    assertThat(call.request.getChannelName()).isEqualTo("C");
    assertThat(call.request.getOffsetToken()).isEqualTo("42");
    assertThat(call.request.getNormalizeColumnNames()).isFalse();
  }

  @Test
  void callsOncePerSchemaPerPipe() {
    Schema a = struct("a");
    Schema b = struct("b");

    evolver.evolveIfNeeded("P", "C0", 0, a, a);
    evolver.evolveIfNeeded("P", "C0", 1, a, a);
    evolver.evolveIfNeeded("P", "C0", 2, struct("a"), a);
    evolver.evolveIfNeeded("P", "C1", 0, a, a);
    evolver.evolveIfNeeded("P", "C0", 3, b, b);
    evolver.evolveIfNeeded("P", "C0", 4, a, a);
    evolver.evolveIfNeeded("Q", "C2", 0, a, a);

    assertThat(client.calls).extracting(c -> c.pipe).containsExactly("P", "P", "Q");
  }

  @Test
  void failedCallIsRetriedOnNextRecord() {
    Schema a = struct("a");
    client.failNext = true;

    assertThatExceptionOfType(EvolveSchemaException.class)
        .isThrownBy(() -> evolver.evolveIfNeeded("P", "C", 0, a, a));
    evolver.evolveIfNeeded("P", "C", 0, a, a);

    assertThat(client.calls).hasSize(2);
  }

  @Test
  void forgetsSchemasWhenCapIsReached() {
    Schema first = struct("f0");
    evolver.evolveIfNeeded("P", "C", 0, first, first);
    for (int i = 1; i <= PipeSchemaEvolver.MAX_SCHEMAS_PER_PIPE; i++) {
      Schema s = struct("f" + i);
      evolver.evolveIfNeeded("P", "C", i, s, s);
    }
    evolver.evolveIfNeeded("P", "C", 0, first, first);

    assertThat(client.calls).hasSize(PipeSchemaEvolver.MAX_SCHEMAS_PER_PIPE + 2);
  }

  private static Schema struct(String field) {
    return SchemaBuilder.struct().field(field, Schema.INT64_SCHEMA).build();
  }

  private static final class Call {
    final String database;
    final String schema;
    final String pipe;
    final EvolveSchemaRequest request;

    Call(String database, String schema, String pipe, EvolveSchemaRequest request) {
      this.database = database;
      this.schema = schema;
      this.pipe = pipe;
      this.request = request;
    }
  }

  private static final class FakeClient implements EvolveSchemaClient {
    final List<Call> calls = new ArrayList<>();
    boolean failNext;

    @Override
    public EvolveSchemaResponse evolveSchema(
        String database, String schema, String pipe, EvolveSchemaRequest request) {
      calls.add(new Call(database, schema, pipe, request));
      if (failNext) {
        failNext = false;
        throw new EvolveSchemaException(503, null, "unavailable");
      }
      return new EvolveSchemaResponse(null, null, null, null, false);
    }
  }
}
