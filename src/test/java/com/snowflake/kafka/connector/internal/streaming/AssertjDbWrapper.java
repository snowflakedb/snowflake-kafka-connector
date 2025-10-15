package com.snowflake.kafka.connector.internal.streaming;

import static org.assertj.db.api.Assertions.assertThat;

import org.assertj.db.api.TableAssert;
import org.assertj.db.type.Table;

/**
 * Wrapper class to avoid name clashing with jassert-core library. Jassert-core and jassert-db, both
 * have the same 'assertThat' construct. To use both of them, it would require to reference one of
 * them as FQN. So I'm aliasing the assertj-db in this wrapper
 */
final class AssertjDbWrapper {

  public static TableAssert dbAssertThat(Table table) {
    return assertThat(table);
  }
}
