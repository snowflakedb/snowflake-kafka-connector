package com.snowflake.kafka.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.List;
import org.junit.Test;

public class RegexTopicToTableResolverTest {

  private static RegexTopicToTableResolver resolverFrom(String input) {
    List<TopicToTableParser.Entry> entries = TopicToTableParser.parseAndValidate(input);
    return RegexTopicToTableResolver.from(entries);
  }

  @Test
  public void testExactMatchUnquotedIsUppercased() {
    TopicToTableResolver resolver = resolverFrom("my_topic:my_table");

    assertEquals("MY_TABLE", resolver.resolve("my_topic"));
  }

  @Test
  public void testExactMatchQuotedPreservesCase() {
    TopicToTableResolver resolver = resolverFrom("my_topic:\"My_Table\"");

    assertEquals("My_Table", resolver.resolve("my_topic"));
  }

  @Test
  public void testGroupSubstitutionUnquotedIsUppercased() {
    TopicToTableResolver resolver = resolverFrom("src_(.*):dest_$1");

    assertEquals("DEST_ORDERS", resolver.resolve("src_orders"));
    assertEquals("DEST_USERS", resolver.resolve("src_users"));
  }

  @Test
  public void testGroupSubstitutionQuotedPreservesCase() {
    TopicToTableResolver resolver = resolverFrom("src_(.*):\"Dest_$1\"");

    assertEquals("Dest_Orders", resolver.resolve("src_Orders"));
  }

  @Test
  public void testMultipleGroups() {
    TopicToTableResolver resolver = resolverFrom("(.*)\\.(.*):\"$1_$2\"");

    assertEquals("db_events", resolver.resolve("db.events"));
    assertEquals("app_logs", resolver.resolve("app.logs"));
  }

  @Test
  public void testNoMatchReturnsNull() {
    TopicToTableResolver resolver = resolverFrom("prefix_.*:table");

    assertNull(resolver.resolve("other_topic"));
  }

  @Test
  public void testMultiplePatterns() {
    TopicToTableResolver resolver = resolverFrom("alpha_(.*):a_$1, beta_(.*):b_$1");

    assertEquals("A_X", resolver.resolve("alpha_x"));
    assertEquals("B_Y", resolver.resolve("beta_y"));
    assertNull(resolver.resolve("gamma_z"));
  }

  @Test
  public void testTableNamesExcludesDynamicTemplates() {
    TopicToTableResolver resolver = resolverFrom("a:static_table, b_(.*):dynamic_$1");

    Collection<String> names = resolver.tableNames();
    assertEquals(1, names.size());
    assertTrue(names.contains("STATIC_TABLE"));
  }

  @Test
  public void testTableNamesQuotedStaticPreservesCase() {
    TopicToTableResolver resolver = resolverFrom("a:\"Mixed_Case\"");

    Collection<String> names = resolver.tableNames();
    assertEquals(1, names.size());
    assertTrue(names.contains("Mixed_Case"));
  }

  @Test
  public void testFullMatchRequired() {
    TopicToTableResolver resolver = resolverFrom("abc:table");

    assertNull(resolver.resolve("xabcx"));
    assertEquals("TABLE", resolver.resolve("abc"));
  }
}
