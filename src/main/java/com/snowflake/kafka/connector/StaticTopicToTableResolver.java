package com.snowflake.kafka.connector;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Resolves topic names to table names using exact match first, then regex matching against the keys
 * in declaration order. The mapped table name is returned as-is (no group substitution).
 */
public class StaticTopicToTableResolver implements TopicToTableResolver {
  private final Map<String, String> topicToTable;

  public StaticTopicToTableResolver(Map<String, String> topicToTable) {
    this.topicToTable = new LinkedHashMap<>(topicToTable);
  }

  /** Build a static resolver (legacy behavior, no group substitution) from parsed entries. */
  public static StaticTopicToTableResolver from(List<TopicToTableParser.Entry> entries) {
    Map<String, String> topicToTable =
        entries.stream()
            .collect(
                LinkedHashMap::new,
                (map, entry) -> {
                  // Uppercase unquoted table names at build time (legacy); preserve quoted ones.
                  String table =
                      entry.shouldUppercase()
                          ? entry.getTable().toUpperCase(Locale.ROOT)
                          : entry.getTable();
                  if (map.put(entry.getTopic(), table) != null) {
                    throw new IllegalStateException("Duplicate topic: " + entry.getTopic());
                  }
                },
                Map::putAll);
    return new StaticTopicToTableResolver(topicToTable);
  }

  @Override
  @Nullable
  public String resolve(String topic) {
    if (topicToTable.containsKey(topic)) {
      return topicToTable.get(topic);
    }

    for (Map.Entry<String, String> entry : topicToTable.entrySet()) {
      if (topic.matches(entry.getKey())) {
        return entry.getValue();
      }
    }

    return null;
  }

  @Override
  public Collection<String> tableNames() {
    return Collections.unmodifiableCollection(topicToTable.values());
  }
}
