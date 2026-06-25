package com.snowflake.kafka.connector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;

/**
 * Resolves topic names to table names using compiled regex patterns with group substitution.
 * Patterns are compiled once at construction time and matched in declaration order. The replacement
 * template may contain group references ($1, $2, etc.). Unquoted table templates are uppercased
 * after substitution; quoted templates preserve the case of both the template and the substituted
 * groups.
 */
public class RegexTopicToTableResolver implements TopicToTableResolver {

  private final List<ResolverEntry> entries;

  private RegexTopicToTableResolver(List<TopicToTableParser.Entry> parsedEntries) {
    List<ResolverEntry> built = new ArrayList<>(parsedEntries.size());
    for (TopicToTableParser.Entry entry : parsedEntries) {
      built.add(
          new ResolverEntry(
              Pattern.compile(entry.getTopic()), entry.getTable(), entry.shouldUppercase()));
    }
    this.entries = Collections.unmodifiableList(built);
  }

  /** Build a regex resolver with group substitution support from parsed entries. */
  public static RegexTopicToTableResolver from(List<TopicToTableParser.Entry> entries) {
    return new RegexTopicToTableResolver(entries);
  }

  @Override
  @Nullable
  public String resolve(String topic) {
    for (ResolverEntry entry : entries) {
      Matcher matcher = entry.pattern.matcher(topic);
      if (matcher.matches()) {
        String result = matcher.replaceFirst(entry.template);
        if (entry.shouldUppercase) {
          result = result.toUpperCase(Locale.ROOT);
        }
        return result;
      }
    }
    return null;
  }

  /**
   * Returns table names that can be statically determined. Templates containing group references
   * ($0-$9) are excluded since their resolved names depend on the matched topic.
   */
  @Override
  public Collection<String> tableNames() {
    List<String> names = new ArrayList<>();
    for (ResolverEntry entry : entries) {
      if (!containsGroupReference(entry.template)) {
        String table =
            entry.shouldUppercase ? entry.template.toUpperCase(Locale.ROOT) : entry.template;
        names.add(table);
      }
    }
    return Collections.unmodifiableList(names);
  }

  private static boolean containsGroupReference(String template) {
    for (int i = 0; i < template.length() - 1; i++) {
      if (template.charAt(i) == '$' && Character.isDigit(template.charAt(i + 1))) {
        return true;
      }
    }
    return false;
  }

  private static final class ResolverEntry {
    final Pattern pattern;
    final String template;
    final boolean shouldUppercase;

    ResolverEntry(Pattern pattern, String template, boolean shouldUppercase) {
      this.pattern = pattern;
      this.template = template;
      this.shouldUppercase = shouldUppercase;
    }
  }
}
