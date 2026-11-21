package com.snowflake.kafka.connector.internal.streaming;

import com.google.common.base.Strings;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.SnowflakeErrors;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Parses the streaming client override map and enforces that it only carries Snowpipe Streaming SDK
 * tuning parameters. Parameters that select the endpoint, transport, identity, credentials, or
 * request headers of the client are rejected, because the connector derives those from its own
 * connection settings.
 */
public final class StreamingClientOverrides {

  /**
   * SDK parameters that only tune buffering, memory throttling, timeouts, retries, and telemetry
   * cadence. Keys are lower case because the SDK matches parameter names case-sensitively in lower
   * case. A key must be honored by the SDK version the connector ships with and must not influence
   * where requests go or how they are authenticated.
   */
  static final Set<String> ALLOWED_KEYS =
      Set.of(
          // memory throttling
          "mem_throttle_threshold_percentage",
          "mem_throttle_threshold_mb_by_process",
          "skip_mem_throttle_if_available_mb",
          "mem_throttle_by_process",
          "mem_throttle_by_system",
          "mem_throttle_by_pod",
          "container_memory_accounting_mode",
          // HTTP timeouts and connection reuse
          "mgmt_http_timeout_seconds",
          "data_http_timeout_seconds",
          "http11_disable_connection_reuse",
          // retry policies
          "mgmt_retry_initial_backoff_seconds",
          "mgmt_retry_backoff_multiplier",
          "mgmt_retry_max_backoff_seconds",
          "mgmt_retry_total_backoff_seconds",
          "mgmt_retry_jitter_ratio",
          "data_retry_initial_backoff_seconds",
          "data_retry_backoff_multiplier",
          "data_retry_max_backoff_seconds",
          "data_retry_total_backoff_seconds",
          "data_retry_jitter_ratio",
          "open_channel_409_retry_initial_backoff_seconds",
          "open_channel_409_retry_backoff_multiplier",
          "open_channel_409_retry_max_backoff_seconds",
          "open_channel_409_retry_total_backoff_seconds",
          "open_channel_409_retry_jitter_ratio",
          "misclassified_404_retry_initial_backoff_seconds",
          "misclassified_404_retry_backoff_multiplier",
          "misclassified_404_retry_max_backoff_seconds",
          "misclassified_404_retry_total_backoff_seconds",
          "misclassified_404_retry_jitter_ratio",
          "non_scoped_auth_failure_retry_initial_backoff_seconds",
          "non_scoped_auth_failure_retry_backoff_multiplier",
          "non_scoped_auth_failure_retry_max_backoff_seconds",
          "non_scoped_auth_failure_retry_total_backoff_seconds",
          "non_scoped_auth_failure_retry_jitter_ratio",
          "treat_misclassified_404_as_500",
          // file upload
          "per_file_upload_timeout_seconds",
          "file_upload_initial_backoff_seconds",
          "file_upload_max_backoff_seconds",
          "file_upload_total_backoff_seconds",
          "file_upload_max_retry_count",
          "gcs_streaming_chunk_size_bytes",
          // buffering and request sizing
          "max_buffered_kb_for_rowset_mode",
          "target_rowset_payload_size_mb",
          "max_rowset_payload_size_mb",
          "target_buffer_file_size_mb",
          "ignore_switch_to_file_mode",
          "num_failure_before_forcing_file_mode",
          "force_file_mode_seconds",
          "max_append_request_size_mb",
          "max_outstanding_snowflake_requests",
          "max_inflight_rows_per_channel",
          "max_inflight_megabytes_per_channel",
          "max_append_request_buffer_duration_ms",
          "enable_rowset_compression",
          // refresh intervals
          "pipe_info_refresh_buffer_duration_seconds",
          "channel_status_cache_refresh_interval_seconds",
          // SDK telemetry
          "enable_telemetry_reporting",
          "telemetry_reporting_interval_seconds",
          "telemetry_flush_timeout_seconds");

  private StreamingClientOverrides() {}

  /**
   * Parses {@code key1:value1,key2:value2} into SDK parameter overrides with lower-cased keys.
   *
   * @throws com.snowflake.kafka.connector.internal.SnowflakeKafkaConnectorException if the format
   *     is invalid, a key is not an allowed tuning parameter, a key is repeated, or a value
   *     contains control characters
   */
  public static Map<String, String> parse(String overrideMap) {
    Map<String, String> overrides = new HashMap<>();
    if (Strings.isNullOrEmpty(overrideMap)) {
      return overrides;
    }
    // Pairs are split manually so that keys differing only in case are detected as duplicates.
    for (String pair : overrideMap.split(",")) {
      Map.Entry<String, String> entry =
          Utils.parseCommaSeparatedKeyValuePairs(pair).entrySet().iterator().next();
      String key = entry.getKey().toLowerCase(Locale.ROOT);
      String value = entry.getValue();
      if (!ALLOWED_KEYS.contains(key)) {
        throw SnowflakeErrors.ERROR_0030.getException(
            "Unsupported key '"
                + escapeControlCharacters(entry.getKey())
                + "'. Only Snowpipe Streaming SDK tuning parameters are accepted: "
                + new TreeSet<>(ALLOWED_KEYS));
      }
      if (containsControlCharacter(value)) {
        throw SnowflakeErrors.ERROR_0030.getException(
            "Value for key '" + key + "' contains control characters");
      }
      if (overrides.put(key, value) != null) {
        throw SnowflakeErrors.ERROR_0030.getException("Key '" + key + "' is set more than once");
      }
    }
    return overrides;
  }

  /**
   * Replaces every control character with its {@code U+XXXX} code point, so a rejected key cannot
   * break a log line or an error message.
   */
  static String escapeControlCharacters(String s) {
    StringBuilder sb = new StringBuilder(s.length());
    for (char c : s.toCharArray()) {
      if (isControlCharacter(c)) {
        sb.append(String.format("U+%04X", (int) c));
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  private static boolean containsControlCharacter(String s) {
    return s.chars().anyMatch(c -> isControlCharacter((char) c));
  }

  // Line and paragraph separators end a line in some log viewers even though they are not ISO
  // control characters.
  private static boolean isControlCharacter(char c) {
    return Character.isISOControl(c) || c == '\u2028' || c == '\u2029';
  }
}
