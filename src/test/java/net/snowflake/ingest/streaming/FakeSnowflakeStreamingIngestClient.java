package net.snowflake.ingest.streaming;

import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Fake implementation of {@link SnowflakeStreamingIngestClient}. Uses in memory state only.
 * Cooperates with {@link FakeSnowflakeStreamingIngestChannel} for simulating ingest-sdk. Should
 * provide a drop in replacement for most testing scenarios without a need to be connected to any
 * Snowflake deployment. The implementation thread safety relies on {@link ConcurrentHashMap}
 */
public class FakeSnowflakeStreamingIngestClient implements SnowflakeStreamingIngestClient {

  private final String name;
  private boolean closed;
  private static final KCLogger LOGGER =
      new KCLogger(FakeSnowflakeStreamingIngestClient.class.getName());
  private final ConcurrentHashMap<String, SnowflakeStreamingIngestChannel> channelCache =
      new ConcurrentHashMap<>();

  public FakeSnowflakeStreamingIngestClient(String name) {
    this.name = name;
  }

  @Override
  public SnowflakeStreamingIngestChannel openChannel(OpenChannelRequest request) {
    String fqdn =
        String.format("%s.%s", request.getFullyQualifiedTableName(), request.getChannelName());
    return channelCache.computeIfAbsent(
        fqdn,
        (key) ->
            new FakeSnowflakeStreamingIngestChannel(
                this, name, request.getDBName(), request.getSchemaName(), request.getTableName()));
  }

  @Override
  public void dropChannel(DropChannelRequest request) {
    String fqdn =
        String.format("%s.%s", request.getFullyQualifiedTableName(), request.getChannelName());
    SnowflakeStreamingIngestChannel result = channelCache.remove(fqdn);
    if (result == null) {
      LOGGER.warn("Dropping non-existing channel {}", fqdn);
    }
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void setRefreshToken(String refreshToken) {}

  @Override
  public boolean isClosed() {
    return closed;
  }

  @Override
  public Map<String, String> getLatestCommittedOffsetTokens(
      List<SnowflakeStreamingIngestChannel> channels) {
    return channels.stream()
        .collect(
            Collectors.toMap(
                SnowflakeStreamingIngestChannel::getFullyQualifiedName,
                (c) ->
                    channelCache.get(c.getFullyQualifiedName()).getLatestCommittedOffsetToken()));
  }

  @Override
  public void close() throws Exception {
    closed = true;
  }
}
