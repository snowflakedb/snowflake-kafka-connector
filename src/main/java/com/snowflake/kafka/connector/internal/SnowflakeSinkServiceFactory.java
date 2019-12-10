package com.snowflake.kafka.connector.internal;

import java.security.PrivateKey;
import java.util.Map;

/**
 * A factory to create {@link SnowflakeSinkService}
 */
public class SnowflakeSinkServiceFactory
{
  /**
   * create service builder
   * @param conn snowflake connection service
   * @return a builder instance
   */
  public static SnowflakeSinkServiceBuilder builder(
    SnowflakeConnectionService conn)
  {
    return new SnowflakeSinkServiceBuilder(conn);
  }

  /**
   * Builder class to create instance of {@link SnowflakeSinkService}
   */
  public static class SnowflakeSinkServiceBuilder extends Logging
  {
    private final SnowflakeSinkService service;

    private SnowflakeSinkServiceBuilder(SnowflakeConnectionService conn)
    {
      this.service = new SnowflakeSinkServiceV1(conn);
      logInfo("{} created", this.getClass().getName());
    }

    public SnowflakeSinkServiceBuilder addTask(String tableName,
                                               String topic,
                                               int partition)
    {
      this.service.startTask(tableName, topic, partition);
      logInfo("create new task in {} - table: {}, topic: {}, partition: {}",
        SnowflakeSinkService.class.getName(), tableName, topic, partition);
      return this;
    }

    public SnowflakeSinkServiceBuilder setRecordNumber(long num)
    {
      this.service.setRecordNumber(num);
      logInfo("record number is limited to {}", num);
      return this;
    }

    public SnowflakeSinkServiceBuilder setFileSize(long size)
    {
      this.service.setFileSize(size);
      logInfo("file size is limited to {}", size);
      return this;
    }

    public SnowflakeSinkServiceBuilder setFlushTime(long time)
    {
      this.service.setFlushTime(time);
      logInfo("flush time is limited to {}", time);
      return this;
    }
    public SnowflakeSinkServiceBuilder setTopic2TableMap(Map<String, String> topic2TableMap)
    {
      this.service.setTopic2TableMap(topic2TableMap);
      StringBuilder map = new StringBuilder();
      for (Map.Entry<String, String> entry: topic2TableMap.entrySet())
      {
        map.append(entry.getKey()).append(" -> ").append(entry.getValue()).append("\n");
      }
      logInfo("set topic 2 table map \n {}", map.toString());
      return this;
    }

    public SnowflakeSinkService build()
    {
      logInfo("{} created", SnowflakeSinkService.class.getName());
      return service;
    }

  }
}
