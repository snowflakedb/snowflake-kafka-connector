package com.snowflake.kafka.connector.fake;

import com.snowflake.kafka.connector.SnowflakeSinkConnector;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

public class SnowflakeFakeSinkConnector extends SinkConnector {

  private static final KCLogger LOGGER = new KCLogger(SnowflakeSinkConnector.class.getName());
  private Map<String, String> config; // connector configuration, provided by
  // user through kafka connect framework
  @Override
  public void start(Map<String, String> parsedConfig) {
    LOGGER.debug("Starting " + SnowflakeFakeSinkConnector.class.getSimpleName());
    config = new HashMap<>(parsedConfig);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return SnowflakeFakeSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>();
    Map<String, String> conf = new HashMap<>(config);
    conf.put(Utils.TASK_ID, "fakeTask1");
    taskConfigs.add(conf);
    return taskConfigs;
  }

  @Override
  public void stop() {
    LOGGER.debug("Stopping " + SnowflakeFakeSinkConnector.class.getSimpleName());
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public String version() {
    return Utils.VERSION;
  }
}
