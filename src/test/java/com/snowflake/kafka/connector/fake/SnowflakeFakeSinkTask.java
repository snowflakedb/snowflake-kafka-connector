package com.snowflake.kafka.connector.fake;

import com.snowflake.kafka.connector.SnowflakeSinkConnector;
import com.snowflake.kafka.connector.Utils;
import com.snowflake.kafka.connector.internal.KCLogger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

public class SnowflakeFakeSinkTask extends SinkTask {

  private static final KCLogger LOGGER = new KCLogger(SnowflakeSinkConnector.class.getName());

  private static final List<SinkRecord> records = Collections.synchronizedList(new ArrayList<>());

  public static List<SinkRecord> getRecords() {
    return Arrays.asList(records.toArray(new SinkRecord[0]));
  }

  public static void resetRecords() {
    records.clear();
  }

  @Override
  public String version() {
    return Utils.VERSION;
  }

  @Override
  public void start(Map<String, String> map) {
    LOGGER.debug("Starting " + SnowflakeFakeSinkTask.class.getSimpleName());
    resetRecords();
  }

  @Override
  public void put(Collection<SinkRecord> collection) {
    records.addAll(collection);
  }

  @Override
  public void stop() {
    LOGGER.debug("Stopping " + SnowflakeFakeSinkTask.class.getSimpleName());
  }
}
