package com.snowflake.kafka.connector.internal;

import com.fasterxml.jackson.databind.annotation.JsonAppend;
import com.snowflake.kafka.connector.Utils;

import java.util.Map;
import java.util.Properties;

public class SnowflakeConnectionServiceFactory
{
  public static SnowflakeConnectionServiceBuilder builder()
  {
    return new SnowflakeConnectionServiceBuilder();
  }
  public static class SnowflakeConnectionServiceBuilder extends Logging
  {
    private Properties prop;
    private Properties proxyProperties;
    private SnowflakeURL url;
    private String connectorName;
    private String taskID = "-1";

    // For testing only
    public SnowflakeConnectionServiceBuilder setProperties(Properties prop)
    {
      this.prop = prop;
      return this;
    }

    // For testing only
    public Properties getProperties()
    {
      return this.prop;
    }

    public SnowflakeConnectionServiceBuilder setURL(SnowflakeURL url)
    {
      this.url = url;
      return this;
    }

    public SnowflakeConnectionServiceBuilder setConnectorName(String name)
    {
      this.connectorName = name;
      return this;
    }

    public SnowflakeConnectionServiceBuilder setTaskID(String taskID)
    {
      this.taskID = taskID;
      return this;
    }

    public SnowflakeConnectionServiceBuilder setProperties(Map<String, String> conf)
    {
      if(!conf.containsKey(Utils.SF_URL))
      {
        throw SnowflakeErrors.ERROR_0017.getException();
      }
      this.url = new SnowflakeURL(conf.get(Utils.SF_URL));
      this.prop = InternalUtils.createProperties(conf, this.url.sslEnabled());
      // TODO: Ideally only one property is required, but because we dont pass it around in JDBC and snowpipe SDK,
      //  it is better if we have two properties decoupled
      // Right now, proxy parameters are picked from jvm system properties, in future they need to be decoupled
      this.proxyProperties = InternalUtils.generateProxyParametersIfRequired(conf);
      this.connectorName = conf.get(Utils.NAME);
      return this;
    }

    public SnowflakeConnectionService build()
    {
      InternalUtils.assertNotEmpty("properties", prop);
      InternalUtils.assertNotEmpty("url", url);
      InternalUtils.assertNotEmpty("connectorName", connectorName);
      return new SnowflakeConnectionServiceV1(prop, url, connectorName, taskID, proxyProperties);
    }
  }
}
