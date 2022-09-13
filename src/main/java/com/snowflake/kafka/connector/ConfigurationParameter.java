package com.snowflake.kafka.connector;

  import com.snowflake.kafka.connector.internal.Logging;
  import org.slf4j.Logger;
  import org.slf4j.LoggerFactory;

public final class ConfigurationParameter {
  public class SnowpipeConstants {

    // empty constructor
    private SnowpipeConstants() {}
  }

  public class StreamingConstants {

    // empty constructor
    private StreamingConstants() {}
  }

  public class BufferConstants {

    // empty constructor
    private BufferConstants() {}
  }

  private static final String TO_STRING_FORMAT = "Configuration parameter '{}' with value '{}";
  private static final String EXCEPTION_TO_STRING_NO_ERRORMSG = "Invalid configuration parameter'{}' with value '{}'";
  private static final String EXCEPTION_TO_STRING_WITH_ERRORMSG = "Invalid configuration parameter'{}' with value '{}. Error: {}'";
  private static final Logger LOGGER = LoggerFactory.getLogger(Utils.class.getName());

  private final String paramName;
  private final String paramValue;
  private final String errorMsg;
  public boolean isValid;

  public ConfigurationParameter(String paramName, String paramValue, String errorMsg)
  {
    this.paramName = paramName;
    this.paramValue = paramValue;
    this.errorMsg = errorMsg;
    this.isValid = true;

    // we want to log parameter creation
    LOGGER.error(this.toString());
  }

  public String getParamName() {
    return this.getStringOrEmpty(this.paramName);
  }

  public String getParamValue() {
    return this.getStringOrEmpty(this.paramValue);
  }

  public String getErrorMsg() {
    return this.getStringOrEmpty(this.errorMsg);
  }

  public String getExceptionString() {
    if (isValid) {
      return this.toString() + " is valid";
    }

    String parsedErrorMessage = this.getErrorMsg();
    if (parsedErrorMessage.equalsIgnoreCase(""))
      return Logging.logMessage(this.EXCEPTION_TO_STRING_NO_ERRORMSG,
        this.getParamName(),
        this.getParamValue());

    return Logging.logMessage(this.EXCEPTION_TO_STRING_WITH_ERRORMSG,
      this.getParamName(),
      this.getParamValue(),
      this.getErrorMsg());
  }

  @Override
  public String toString() {
    return Logging.logMessage(TO_STRING_FORMAT, this.getParamName(), this.getParamValue());
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ConfigurationParameter))
      return false;

    ConfigurationParameter invalidConfigParam = (ConfigurationParameter) obj;

    return this.paramName.equals(invalidConfigParam.paramName)
      && this.paramValue.equals(invalidConfigParam.paramValue)
      && this.errorMsg.equals(invalidConfigParam.errorMsg);
  }

  private String getStringOrEmpty(String input) {
    return input == null || input.isEmpty() ? "" : input;
  }
}
