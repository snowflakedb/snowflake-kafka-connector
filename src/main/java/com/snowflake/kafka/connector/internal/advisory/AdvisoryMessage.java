package com.snowflake.kafka.connector.internal.advisory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** One advisory message returned by SYSTEM$GET_KC_ADVISORY_MESSAGES. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AdvisoryMessage {

  @JsonProperty("level")
  String level;

  @JsonProperty("code")
  String code;

  @JsonProperty("text")
  String text;

  public AdvisoryMessage() {}

  public AdvisoryMessage(String level, String code, String text) {
    this.level = level;
    this.code = code;
    this.text = text;
  }

  public String getLevel() {
    return level;
  }

  public String getCode() {
    return code;
  }

  public String getText() {
    return text;
  }
}
