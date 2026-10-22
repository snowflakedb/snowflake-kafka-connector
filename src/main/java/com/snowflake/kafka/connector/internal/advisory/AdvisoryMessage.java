package com.snowflake.kafka.connector.internal.advisory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** One advisory message returned by SYSTEM$GET_KC_ADVISORY_MESSAGES. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class AdvisoryMessage {

  @JsonProperty("level")
  String level;

  @JsonProperty("text")
  String text;

  public AdvisoryMessage() {}

  public AdvisoryMessage(String level, String text) {
    this.level = level;
    this.text = text;
  }

  public String getLevel() {
    return level;
  }

  public String getText() {
    return text;
  }
}
