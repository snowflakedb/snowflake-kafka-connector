package com.snowflake.kafka.connector.internal.advisory;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;

/** Deserialized response from SYSTEM$GET_KC_ADVISORY_MESSAGES. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class KcAdvisoryResponse {

  @JsonProperty("messages")
  List<AdvisoryMessage> messages = new ArrayList<>();

  public List<AdvisoryMessage> getMessages() {
    return messages == null ? new ArrayList<>() : messages;
  }
}
