package com.snowflake.kafka.connector.testcontainers;

import com.fasterxml.jackson.annotation.JsonProperty;

class KafkaConnectPlugin {

    @JsonProperty("class")
    private String clazz;
    private String type;
    private String version;

    public String getClazz() {
        return clazz;
    }

    public void setClazz(final String clazz) {
        this.clazz = clazz;
    }

    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(final String version) {
        this.version = version;
    }
}
