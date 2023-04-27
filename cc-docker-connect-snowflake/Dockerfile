ARG DOCKER_REGISTRY
ARG BASE_VERSION

FROM ${DOCKER_REGISTRY}confluentinc/cc-built-base:${BASE_VERSION}

COPY \
    kafka-connect-snowflake/target/components/packages/snowflakeinc-snowflake-kafka-connector-ccloud*/snowflakeinc-snowflake-kafka-connector-ccloud*/lib \
    /usr/share/java/connectors/plugins/kafka-connect-snowflake-sink
