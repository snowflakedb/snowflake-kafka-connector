# Build from the test/ directory. Reuse the existing Apache image/configuration.
# Runtime code and the exact KC v4 JAR are supplied through a run-scoped stage.
ARG KAFKA_IMAGE
FROM ${KAFKA_IMAGE} AS kafka
FROM python:3.13-slim-bookworm
COPY --from=kafka /opt/java/openjdk /opt/java/openjdk
COPY --from=kafka /opt/kafka /opt/kafka
ENV JAVA_HOME=/opt/java/openjdk
ENV PATH="/opt/java/openjdk/bin:${PATH}"
RUN apt-get update && apt-get install -y --no-install-recommends curl coreutils \
    && rm -rf /var/lib/apt/lists/*
# Same core Python dependencies as docker/Dockerfile.test-runner.
RUN pip install --no-cache-dir requests certifi \
    'confluent-kafka[avro,json,protobuf]==2.13.0' avro-python3 kafka-python-ng \
    'snowflake-connector-python==4.2.0' 'pytest==8.3.5'
COPY apache_properties/kraft-server.properties /opt/kafka/config/kraft-server.properties
COPY apache_properties/connect-distributed.properties /opt/kafka/config/connect-distributed.properties
COPY connect-log4j.properties /opt/kafka/config/connect-log4j.properties
WORKDIR /work
