# Base image for the KC SPCS release test. Deliberately contains only a JRE:
# Kafka, the connector JAR and the harness script come from the stage volume, so
# the image is built and pushed once at provisioning time, not per release.
FROM eclipse-temurin:17-jre
RUN apt-get update && apt-get install -y --no-install-recommends procps \
    && rm -rf /var/lib/apt/lists/*
