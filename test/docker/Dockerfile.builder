# Builder image for compiling protobuf dependencies
# Build artifacts are created during image build (cached) and can be copied out

FROM maven:3.9-eclipse-temurin-11

# Install protoc 3.21.x (compatible with protobuf-java 3.21.12)
ARG PROTOC_VERSION=21.12
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    unzip \
    git \
    python3 \
    && rm -rf /var/lib/apt/lists/*

# Download and install protoc
RUN ARCH=$(dpkg --print-architecture) && \
    if [ "$ARCH" = "arm64" ]; then PROTOC_ARCH="aarch_64"; else PROTOC_ARCH="x86_64"; fi && \
    curl -sLO "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${PROTOC_ARCH}.zip" && \
    unzip -q "protoc-${PROTOC_VERSION}-linux-${PROTOC_ARCH}.zip" -d /usr/local && \
    rm "protoc-${PROTOC_VERSION}-linux-${PROTOC_ARCH}.zip" && \
    chmod +x /usr/local/bin/protoc

WORKDIR /build

# Clone and build BlueApron protobuf converter
ARG CONVERTER_VERSION=3.1.0
RUN mkdir -p /output && \
    git clone -q https://github.com/blueapron/kafka-connect-protobuf-converter /build/converter && \
    cd /build/converter && \
    git checkout -q tags/v${CONVERTER_VERSION} && \
    mvn clean package -q -DskipTests && \
    cp target/kafka-connect-protobuf-converter-*-jar-with-dependencies.jar /output/

# Copy protobuf source and compile to Java
# Build context is test/ directory
COPY test_data/sensor.proto /build/test_data/
COPY test_data/protobuf/pom.xml /build/test_data/protobuf/
RUN mkdir -p /build/test_data/protobuf/src/main/java && \
    protoc --proto_path=/build/test_data --java_out=/build/test_data/protobuf/src/main/java sensor.proto

# Build protobuf test data JAR
RUN cd /build/test_data/protobuf && \
    mvn clean package -q -DskipTests && \
    mkdir -p /output && \
    cp target/kafka-test-protobuf-*-jar-with-dependencies.jar /output/

# Output directory contains the built JARs
# Copy them out with: docker cp $(docker create protobuf-builder):/output/. ./target/
WORKDIR /output
