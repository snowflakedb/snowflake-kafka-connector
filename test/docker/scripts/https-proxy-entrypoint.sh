#!/bin/bash
# Wraps start-apache-kafka.sh for the forward-proxy E2E overlay.
#
# The worker sits on an internal-only network, so every Snowflake call must go
# through the https-proxy container. This script makes the worker trust the
# mitmproxy CA (JVM trust store for JDBC, SSL_CERT_FILE for the Rust SDK) and
# points the JVM proxy system properties at the worker-tier listener.

set -e

CA_PEM=/proxy-certs/mitmproxy-ca-cert.pem
TRUSTSTORE=/tmp/https-proxy-truststore.p12
TRUSTSTORE_PASSWORD=changeit

for _ in $(seq 1 60); do
    [ -s "$CA_PEM" ] && break
    sleep 1
done
[ -s "$CA_PEM" ] || { echo "mitmproxy CA not found at $CA_PEM" >&2; exit 1; }

cp "$JAVA_HOME/lib/security/cacerts" "$TRUSTSTORE"
chmod u+w "$TRUSTSTORE"
keytool -importcert -noprompt -alias https-proxy-e2e -file "$CA_PEM" \
    -keystore "$TRUSTSTORE" -storepass "$TRUSTSTORE_PASSWORD"

export SSL_CERT_FILE="$CA_PEM"
export KAFKA_OPTS="${KAFKA_OPTS:-} \
-Djavax.net.ssl.trustStore=$TRUSTSTORE \
-Djavax.net.ssl.trustStorePassword=$TRUSTSTORE_PASSWORD \
-Dhttp.useProxy=true \
-Dhttps.proxyHost=$JVM_PROXY_HOST -Dhttps.proxyPort=$JVM_PROXY_PORT \
-Dhttp.proxyHost=$JVM_PROXY_HOST -Dhttp.proxyPort=$JVM_PROXY_PORT"

echo "https-proxy overlay: KAFKA_OPTS=$KAFKA_OPTS"
exec /bin/bash /opt/start.sh
