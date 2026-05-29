FROM mitmproxy/mitmproxy:11

# Copy the addon script
COPY addon_410.py /addon/addon_410.py

# Expose: 8080 = HTTP reverse proxy, 9080 = control API
EXPOSE 8080 9080

# Run in reverse proxy mode: accept plain HTTP on port 8080, forward to the
# real Snowflake host over HTTPS. No TLS on the frontend means no certificate
# issues with the Rust SDK's compiled-in CA roots.
#
# keep_host_header=false ensures the upstream Host header is set to UPSTREAM_HOST
# (not the Docker alias the SDK used to reach us).
ENTRYPOINT ["sh", "-c", "\
    exec mitmdump \
        --mode reverse:https://${UPSTREAM_HOST}/ \
        --listen-port 8080 \
        --set keep_host_header=false \
        --set upstream_cert=false \
        -s /addon/addon_410.py \
"]
