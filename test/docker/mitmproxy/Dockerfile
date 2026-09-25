FROM mitmproxy/mitmproxy:11

# Test CA + leaf are generated at container start into /certs (compose volume).
# The Connect worker mounts the same volume and trusts the CA via SSL_CERT_FILE.
COPY addon_410.py /addon/addon_410.py
COPY generate_certs.py /addon/generate_certs.py

# Expose: 8080 = HTTPS reverse proxy, 9080 = control API (plain HTTP)
EXPOSE 8080 9080

# Reverse proxy: TLS on the listen port, HTTPS to the real Snowflake host.
# generate_certs.py writes a leaf with SAN mitmproxy + mitmproxy-subdomain.
# keep_host_header=false sets the upstream Host to UPSTREAM_HOST.
ENTRYPOINT ["sh", "-c", "\
    python3 /addon/generate_certs.py && \
    exec mitmdump \
        --mode reverse:https://${UPSTREAM_HOST}/ \
        --listen-port 8080 \
        --certs /certs/mitmproxy.pem \
        --set keep_host_header=false \
        --set upstream_cert=false \
        -s /addon/addon_410.py \
"]
