FROM mitmproxy/mitmproxy:11

# Copy the addon script and the test-only leaf cert (SAN: mitmproxy,
# mitmproxy-subdomain). The Connect worker trusts mitmproxy-ca.crt via SSL_CERT_FILE.
COPY addon.py /addon/addon.py
COPY certs/mitmproxy.pem /certs/mitmproxy.pem

# Expose: 8080 = HTTPS reverse proxy, 9080 = control API (plain HTTP)
EXPOSE 8080 9080

# Reverse proxy: TLS on the listen port, HTTPS to the real Snowflake host.
# mitmproxy autodetects client TLS; --certs supplies the leaf for both Docker
# aliases. keep_host_header=false sets the upstream Host to UPSTREAM_HOST.
ENTRYPOINT ["sh", "-c", "\
    exec mitmdump \
        --mode reverse:https://${UPSTREAM_HOST}/ \
        --listen-port 8080 \
        --certs /certs/mitmproxy.pem \
        --set keep_host_header=false \
        --set upstream_cert=false \
        -s /addon/addon.py \
"]
