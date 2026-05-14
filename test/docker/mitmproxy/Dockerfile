FROM mitmproxy/mitmproxy:11

# Copy the addon script
COPY addon_410.py /addon/addon_410.py

# Expose: 8080 = HTTP proxy, 9080 = control API
EXPOSE 8080 9080

# Generate CA certs at runtime (not build time) to ensure the cert shared
# via /certs/ volume matches what the proxy actually uses.
# Remove stale cert first so the healthcheck doesn't pass prematurely.
ENTRYPOINT ["sh", "-c", "\
    rm -f /certs/mitmproxy-ca-cert.pem && \
    mitmdump --mode regular --listen-port 0 & sleep 2 && kill $! 2>/dev/null; \
    cp /root/.mitmproxy/mitmproxy-ca-cert.pem /certs/mitmproxy-ca-cert.pem && \
    exec mitmdump \
        --mode regular \
        --listen-port 8080 \
        --set upstream_cert=false \
        -s /addon/addon_410.py \
"]
