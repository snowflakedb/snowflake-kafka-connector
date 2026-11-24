FROM mitmproxy/mitmproxy:11

# Forward-proxy listeners (8080 connector tier, 8081 worker JVM tier, 8082 a
# second connector's proxy) and a control API on 9080. 8081 requires Basic
# credentials when JVM_TIER_PROXY_USER and JVM_TIER_PROXY_PASSWORD are set.
# mitmproxy generates its CA into confdir on first start; the Connect worker
# reads it from the shared volume to build its trust stores.
COPY proxy_counter.py /addon/proxy_counter.py

EXPOSE 8080 8081 8082 9080

ENTRYPOINT ["mitmdump", \
    "--mode", "regular@8080", \
    "--mode", "regular@8081", \
    "--mode", "regular@8082", \
    "--set", "confdir=/certs", \
    "-s", "/addon/proxy_counter.py"]
