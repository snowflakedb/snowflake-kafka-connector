FROM mitmproxy/mitmproxy:11

# Two forward-proxy listeners (8080 connector tier, 8081 worker JVM tier) and a
# control API on 9080. mitmproxy generates its CA into confdir on first start;
# the Connect worker reads it from the shared volume to build its trust stores.
COPY proxy_counter.py /addon/proxy_counter.py

EXPOSE 8080 8081 9080

ENTRYPOINT ["mitmdump", \
    "--mode", "regular@8080", \
    "--mode", "regular@8081", \
    "--set", "confdir=/certs", \
    "-s", "/addon/proxy_counter.py"]
