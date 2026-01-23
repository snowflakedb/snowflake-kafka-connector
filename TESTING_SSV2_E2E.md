# Kafka Push Connector: SSV2 E2E Test Runbook

This runbook covers running the simplest Snowpipe Streaming (SSV2) e2e test in
`snowflake-kafka-connector`.

## Prereqs

- Java 11
- Maven
- Python 3 + venv
- `protoc`
- `librdkafka` 1.9.2 (build locally to avoid GLIBC mismatch)
- Snowflake test account with key-pair auth configured

## 1) Clone and checkout

```
git clone https://github.com/snowflakedb/snowflake-kafka-connector
cd snowflake-kafka-connector
git checkout master
```

## 2) Install Java 11 + protoc (nix example)

```
nix --extra-experimental-features "nix-command flakes" profile install \
  nixpkgs#temurin-bin-11 nixpkgs#protobuf
```

## 3) Create Python venv + deps

```
python3 -m venv venv
source venv/bin/activate
python -m pip install --upgrade pip setuptools
python -m pip install requests certifi \
  "confluent-kafka[avro,json,protobuf]==1.9.2" \
  avro kafka-python snowflake-connector-python fastavro
```

## 4) Build librdkafka 1.9.2 (local prefix)

```
cd /tmp
wget -O librdkafka-1.9.2.tar.gz \
  https://github.com/confluentinc/librdkafka/archive/refs/tags/v1.9.2.tar.gz
tar xzf librdkafka-1.9.2.tar.gz
cd librdkafka-1.9.2
./configure --prefix=$HOME/.local/rdkafka-1.9.2
make -j$(nproc)
make install
```

## 5) Create `profile.json`

Create `/home/repo/snowflake-kafka-connector/profile.json` with:

```
{
  "user": "<USER>",
  "password": "<PASSWORD>",
  "account": "<ACCOUNT>",
  "host": "<ACCOUNT>.<REGION>.snowflakecomputing.com:443",
  "url": "https://<ACCOUNT>.<REGION>.snowflakecomputing.com",
  "scheme": "https",
  "ssl": "on",
  "role": "<ROLE>",
  "warehouse": "<WAREHOUSE>",
  "database": "<DATABASE>",
  "schema": "<SCHEMA>",
  "private_key": "<BASE64_PKCS8_UNENCRYPTED>",
  "encrypted_private_key": "<BASE64_PKCS8_ENCRYPTED>",
  "private_key_passphrase": "<PASSPHRASE>"
}
```

Notes:
- Use key-pair auth; password auth is not sufficient for tests.
- Register the public key for the user:

```
ALTER USER <USER> SET RSA_PUBLIC_KEY='<BASE64_PUBLIC_KEY>';
```

## 6) Fix local hostname resolution

Kafka and Connect resolve the machine hostname during startup. If it is not
resolvable, add it to `/etc/hosts`. Example:

```
echo "127.0.0.1 <your-hostname>" | sudo tee -a /etc/hosts
```

## 7) Run the simplest SSV2 e2e test

```
cd /home/repo/snowflake-kafka-connector/test
source ../venv/bin/activate
export SNOWFLAKE_CREDENTIAL_FILE=/home/repo/snowflake-kafka-connector/profile.json
export LD_LIBRARY_PATH=$HOME/.local/rdkafka-1.9.2/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
export SKIP_PROTOBUF_COMPILE=true
export JAVA_HOME=/nix/store/agcdjidc7hcwlib0j3ah8ykik42ccfkw-temurin-bin-11.0.28
export PATH="$JAVA_HOME/bin:$PATH"

./run_test_confluent.sh 7.8.0 ./apache_properties false false \
  --skipProxy --tests=TestSnowpipeStreamingStringJson
```

Expected end:
```
=== Passed TestSnowpipeStreamingStringJson ===
=== All test passed ===
```

## What not to share

- `keys/` (private key material)
- `venv/`
- `apache.tgz`, `sf.log.1`, generated protobuf outputs

