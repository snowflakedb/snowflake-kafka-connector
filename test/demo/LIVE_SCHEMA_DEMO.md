# Live Schema Evolution Demo (Kafka Connect + Streaming V2 + QA6)

## What this demo proves

1. Stream continuously at 50 rows/second from a schema file.
2. Change schema live by editing one JSON file.
3. Show automatic table schema evolution and `SchemaEvolutionRecord`.
4. Show landed-row lag watermark computed on the client clock:
   - `ingestLagMs = clientNowMs - maxSendTimeMs`

## One-time setup

Build connector plugin zip:

```bash
cd /home/repo/snowflake-kafka-connector/test
export SNOWFLAKE_CREDENTIAL_FILE=/home/repo/snowflake-kafka-connector/profile.json
./build_runtime_jar.sh /home/repo/snowflake-kafka-connector package confluent AWS
```

## Start infra (while you present context)

```bash
cd /home/repo/snowflake-kafka-connector
nix --extra-experimental-features "nix-command flakes" shell nixpkgs#temurin-bin-11 --command bash -lc '
  cd test
  ./demo/live_demo_start_infra.sh 7.8.0
'
```

This prints `Infra ready in <N> seconds` when Kafka, Connect, and Schema Registry are all reachable.

## Demo script commands

Run from `test/` with venv active:

```bash
cd /home/repo/snowflake-kafka-connector/test
source ../venv/bin/activate
export LD_LIBRARY_PATH=$HOME/.local/rdkafka-1.9.2/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
```

Choose a demo id:

```bash
DEMO_ID=live$(date +%H%M%S)
```

### Terminal 2 (live view)

Start an in-place refreshing watcher in a second terminal:

```bash
cd /home/repo/snowflake-kafka-connector/test
source ../venv/bin/activate
export LD_LIBRARY_PATH=$HOME/.local/rdkafka-1.9.2/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}
./demo/live_show.sh "$DEMO_ID"
```

This refreshes every 500ms by default and automatically stops when row count reaches 2000.
You can still override it, for example every 2 seconds:

```bash
./demo/live_show.sh "$DEMO_ID" 2
```

You can also override expected row count:

```bash
./demo/live_show.sh "$DEMO_ID" 0.5 1500
```

### Terminal 1 (schema-driven sender)

Initialize topic/table/connector:

```bash
python demo/live_demo.py --profile /home/repo/snowflake-kafka-connector/profile.json --demo-id "$DEMO_ID" init
```

`init` always starts clean by truncating the interactive table.

Start with baseline schema file (fieldName only):

```bash
cat > ./demo/live_schema.json <<'JSON'
{
  "fields": [
    { "name": "fieldName", "type": "string" }
  ]
}
JSON
```

Start continuous sending (50 rows every second) in terminal 1:

```bash
python demo/live_demo.py \
  --profile /home/repo/snowflake-kafka-connector/profile.json \
  --demo-id "$DEMO_ID" \
  stream-send \
  --schema-file ./demo/live_schema.json \
  --batch-size 50 \
  --interval-sec 1 \
  --max-batches 40
```

`--max-batches 40` with `--batch-size 50` gives 2000 total rows.

While it is running, edit `./demo/live_schema.json` live to add an audience-provided field:

```json
{
  "fields": [
    { "name": "fieldName", "type": "string" },
    { "name": "customerTier", "type": "string" }
  ]
}
```

Within a few polling cycles, terminal 2 (`live_show.sh`) will show:

- row count increasing continuously
- new column appearing in `Columns`
- latency watermark using client clock:
  - `maxSendTimeMs` from landed rows
  - `clientNowMs`
  - `ingestLagMs = clientNowMs - maxSendTimeMs`
- latest landed row latency:
  - `latestRowLatencyMs = latestPushTimeMs - latestSendTimeMs`
  - this is a better "send -> landed" latency indicator than watermark lag at end
- `SchemaEvolutionRecord` containing:
  - `"evolutionType":"ADD_COLUMN"`
  - `"evolutionMode":"SNOWPIPE_STREAMING"`

Supported JSON schema field `type` values:

- `string`
- `number`
- `boolean`
- `object`
- `array`

Stop the sender in terminal 1 with `Ctrl+C`.  
Terminal 2 auto-exits when row count reaches the expected threshold (default 2000).

## Cleanup

```bash
python demo/live_demo.py --profile /home/repo/snowflake-kafka-connector/profile.json --demo-id "$DEMO_ID" cleanup
./demo/live_demo_stop_infra.sh
```
