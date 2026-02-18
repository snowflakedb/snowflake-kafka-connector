#!/usr/bin/env python3
import argparse
import json
import re
import time
from dataclasses import dataclass
from pathlib import Path

import requests
import snowflake.connector
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from cryptography.hazmat.primitives import serialization


def parse_private_key(private_key_base64: str, passphrase: str):
    is_encrypted = bool(passphrase)
    header = "-----BEGIN ENCRYPTED PRIVATE KEY-----" if is_encrypted else "-----BEGIN PRIVATE KEY-----"
    footer = "-----END ENCRYPTED PRIVATE KEY-----" if is_encrypted else "-----END PRIVATE KEY-----"

    pk = re.sub("-----BEGIN.*KEY-----|-----END.*KEY-----|\\s", "", private_key_base64)
    chunks = [pk[i : i + 64] for i in range(0, len(pk), 64)]
    pem = f"{header}\n" + "\n".join(chunks) + f"\n{footer}"
    return serialization.load_pem_private_key(
        pem.encode(),
        password=passphrase.encode() if is_encrypted else None,
    ).private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


@dataclass
class DemoNames:
    demo_id: str
    topic: str
    table: str
    connector: str


class LiveDemo:
    def __init__(self, profile_path: str, bootstrap: str, connect_url: str):
        with open(profile_path, "r", encoding="utf-8") as f:
            self.profile = json.load(f)

        private_key = self.profile.get("encrypted_private_key") or self.profile.get("private_key")
        if not private_key:
            raise ValueError("profile.json must contain private_key or encrypted_private_key")

        private_key_bytes = parse_private_key(private_key, self.profile.get("private_key_passphrase", ""))
        account = self.profile["host"].split(".snowflakecomputing.com")[0]

        self.sf = snowflake.connector.connect(
            user=self.profile["user"],
            private_key=private_key_bytes,
            account=account,
            warehouse=self.profile["warehouse"],
            database=self.profile["database"],
            schema=self.profile["schema"],
        )
        self.admin = AdminClient({"bootstrap.servers": bootstrap, "broker.address.family": "v4"})
        self.producer = Producer({"bootstrap.servers": bootstrap, "broker.address.family": "v4"})
        self.connect_url = connect_url.rstrip("/")

    @staticmethod
    def names(demo_id: str) -> DemoNames:
        clean = re.sub(r"[^A-Za-z0-9_]", "_", demo_id).lower()
        return DemoNames(
            demo_id=clean,
            topic=f"live_schema_demo_{clean}",
            table=f"LIVE_SCHEMA_DEMO_{clean}",
            connector=f"live_schema_demo_connector_{clean}",
        )

    def init(self, names: DemoNames):
        self._create_topic(names.topic)
        self._create_table(names.table)
        self._truncate_table(names.table)
        self._create_connector(names)

    def cleanup(self, names: DemoNames):
        requests.delete(f"{self.connect_url}/connectors/{names.connector}", timeout=10)
        self.sf.cursor().execute(f"DROP TABLE IF EXISTS {names.table}")

    def send(
        self,
        names: DemoNames,
        count: int,
        version: str,
        start_index: int,
        new_column_name: str,
        new_column_type: str,
    ):
        for i in range(start_index, start_index + count):
            payload = {"fieldName": str(i), "sendTimeMs": int(time.time() * 1000)}
            if version == "v2":
                payload[new_column_name] = self._value_for_type(new_column_type, i)
            self.producer.produce(names.topic, value=json.dumps(payload).encode("utf-8"), key=f"k-{i}".encode("utf-8"))
        self.producer.flush()

    def stream_send(
        self,
        names: DemoNames,
        schema_file: str,
        batch_size: int,
        interval_sec: float,
        start_index: int,
        max_batches: int,
    ):
        index = start_index
        batch_no = 0
        print(
            f"Streaming from schema file {schema_file}, batch_size={batch_size}, "
            f"interval_sec={interval_sec}, max_batches={max_batches if max_batches > 0 else 'infinite'}"
        )
        while True:
            fields = self._load_schema_fields(schema_file)
            for _ in range(batch_size):
                payload = self._payload_from_schema(fields, index)
                self.producer.produce(
                    names.topic,
                    value=json.dumps(payload).encode("utf-8"),
                    key=f"k-{index}".encode("utf-8"),
                )
                index += 1
            self.producer.flush()
            batch_no += 1
            print(
                f"batch={batch_no} sent={batch_size} total_sent={index - start_index} "
                f"schema_fields={[f['name'] + ':' + f['type'] for f in fields]}"
            )
            if max_batches > 0 and batch_no >= max_batches:
                return
            time.sleep(interval_sec)

    def watch(self, names: DemoNames, expected_count: int, timeout_sec: int):
        start = time.time()
        while True:
            row_count = self._count_rows(names.table)
            if row_count >= expected_count:
                print(f"Reached row count: {row_count}")
                return
            if time.time() - start > timeout_sec:
                raise TimeoutError(f"Timed out waiting for {expected_count} rows. Last count: {row_count}")
            time.sleep(2)

    def show(
        self,
        names: DemoNames,
        expected_row_count: int = 2000,
        stop_at_expected: bool = False,
        poll_interval_sec: float = 0.5,
    ):
        if stop_at_expected:
            while True:
                row_count, snapshot = self._show_once(names)
                print("\033[H\033[2J", end="")
                print(snapshot, flush=True)
                if row_count >= expected_row_count:
                    print(f"Reached expected row count: {row_count} / {expected_row_count}")
                    return
                time.sleep(poll_interval_sec)
        else:
            _, snapshot = self._show_once(names)
            print(snapshot)

    def _show_once(self, names: DemoNames):
        lines = []
        lines.append(f"Table: {names.table}")
        if not self._table_exists(names.table):
            lines.append("Status: waiting for table creation...")
            return 0, "\n".join(lines)
        row_count = self._count_rows(names.table)
        lines.append(f"Row count: {row_count}")
        lines.append("Columns:")
        rows = self.sf.cursor().execute(
            f"SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_schema = %s AND table_name = %s ORDER BY ordinal_position",
            (self.profile["schema"].upper(), names.table.upper()),
        ).fetchall()
        for r in rows:
            lines.append(f"  {r[0]} ({r[1]})")

        lines.append("SchemaEvolutionRecord rows (if any):")
        try:
            evol_rows = self.sf.cursor().execute(
                f"SELECT column_name, schema_evolution_record FROM information_schema.columns "
                f"WHERE table_schema = %s AND table_name = %s AND schema_evolution_record IS NOT NULL "
                f"ORDER BY ordinal_position",
                (self.profile["schema"].upper(), names.table.upper()),
            ).fetchall()
            if evol_rows:
                for col, rec in evol_rows:
                    lines.append(f"  {col}: {rec}")
            else:
                lines.append("  (none yet)")
        except Exception as e:
            lines.append(f"  Failed to query information_schema.schema_evolution_record: {e}")

        lines.append("DESCRIBE TABLE rows with schema evolution record:")
        desc_rows = self.sf.cursor().execute(f"DESCRIBE TABLE {names.table}").fetchall()
        for row in desc_rows:
            if len(row) > 12 and row[12]:
                lines.append(f"  {row}")

        lines.append("Server-side ingest lag (current_timestamp - latest ingest timestamp):")
        try:
            lag_row = self.sf.cursor().execute(
                f"""
                SELECT
                  COUNT(*) AS row_count,
                  MAX(RECORD_METADATA:"SnowflakeConnectorPushTime"::NUMBER) AS latest_push_ms,
                  (
                    DATE_PART(EPOCH_MILLISECOND, CURRENT_TIMESTAMP())::NUMBER
                    - MAX(RECORD_METADATA:"SnowflakeConnectorPushTime"::NUMBER)
                  ) AS ingest_lag_ms
                FROM {names.table}
                WHERE RECORD_METADATA:"SnowflakeConnectorPushTime" IS NOT NULL
                """
            ).fetchone()
            if lag_row is None or lag_row[1] is None:
                lines.append("  (no row with latency fields yet)")
            else:
                lines.append(
                    f"  rowCount={int(lag_row[0])} latestPushTimeMs={int(lag_row[1])} "
                    f"ingestLagMs={int(lag_row[2])}"
                )
        except Exception as e:
            lines.append(f"  Failed to compute server-side ingest lag: {e}")

        lines.append("Latest landed row latency (send -> connector push):")
        try:
            latest = self.sf.cursor().execute(
                f"""
                SELECT
                  FIELDNAME::STRING AS field_name,
                  SENDTIMEMS::NUMBER AS send_ms,
                  RECORD_METADATA:"SnowflakeConnectorPushTime"::NUMBER AS push_ms,
                  (RECORD_METADATA:"SnowflakeConnectorPushTime"::NUMBER - SENDTIMEMS::NUMBER) AS latency_ms
                FROM {names.table}
                WHERE SENDTIMEMS IS NOT NULL
                  AND RECORD_METADATA:"SnowflakeConnectorPushTime" IS NOT NULL
                ORDER BY push_ms DESC
                LIMIT 1
                """
            ).fetchone()
            if latest is None:
                lines.append("  (no row with push metadata yet)")
            else:
                lines.append(
                    f"  fieldName={latest[0]} sendTimeMs={int(latest[1])} "
                    f"pushTimeMs={int(latest[2])} latestRowLatencyMs={int(latest[3])}"
                )
        except Exception as e:
            lines.append(f"  Failed to compute latest landed row latency: {e}")
        return row_count, "\n".join(lines)

    @staticmethod
    def _value_for_type(value_type: str, index: int):
        if value_type == "string":
            return f"live_{index}"
        if value_type == "number":
            return index
        if value_type == "boolean":
            return index % 2 == 0
        if value_type == "object":
            return {"k": f"v{index}", "i": index}
        if value_type == "array":
            return [index, f"s{index}"]
        raise ValueError(f"Unsupported value type: {value_type}")

    def _create_topic(self, topic: str):
        result = self.admin.create_topics([NewTopic(topic, 3, 1)])
        future = result.get(topic)
        try:
            if future is not None:
                future.result()
        except Exception as e:
            if "TopicExists" not in str(e):
                raise

    def _create_table(self, table: str):
        self.sf.cursor().execute(
            f"CREATE OR REPLACE INTERACTIVE TABLE {table} ("
            f"RECORD_METADATA VARIANT, FIELDNAME VARCHAR, SENDTIMEMS NUMBER"
            f") CLUSTER BY (FIELDNAME) ENABLE_SCHEMA_EVOLUTION = TRUE"
        )

    def _truncate_table(self, table: str):
        self.sf.cursor().execute(f"TRUNCATE TABLE {table}")

    def _create_connector(self, names: DemoNames):
        requests.delete(f"{self.connect_url}/connectors/{names.connector}", timeout=10)
        payload = {
            "name": names.connector,
            "config": {
                "connector.class": "com.snowflake.kafka.connector.SnowflakeStreamingSinkConnector",
                "topics": names.topic,
                "snowflake.topic2table.map": f"{names.topic}:{names.table}",
                "tasks.max": "1",
                "snowflake.url.name": self.profile["host"],
                "snowflake.user.name": self.profile["user"],
                "snowflake.private.key": self.profile["private_key"],
                "snowflake.database.name": self.profile["database"],
                "snowflake.schema.name": self.profile["schema"],
                "snowflake.role.name": self.profile["role"],
                "key.converter": "org.apache.kafka.connect.storage.StringConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "errors.tolerance": "all",
                "errors.log.enable": "true",
                "jmx": "true",
            },
        }
        r = requests.post(f"{self.connect_url}/connectors", json=payload, timeout=20)
        if r.status_code not in (200, 201):
            raise RuntimeError(f"Failed to create connector: {r.status_code} {r.text}")

    def _count_rows(self, table: str) -> int:
        return int(self.sf.cursor().execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])

    def _table_exists(self, table: str) -> bool:
        row = self.sf.cursor().execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = %s AND table_name = %s",
            (self.profile["schema"].upper(), table.upper()),
        ).fetchone()
        return int(row[0]) > 0

    @staticmethod
    def _load_schema_fields(schema_file: str):
        raw = json.loads(Path(schema_file).read_text(encoding="utf-8"))
        fields = raw.get("fields")
        if not isinstance(fields, list) or not fields:
            raise ValueError("Schema file must contain non-empty 'fields' array")
        normalized = []
        for field in fields:
            if not isinstance(field, dict):
                raise ValueError("Each schema field must be an object")
            name = field.get("name")
            value_type = field.get("type")
            if not isinstance(name, str) or not name:
                raise ValueError("Each schema field must have non-empty string 'name'")
            if value_type not in {"string", "number", "boolean", "object", "array"}:
                raise ValueError(f"Unsupported schema type for field '{name}': {value_type}")
            normalized.append({"name": name, "type": value_type})
        if not any(f["name"] == "fieldName" for f in normalized):
            normalized.insert(0, {"name": "fieldName", "type": "string"})
        return normalized

    def _payload_from_schema(self, fields, index: int):
        payload = {"sendTimeMs": int(time.time() * 1000)}
        for field in fields:
            name = field["name"]
            value_type = field["type"]
            if name == "fieldName":
                payload[name] = str(index)
            else:
                payload[name] = self._value_for_type(value_type, index)
        return payload


def main():
    parser = argparse.ArgumentParser(description="Interactive live schema evolution demo helper")
    parser.add_argument("--profile", required=True)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--connect-url", default="http://localhost:8083")
    parser.add_argument("--demo-id", required=True)

    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("init")
    sub.add_parser("cleanup")
    send_cmd = sub.add_parser("send")
    send_cmd.add_argument("--version", choices=["v1", "v2"], required=True)
    send_cmd.add_argument("--count", type=int, default=50)
    send_cmd.add_argument("--start-index", type=int, default=0)
    send_cmd.add_argument("--new-column-name", default="newField")
    send_cmd.add_argument(
        "--new-column-type",
        choices=["string", "number", "boolean", "object", "array"],
        default="string",
    )
    watch_cmd = sub.add_parser("watch")
    watch_cmd.add_argument("--expected-count", type=int, required=True)
    watch_cmd.add_argument("--timeout-sec", type=int, default=120)
    stream_send_cmd = sub.add_parser("stream-send")
    stream_send_cmd.add_argument("--schema-file", required=True)
    stream_send_cmd.add_argument("--batch-size", type=int, default=50)
    stream_send_cmd.add_argument("--interval-sec", type=float, default=1.0)
    stream_send_cmd.add_argument("--start-index", type=int, default=0)
    stream_send_cmd.add_argument("--max-batches", type=int, default=0)
    show_cmd = sub.add_parser("show")
    show_cmd.add_argument("--expected-row-count", type=int, default=2000)
    show_cmd.add_argument("--stop-at-expected", action="store_true")
    show_cmd.add_argument("--poll-interval-sec", type=float, default=0.5)

    args = parser.parse_args()
    demo = LiveDemo(args.profile, args.bootstrap, args.connect_url)
    names = demo.names(args.demo_id)

    print(f"demo_id={names.demo_id}")
    print(f"topic={names.topic}")
    print(f"table={names.table}")
    print(f"connector={names.connector}")

    if args.command == "init":
        demo.init(names)
    elif args.command == "cleanup":
        demo.cleanup(names)
    elif args.command == "send":
        demo.send(
            names,
            args.count,
            args.version,
            args.start_index,
            args.new_column_name,
            args.new_column_type,
        )
    elif args.command == "watch":
        demo.watch(names, args.expected_count, args.timeout_sec)
    elif args.command == "stream-send":
        demo.stream_send(
            names,
            args.schema_file,
            args.batch_size,
            args.interval_sec,
            args.start_index,
            args.max_batches,
        )
    elif args.command == "show":
        demo.show(
            names,
            expected_row_count=args.expected_row_count,
            stop_at_expected=args.stop_at_expected,
            poll_interval_sec=args.poll_interval_sec,
        )


if __name__ == "__main__":
    main()
