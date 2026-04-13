"""
### Migration with duplicates but no gaps

During migration, KC v4 will inherit consumer group offsets from KC v3 if the following
conditions are met:

1. The new connector is given the same name as the old one.
    (They will belong to the same consumer group.)
2. At most `offsets.retention.minutes` has passed (defaults to 7 days).

Inheriting the consumer group offsets means that the new connector will start ingesting from
the last offset committed to *Kafka*. It's possible, especially under continuous load, that
Kafka will not be fully caught up to the last offset committed to Snowflake.

This will result in duplicate data being ingested, but no gaps.
It should be possible to deduplicate the data after ingestion using the RECORD_METADATA column.

### Migration with possible gaps

If the new connector has a different name, or too much time has passed, then depending on the
value of `auto.offset.reset`, the KC v4 will start ingesting:
- for `earliest`: from the beginning of the partition
- for `latest`: only data ingested after the new connector was created
"""

import logging
import os
import time
import pytest

from lib.config_migration import V3_CONFIG_TEMPLATE, v3_config_to_v4
from lib.driver import KafkaDriver
from lib.utils import RecordProducer, wait_for

pytestmark = pytest.mark.compatibility


# Don't parameterize on v3, we create both connector versions explicitly here.
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_migration_without_ingestion(
    driver: KafkaDriver,
    name_salt,
    create_custom_connector,
    create_table,
    wait_for_rows,
):
    """Test migration when there are no in-flight data during switchover."""

    test_name = "test_migration_without_duplicates"

    table = create_table(
        test_name.upper(), columns='(record_metadata variant, "NUMBER" varchar)'
    )
    topic = f"{test_name}{name_salt}"

    producer = RecordProducer(driver, topic)

    v3_config_template = {
        **V3_CONFIG_TEMPLATE,
        "topics": topic,
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "snowflake.enable.schematization": "true",
    }

    logging.info("Creating v3 connector and sending initial batch")
    v3_connector = create_custom_connector(test_name, v3_config_template)
    producer.send(10)
    logging.info(f"Produced 10 records (total: {producer.records_produced})")
    wait_for_rows(
        table_name=table.name,
        expected=producer.records_produced,
        connector_name=v3_connector.name,
    )

    logging.info(
        f"Waiting for Kafka consumer group offset to catch up to {producer.records_produced}"
    )
    assert wait_for(
        lambda: (
            (driver.get_consumer_group_offset(v3_connector.name, topic) or 0)
            >= producer.records_produced
        )
    ), f"Consumer group offset never reached {producer.records_produced}"
    logging.info(
        f"Consumer group offset: {driver.get_consumer_group_offset(v3_connector.name, topic)}"
    )

    logging.info("Closing v3 connector")
    assert v3_connector.close(wait_timeout=60)
    logging.info("Sending second batch while connector is down")
    producer.send(10)
    logging.info(f"Produced 10 records (total: {producer.records_produced})")

    logging.info("Creating v4 connector and sending third batch")
    v4_config_template = v3_config_to_v4(v3_config_template)
    v4_connector = create_custom_connector(test_name, v4_config_template)
    producer.send(10)
    logging.info(f"Produced 10 records (total: {producer.records_produced})")
    logging.info(
        f"Waiting for all {producer.records_produced} records to land in Snowflake"
    )
    wait_for_rows(
        table_name=table.name,
        expected=producer.records_produced,
        connector_name=v4_connector.name,
    )
    logging.info(
        f"All {producer.records_produced} records ingested — no gaps, no duplicates"
    )


# Don't parameterize on v3, we create both connector versions explicitly here.
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.parametrize(
    "ssv1_offset_migration",
    [
        "skip",
        pytest.param(
            "strict",
            marks=pytest.mark.skipif(
                # only run this test if the local proxy is available
                not os.environ.get("SNOWPIPE_STREAMING_URL"),
                reason="SNOW-3350611: SYSTEM$MIGRATE_SSV1_CHANNEL_OFFSET requires "
                "local proxy (not available on non-internal accounts)",
            ),
        ),
    ],
)
def test_migration_with_ingestion(
    driver: KafkaDriver,
    name_salt,
    create_custom_connector,
    create_table,
    wait_for_rows,
    ssv1_offset_migration,
):
    """Test migration when there are in-flight data during switchover.

    With ssv1_offset_migration=skip (default), KC v4 starts from the consumer group offset,
    which may lag behind the SSv1 committed offset, causing duplicates.

    With ssv1_offset_migration=strict, KC v4 reads the SSv1 committed offset and uses it
    as the starting point, so no duplicates should occur.
    """

    # Mixed case on purpose to ensure case sensitivity is handled correctly.
    test_name = f"test_Migration_with_possible_duplicates_{ssv1_offset_migration}"
    warmup_records = 10

    table = create_table(
        test_name.upper(),
        columns='(record_metadata variant, "NUMBER" varchar)',
    )
    topic = f"{test_name}{name_salt}"

    producer = RecordProducer(driver, topic)

    v3_config_template = {
        **V3_CONFIG_TEMPLATE,
        "topics": topic,
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "snowflake.enable.schematization": "true",
    }

    logging.info(f"Creating v3 connector and sending {warmup_records} warmup records")
    v3_connector = create_custom_connector(test_name, v3_config_template)
    producer.send(warmup_records)
    logging.info(
        f"Produced {warmup_records} records (total: {producer.records_produced})"
    )
    wait_for_rows(
        table_name=table.name,
        expected=producer.records_produced,
        connector_name=v3_connector.name,
    )

    logging.info("Starting continuous producer")
    producer.start_continuous()

    try:
        logging.info("Waiting for v3 to ingest beyond the warmup batch")
        assert wait_for(lambda: table.select_scalar("count(*)") > warmup_records), (
            f"v3 never ingested beyond {warmup_records} warmup records"
        )
        logging.info(f"v3 ingested {table.select_scalar('count(*)')} rows so far")

        logging.info("Closing v3 connector while data is still flowing")
        assert v3_connector.close(wait_timeout=60)

        logging.info(
            "Creating v4 connector (same name → inherits consumer group offsets)"
        )
        v4_config_template = {
            **v3_config_to_v4(v3_config_template),
            "snowflake.streaming.classic.offset.migration": ssv1_offset_migration,
        }
        v4_connector = create_custom_connector(test_name, v4_config_template)

        logging.info("Letting v4 catch up for 5s before snapshot")
        time.sleep(5)
        records_produced_so_far = producer.records_produced
        logging.info(
            f"Snapshot: {records_produced_so_far} records produced, "
            f"{table.select_scalar('count(*)')} rows in Snowflake"
        )
        wait_for_rows(
            table_name=table.name,
            at_least=True,
            expected=records_produced_so_far + 1,
            connector_name=v4_connector.name,
        )
        logging.info(
            f"v4 is actively ingesting ({table.select_scalar('count(*)')} rows)"
        )

    finally:
        producer.stop_continuous()

    expected = producer.records_produced
    logging.info(
        f"Waiting for all {expected} distinct records to land in Snowflake "
        f"(currently {table.select_scalar('count(distinct number)')} distinct, "
        f"{table.select_scalar('count(*)')} total)"
    )
    assert wait_for(
        lambda: table.select_scalar("count(distinct number)") == expected,
        timeout=120,
    ), (
        f"Expected {expected} distinct records, "
        f"got {table.select_scalar('count(distinct number)')} distinct / {table.select_scalar('count(*)')} total"
    )

    distinct_offsets = table.select_scalar("count(distinct record_metadata:offset)")
    total_rows = table.select_scalar("count(*)")
    logging.info(
        f"Final: {expected} distinct records, {distinct_offsets} distinct offsets, "
        f"{total_rows} total rows (duplicates: {total_rows - expected})"
    )

    assert distinct_offsets == expected, (
        f"Expected {expected} distinct offsets, got {distinct_offsets}"
    )

    if ssv1_offset_migration == "strict":
        assert total_rows == expected, (
            f"With strict mode, expected exactly {expected} rows (no duplicates), "
            f"but got {total_rows}"
        )
    else:
        assert total_rows > expected, (
            f"Expected duplicates (total > {expected}), but got {total_rows}"
        )


# Don't parameterize on v3, we create both connector versions explicitly here.
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
@pytest.mark.skipif(
    # only run this test if the local proxy is available
    not os.environ.get("SNOWPIPE_STREAMING_URL"),
    reason="SNOW-3350611: SYSTEM$MIGRATE_SSV1_CHANNEL_OFFSET requires "
    "local proxy (not available on non-internal accounts)",
)
def test_migration_different_connector_name(
    driver: KafkaDriver,
    name_salt,
    create_custom_connector,
    create_table,
    wait_for_rows,
):
    """Prove that SYSTEM$MIGRATE_SSV1_CHANNEL_OFFSET migrates offsets server-side.

    Uses a *different* connector name for v4 so there is no consumer group inheritance.
    With auto.offset.reset=earliest, Kafka re-delivers all records from offset 0.
    With ssv1_offset_migration=skip, v4 would re-ingest everything → duplicates.
    With ssv1_offset_migration=strict, the system function writes the SSv1 offset
    to the SSv2 channel, so v4 skips already-committed records → no duplicates.

    IMPORTANT NOTE:
    This test only works because KC v3 did *not* append the connector name to the channel name.
    If it did, we'd be looking up the wrong channel name.
    """

    test_name = "test_Migration_different_connector_name"

    table = create_table(
        test_name.upper(), columns='(record_metadata variant, "NUMBER" varchar)'
    )
    topic = f"{test_name}{name_salt}"

    producer = RecordProducer(driver, topic)

    v3_config_template = {
        **V3_CONFIG_TEMPLATE,
        "topics": topic,
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "snowflake.enable.schematization": "true",
    }

    # Phase 1: v3 ingests records via SSv1
    logging.info("Creating v3 connector and sending initial batch")
    v3_connector = create_custom_connector(test_name, v3_config_template)
    producer.send(20)
    logging.info(f"Produced 20 records (total: {producer.records_produced})")
    wait_for_rows(
        table_name=table.name,
        expected=producer.records_produced,
        connector_name=v3_connector.name,
    )
    logging.info("Closing v3 connector")
    assert v3_connector.close(wait_timeout=60)

    v3_rows = table.select_scalar("count(*)")
    logging.info(f"v3 ingested {v3_rows} rows, now closed")

    # Phase 2: v4 with a DIFFERENT connector name → no consumer group inheritance.
    # auto.offset.reset=earliest forces Kafka to re-deliver from offset 0.
    # The system function is the only mechanism that prevents re-ingestion.
    v4_name = f"{test_name}_v4"
    v4_config_template = {
        **v3_config_to_v4(v3_config_template),
        "snowflake.streaming.classic.offset.migration": "strict",
        "consumer.override.auto.offset.reset": "earliest",
    }
    logging.info(
        f"Creating v4 connector with different name ({v4_name}) and strict mode"
    )
    v4_connector = create_custom_connector(v4_name, v4_config_template)

    # Phase 3: Send more records and verify no duplicates
    producer.send(10)
    expected = producer.records_produced
    logging.info(f"Produced 10 more records (total: {expected})")

    wait_for_rows(
        table_name=table.name,
        expected=expected,
        connector_name=v4_connector.name,
    )

    total_rows = table.select_scalar("count(*)")
    distinct_offsets = table.select_scalar("count(distinct record_metadata:offset)")
    logging.info(
        f"Final: {expected} expected, {distinct_offsets} distinct offsets, "
        f"{total_rows} total rows"
    )

    assert distinct_offsets == expected, (
        f"Expected {expected} distinct offsets, got {distinct_offsets}"
    )
    assert total_rows == expected, (
        f"System function migration should prevent duplicates: "
        f"expected {expected} rows, got {total_rows}"
    )


# Don't parameterize on v3, we create both connector versions explicitly here.
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_migration_from_snowpipe(
    driver: KafkaDriver,
    name_salt,
    create_custom_connector,
    create_table,
    wait_for_rows,
):
    """Test migration from KC v3 file-based Snowpipe to KC v4 (SSv2).

    SNOW-3293138: Verifies that a clean switchover from file-based Snowpipe to
    Snowpipe Streaming produces no gaps and no duplicates when the consumer group
    offsets are inherited (same connector name).
    """

    test_name = "test_migration_from_snowpipe"
    warmup_records = 10

    table = create_table(
        test_name.upper(),
        columns="(record_metadata variant, record_content variant)",
    )
    topic = f"{test_name}{name_salt}"
    producer = RecordProducer(driver, topic)

    # File-based Snowpipe: schematization unsupported, buffer.flush.time >= 10.
    v3_config_template = {
        **V3_CONFIG_TEMPLATE,
        "topics": topic,
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "buffer.flush.time": "10",
    }
    v3_config_template["snowflake.ingestion.method"] = "SNOWPIPE"
    v3_config_template.pop("snowflake.streaming.max.client.lag")

    logging.info(
        f"Creating v3 Snowpipe connector and sending {warmup_records} warmup records"
    )
    v3_connector = create_custom_connector(test_name, v3_config_template)
    producer.send(warmup_records)
    logging.info(
        f"Produced {warmup_records} records (total: {producer.records_produced})"
    )
    wait_for_rows(
        table_name=table.name,
        expected=producer.records_produced,
        connector_name=v3_connector.name,
    )

    logging.info("Starting continuous producer")
    producer.start_continuous()

    try:
        logging.info("Waiting for v3 to ingest beyond the warmup batch")
        assert wait_for(lambda: table.select_scalar("count(*)") > warmup_records), (
            f"v3 never ingested beyond {warmup_records} warmup records"
        )
        logging.info(f"v3 ingested {table.select_scalar('count(*)')} rows so far")

        logging.info("Closing v3 connector while data is still flowing")
        assert v3_connector.close(wait_timeout=60)

        v3_kafka_offset = driver.get_consumer_group_offset(v3_connector.name, topic)
        logging.info(f"v3 consumer group offset after shutdown: {v3_kafka_offset}")

        # File-based Snowpipe ingests staged files asynchronously. If we start
        # v4 (SSv2) before Snowpipe finishes draining, SSv2 rows from newer
        # offsets can land before Snowpipe finishes loading older ones,
        # breaking end-to-end ordering.
        logging.info("Waiting for Snowpipe to finish ingesting staged files")
        wait_for_rows(table_name=table.name, expected=v3_kafka_offset, at_least=True)

        logging.info(
            "Creating v4 connector (same name → inherits consumer group offsets)"
        )
        v4_config_template = v3_config_to_v4(v3_config_template)
        v4_connector = create_custom_connector(test_name, v4_config_template)

        logging.info("Letting v4 catch up for 5s before snapshot")
        time.sleep(5)
        records_produced_so_far = producer.records_produced
        logging.info(
            f"Snapshot: {records_produced_so_far} records produced, "
            f"{table.select_scalar('count(*)')} rows in Snowflake"
        )
        wait_for_rows(
            table_name=table.name,
            at_least=True,
            expected=records_produced_so_far + 1,
            connector_name=v4_connector.name,
        )
        logging.info(
            f"v4 is actively ingesting ({table.select_scalar('count(*)')} rows)"
        )

    finally:
        producer.stop_continuous()

    expected = producer.records_produced
    logging.info(
        f"Waiting for all {expected} distinct records to land in Snowflake "
        f"(currently {table.select_scalar('count(distinct record_content:number)')} "
        f"distinct, {table.select_scalar('count(*)')} total)"
    )
    wait_for_rows(
        table_name=table.name,
        expected=expected,
        connector_name=v4_connector.name,
    )

    total_rows = table.select_scalar("count(*)")
    distinct_numbers = table.select_scalar("count(distinct record_content:number)")
    logging.info(
        f"Final: {expected} expected, {distinct_numbers} distinct, {total_rows} total"
    )
    assert distinct_numbers == expected, (
        f"Expected {expected} distinct records, got {distinct_numbers}"
    )
    assert total_rows == expected, (
        f"Expected exactly {expected} rows (no duplicates), got {total_rows}"
    )
