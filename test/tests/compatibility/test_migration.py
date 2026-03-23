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
import time
import pytest

from lib.config_migration import V3_CONFIG_TEMPLATE, v3_config_to_v4
from lib.driver import KafkaDriver
from lib.utils import RecordProducer, wait_for


# Don't parameterize on v3, we create both connector versions explicitly here.
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_migration_without_duplicates(
    driver: KafkaDriver,
    create_custom_connector,
    create_table,
    wait_for_rows,
):
    """Test migration when there are no in-flight data during switchover."""

    test_name = "test_migration_without_duplicates"

    topic = create_table(columns='(record_metadata variant, "NUMBER" varchar)')

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
        table_name=topic,
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
        table_name=topic,
        expected=producer.records_produced,
        connector_name=v4_connector.name,
    )
    logging.info(
        f"All {producer.records_produced} records ingested — no gaps, no duplicates"
    )


# Don't parameterize on v3, we create both connector versions explicitly here.
@pytest.mark.parametrize("connector_version", ["v4"], indirect=True)
def test_migration_with_possible_duplicates(
    driver: KafkaDriver,
    create_custom_connector,
    create_table,
    wait_for_rows,
):
    """Test migration when there are in-flight data during switchover."""

    test_name = "test_migration_with_possible_duplicates"
    warmup_records = 10

    topic = create_table(
        test_name,
        columns='(record_metadata variant, "NUMBER" varchar)',
    )

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
        table_name=topic,
        expected=producer.records_produced,
        connector_name=v3_connector.name,
    )

    logging.info("Starting continuous producer")
    producer.start_continuous()

    try:
        logging.info("Waiting for v3 to ingest beyond the warmup batch")
        assert wait_for(
            lambda: driver.select_number_of_records(topic) > warmup_records
        ), f"v3 never ingested beyond {warmup_records} warmup records"
        logging.info(
            f"v3 ingested {driver.select_number_of_records(topic)} rows so far"
        )

        logging.info("Closing v3 connector while data is still flowing")
        assert v3_connector.close(wait_timeout=60)

        logging.info(
            "Creating v4 connector (same name → inherits consumer group offsets)"
        )
        v4_config_template = v3_config_to_v4(v3_config_template)
        create_custom_connector(test_name, v4_config_template)

        logging.info("Letting v4 catch up for 5s before snapshot")
        time.sleep(5)
        records_produced_so_far = producer.records_produced
        logging.info(
            f"Snapshot: {records_produced_so_far} records produced, "
            f"{driver.select_number_of_records(topic)} rows in Snowflake"
        )
        assert wait_for(
            lambda: driver.select_number_of_records(topic) > records_produced_so_far
        ), f"v4 never ingested beyond {records_produced_so_far} rows"
        logging.info(
            f"v4 is actively ingesting ({driver.select_number_of_records(topic)} rows)"
        )

    finally:
        producer.stop_continuous()

    def select_scalar(projection: str):
        return (
            driver.snowflake_conn.cursor()
            .execute(f"SELECT {projection} FROM {topic}")
            .fetchone()[0]
        )

    expected = producer.records_produced
    logging.info(
        f"Waiting for all {expected} distinct records to land in Snowflake "
        f"(currently {select_scalar('count(distinct number)')} distinct, "
        f"{select_scalar('count(*)')} total)"
    )
    assert wait_for(
        lambda: select_scalar("count(distinct number)") == expected,
        timeout=120,
    ), (
        f"Expected {expected} distinct records, "
        f"got {select_scalar('count(distinct number)')} distinct / {select_scalar('count(*)')} total"
    )

    distinct_offsets = select_scalar("count(distinct record_metadata:offset)")
    total_rows = select_scalar("count(*)")
    logging.info(
        f"Final: {expected} distinct records, {distinct_offsets} distinct offsets, "
        f"{total_rows} total rows (duplicates: {total_rows - expected})"
    )

    assert distinct_offsets == expected, (
        f"Expected {expected} distinct offsets, got {distinct_offsets}"
    )
    assert total_rows > expected, (
        f"Expected duplicates (total > {expected}), but got {total_rows}"
    )
