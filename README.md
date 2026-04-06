# This is a fork of [snowflake-kakfa-connector v3.3.0](https://github.com/snowflakedb/snowflake-kafka-connector/tree/v3.3.0)
The original snowflake-kafka-connector requires all destination tables for one connector to be a part of the same snowflake schema.
i.e. there is a statically configured database, a statically configured schema, then the connector maps from 
topicName --> tableName dynamically.

A statically defined schema seems like an unnecessary limitation. It forces us to use separate connectors for every tenant, 
which in turn makes it too easy to hit the 'Up to 60 Connect Workers' AWS MSK Connector quota as we must have separate workers 
per connector.

In this fork we allow a single connector to support sending messages to different schemas for different topics.
i.e. to have a dynamic topicName - -> schema mapping.

To make it work, add this new configuration parameter to the connector configuration:
```properties
snowflake.topicPrefix2schema.map: "<topicPrefixName>:<schemaName>"

# Full Example
"connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
"snowflake.schema.name": "MARTIN",
"snowflake.database.name": "EMR_DATA_RAW_DEV",
"snowflake.topic2table.map": "sdx.sei.hcd_test_stg.test_schema.kafka_events:KAFKA_EVENTS,sdx.sei.hcd_test_stg.test_schema2.kafka_events:KAFKA_EVENTS",
"snowflake.topicPrefix2schema.map": "sdx.sei.hcd_test_stg.test_schema.:MARTIN,sdx.sei.hcd_test_stg.test_schema2.:MARTIN2",
"snowflake.ingestion.method": "SNOWPIPE_STREAMING",
"tasks.max": "8",
"snowflake.private.key": "${secretsmanager:infra/msk/hcd/fhir-connector/msk_connect_secrets:private_key}",
"snowflake.user.name": "JOSH_DBT_AIRFLOW",
"snowflake.role.name": "AIRFLOW_DATA_LOAD",
"topics.regex": "sdx.sei.hcd_test_stg.*",
"snowflake.url.name": "EEXPJWQ-UCB21633.snowflakecomputing.com:443"
```
Note that the key values in topicPrefix2schema are prefixes not regexes. These prefixes must not overlap (this is why 
there is a trailing period in the example above as the schema names are very similar). 

Known limitations of this POC: 
* upon startup the connector normally can automatically create a destination table if it doesn't already exist. That doesn't work reliably for tables in different schemas.
* the global 'snowflake.schema.name' config parameter is still needed. 
  * This is confusing because it isn't really used once topicPrefix2schema gets picked up.
  * Its still needed because the code validation logic when starting up still complains if it isn't there.

# Snowflake-kafka-connector
[![License](http://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

Snowflake-kafka-connector is a plugin of Apache Kafka Connect - ingests data from a Kafka Topic to a Snowflake Table. 

[Official documentation](https://docs.snowflake.com/en/user-guide/kafka-connector) for the Snowflake sink Kafka Connector

### Contributing to the Snowflake Kafka Connector
The following requirements must be met before you can merge your PR:
- Tests: all test suites must pass, see the [test README](https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/README-TEST.md)
- Formatter: run this script [`./format.sh`](https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/format.sh) from root
- CLA: all contributers must sign the Snowflake CLA. This is a one time signature, please provide your email so we can work with you to get this signed after you open a PR.

Thank you for contributing! We will review and approve PRs as soon as we can.

### Third party licenses
Custom license handling process is run during build to meet legal standards.
- License files are copied directly from JAR if present in one of the following locations: META-INF/LICENSE.txt, META-INF/LICENSE, META-INF/LICENSE.md
- If no license file is found then license must be manually added to [`process_licenses.py`](https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/scripts/process_licenses.py) script in order to pass build

### Test and Code Coverage Statuses

[![Kafka Connector Integration Test](https://github.com/snowflakedb/snowflake-kafka-connector/actions/workflows/IntegrationTest.yml/badge.svg?branch=master)](https://github.com/snowflakedb/snowflake-kafka-connector/actions/workflows/IntegrationTest.yml)

[![Kafka Connector Apache End2End Test](https://github.com/snowflakedb/snowflake-kafka-connector/actions/workflows/End2EndTestApache.yml/badge.svg?branch=master)](https://github.com/snowflakedb/snowflake-kafka-connector/actions/workflows/End2EndTestApache.yml)

[![Kafka Connector Confluent End2End Test](https://github.com/snowflakedb/snowflake-kafka-connector/actions/workflows/End2EndTestConfluent.yml/badge.svg?branch=master)](https://github.com/snowflakedb/snowflake-kafka-connector/actions/workflows/End2EndTestConfluent.yml)

[![codecov](https://codecov.io/gh/snowflakedb/snowflake-kafka-connector/branch/master/graph/badge.svg)](https://codecov.io/gh/snowflakedb/snowflake-kafka-connector)
