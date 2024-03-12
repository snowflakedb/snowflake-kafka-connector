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
