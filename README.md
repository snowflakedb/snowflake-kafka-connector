# `snowflake-kafka-connector`

[![License](http://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)

The Snowflake Kafka Connector is a plugin for Apache Kafka Connect. It ingests data from a Kafka Topic into a Snowflake Table. 

[Official documentation](https://docs.snowflake.com/en/user-guide/kafka-connector) for the Snowflake Kafka Connector

## Contributing

### Guidelines

The following requirements must be met before you can merge your PR:
- Tests: all test suites must pass, see the [test README](https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/README-TEST.md)
- Formatter: run this script [`./format.sh`](https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/format.sh) from root
- CLA: all contributers must sign the Snowflake CLA. This is a one time signature, please provide your email so we can work with you to get this signed after you open a PR.

Thank you for contributing! We will review and approve PRs as soon as we can.

### Unit tests

```bash
mvn package -Dgpg.skip=true
```

Runs all test files in `src/test` that do not end with `IT`. Requires `SNOWFLAKE_CREDENTIAL_FILE` to be set.

### Integration tests

```bash
mvn verify -Dgpg.skip=true
```

Runs all test files in `src/test`, including unit tests.

### End-to-end tests

Refer to [test/README.md](test/README.md).

## Third party licenses
Custom license handling process is run during build to meet legal standards.
- License files are copied directly from JAR if present in one of the following locations: META-INF/LICENSE.txt, META-INF/LICENSE, META-INF/LICENSE.md
- If no license file is found then license must be manually added to [`process_licenses.py`](https://github.com/snowflakedb/snowflake-kafka-connector/blob/master/scripts/process_licenses.py) script in order to pass build

## Test and Code Coverage Statuses

[![Kafka Connector integration test](https://github.com/snowflakedb/snowflake-kafka-connector/actions/workflows/IntegrationTest.yml/badge.svg?branch=master)](https://github.com/snowflakedb/snowflake-kafka-connector/actions/workflows/IntegrationTest.yml)

[![Kafka Connector end-to-end test](https://github.com/snowflakedb/snowflake-kafka-connector/actions/workflows/end-to-end.yaml/badge.svg?branch=master)](https://github.com/snowflakedb/snowflake-kafka-connector/actions/workflows/end-to-end.yaml)

[![Kafka Connector stress test](https://github.com/snowflakedb/snowflake-kafka-connector/actions/workflows/end-to-end-stress.yml/badge.svg?branch=master)](https://github.com/snowflakedb/snowflake-kafka-connector/actions/workflows/end-to-end-stress.yml)

[![codecov](https://codecov.io/gh/snowflakedb/snowflake-kafka-connector/branch/master/graph/badge.svg)](https://codecov.io/gh/snowflakedb/snowflake-kafka-connector)
