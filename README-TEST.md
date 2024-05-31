# Kafka Connector Test Framework

## CI/CD Test

GitHub Action is used for CI/CD test. The workflows can be found in `.github/workflows`. Currently, there are 5 workflows:

`End2EndTestApacheAws.yml` : Run unit test. Then run end to end test on Apache 2.5.0. Test against Snowflake with on AWS. Run on all PRs.

`End2EndTestApacheAzure.yml` : Same with the previous workflow but test with Snowflake deployed on AZURE.

`End2EndTestConfluentAws.yml` : Run unit test. Then run end to end test on Confluent 5.5.0. Test against Snowflake with on AWS. Run on all PRs.

`End2EndTestConfluentAzure.yml` : Same with the previous workflow but test with Snowflake deployed on AZURE.

`IntegrationTestAws.yml` : Run unit and integration test. Test against Snowflake with on AWS. Run on all PRs.

`IntegrationTestAzure.yml` : Same with the previous workflow but test with Snowflake deployed on AZURE.

`End2EndTestFull.yml` : Run unit test and integration test. Then run end to end test on Confluent 5.2.0, 5.4.0, and 5.5.0 and Apache 2.2.0, 2.4.0, and 2.5.0. Test with Snowflake deployed on AWS. Executed daily, checkout the most recent master branch.

`End2EndTestFullAzure.yml` : Same with the previous workflow but test with Snowflake deployed on AZURE.

`StressTest.yml`: Run unit test. Then run stress test on Confluent 5.5.0 with Snowflake deployed on AWS.

#### Manage secrets

All test requires a `profile.json` to execute, which contains username and private key to login into Snowflake. On GitHub Action this file is encrypted and in each workflow, there is a step called `Decrypt profile.json` to dectypt the file for testing. 

### Unit Test

Unit test can be run by `mvn package -Dgpg.skip=true`. Idealy it should only test function input and output and should not require Snowflake connections. But currently unit test also requires `profile.josn` to execute, due to test splition. All test files in the test folder that does not end with `IT` are executed as unit test. Unit test takes less than one minute to execute.

### Integration Test

Integration test can be run by `mvn verify -Dgpg.skip=true`. Integration test will conver all test files in the maven test folder `src/test`, including unit test files. Integration test usually takes 20-30 minutes to run.

### End to End Test

End to end test spin up an actual Kafka cluster, then send records to Kafka and verify records shows up in Snowflake. To run the test, first make sure evironment variable`SNOWFLAKE_CREDENTIAL_FILE` is set `export SNOWFLAKE_CREDENTIAL_FILE="path/to/profile.json"`. **Then `cd test` to enter the test folder.** End to end test is splited into two steps - building jar file and executing.

```
./build_runtime_jar.sh <path/to/snowflake-git-repo> <package/verify>
Build the current repo and only run unit test:
	./build_runtime_jar.sh ../../snowflake-kafka-connector package
Checkout from GitHub master and run integration test:
	./build_runtime_jar.sh
```

The above command build a jar file and put it at `/usr/local/share/kafka/plugins`. Then the following command will spin up Kafka and Kafka Connect cluster and execute the test suits. 

```
./run_test_<confluent/apache>.sh <version> <path to apache config folder> <stress> <ssl> [--skipProxy] [--tests=TestStringJson,TestAvroAvro,...]
Run end to end test on Confluent 5.5.0 with stress test, without ssl
	./run_test_confluent.sh 5.5.0 ./apache_properties true
Run end to end test on Apache 2.2.0 without stress test, with ssl
	./run_test_apache.sh 2.2.0 ./apache_properties_ssl false true
	
Optional arguments (useful for local development):
    --skipProxy - skip running proxy tests
    --tests - comma separated list of test suites to run
```

Note: if ssl is set to true, path to apache config folder must be `apache_properties_ssl` instead of `apache_properties`

Inside the run test script there are five steps: starting cluster, generating connector configuration, sending records, verifying result, and cleaning up test.

#### Step 1: Start Cluster

The script will download and unzip Confluent/Apache Kafka with specified versions. After that it will spawn threads in the background to start Zookeeper, Kafka, and Kafka Connect. If it is Confluent Kafka, it spins up Schema Registry as well. 

All components contain only a single node. Their configurations can be found in folder `test/apache_properties` or `test/apache_properties_ssl`. If SSL is enabled, we also generate SSL certificates on the fly and use that to encrypt the whole Kafka server and clients. 

#### Step 2: Generate Connector Configuration

Connector configurations in folder `test/rest_request_template` has a one to one correspondence with test cases in folder `test/test_suit`. For example, if a connector is created with `test/rest_request_template/travis_correct_json_json.json`, then the python class that send records to the connector and verify results is defined in `test/test_suit/test_json_json.py`. 

During execution, the driver program `test/test_verify.py` will replace capitalized words such as `SNOWFLAKE_TEST_TOPIC` and `CONFLUENT_SCHEMA_REGISTRY` in the template json file with the actual values. 

#### Step 3: Send Records

`test_json_json.py` defines a cless `TestJsonJson`, which has three major APIs: `send`, `verify` and `clean`. The driver `test/test_verify.py`  will call the three APIs sequentially to send records, verity they appear at snowflake and clean up the test.

Records are sent to a specific partition of a corresponding partition in the topic. The corresponding connector consume the topic and send record to Snowflake.

#### Step 4: Verify Result

The test class verify three things:

- Row count in Snowflake table equals the sent record number
- The first record in the table equals the sent record
- In the end internal stages are cleaned up

Verifying internal stages are cleaned up is tricky. When there is no more record in Kafka, Kafka Connect will close the connector. Resulting in cleaner not having enough time to purge the ingested files from the internal stage. Therefore we only verify this on trivial test cases. In large test cases `travis_pressure_restart` and `travis_pressure_string_json` we don't verify that internal stage is cleaned up. 

#### Step 5: Clean Up

After the test we delete table/stage/pipe and then kill the thread of Zookeeper, Kafka, Kafka Connect and Schema Registry. Manuel clean up can be executed by the following command:

```
python3 test_verify.py localhost:9094 http://localhost:8081 localhost:8083 clean 5.5.0 _8dl9 false false
```

Everytime we run the test, a four digit random sub-string will be appended to the corresponding test table/stage/pipe, in order to avoid two tests running in parallel collide with each other. Therefore, we only need to change the suffix, which, in the above example, is `_8dl9`, to the suffix that we want to clean up. All other parameters are dummy and does not affect the cleaning. 

#### End to End Testcases

| Test Name                                        | Key Converter                               | Value Converter                             | Topic # | Partition # per Topic | Record # per Partition | SinkTask # | Execution Time | Frequency | Notes                                                |
| ------------------------------------------------ | ------------------------------------------- | ------------------------------------------- | ------- | --------------------- | ---------------------- | ---------- | -------------- | --------- | ---------------------------------------------------- |
| travis_correct_avro_avro                         | SnowflakeAvroConverterWithoutSchemaRegistry | SnowflakeAvroConverterWithoutSchemaRegistry | 1       | 1                     | 100                    | 1          | < 4 Mins       | Every PR  |                                                      |
| travis_correct_avrosr_avrosr                     | SnowflakeAvroConverter                      | SnowflakeAvroConverter                      | 1       | 1                     | 100                    | 1          | < 4 Mins       | Every PR  |                                                      |
| travis_correct_json_json                         | SnowflakeJsonConverter                      | SnowflakeJsonConverter                      | 1       | 1                     | 100                    | 1          | < 4 Mins       | Every PR  |                                                      |
| travis_correct_native_complex_smt                | StringConverter                             | JsonConverter (native)                      | 1       | 1                     | 100                    | 1          | < 4 Mins       | Every PR  | SMTs: ValueToKey ExtractField$Key ReplaceField$Value |
| travis_correct_native_string_avrosr              | StringConverter                             | AvroConverter (native)                      | 1       | 1                     | 100                    | 1          | < 4 Mins       | Every PR  |                                                      |
| travis_correct_native_string_json_without_schema | StringConverter                             | JsonConverter (native)                      | 1       | 1                     | 100                    | 1          | < 4 Mins       | Every PR  |                                                      |
| travis_correct_native_string_protobuf            | StringConverter                             | ProtobufConverter (blueapron)               | 1       | 1                     | 100                    | 1          | < 4 Mins       | Every PR  |                                                      |
| travis_correct_string_avro                       | StringConverter                             | SnowflakeAvroConverterWithoutSchemaRegistry | 1       | 1                     | 100                    | 1          | < 4 Mins       | Every PR  |                                                      |
| travis_correct_string_avrosr                     | StringConverter                             | SnowflakeAvroConverter                      | 1       | 1                     | 100                    | 1          | < 4 Mins       | Every PR  |                                                      |
| travis_correct_string_json                       | StringConverter                             | SnowflakeJsonConverter                      | 1       | 1                     | 100                    | 1          | < 4 Mins       | Every PR  |                                                      |
| travis_pressure_restart                          | StringConverter                             | SnowflakeJsonConverter                      | 10      | 3                     | 200,000                | 3          | < 10 Mins      | Daily     | Restarts connector every minute while consuming      |
| travis_pressure_string_json                      | StringConverter                             | SnowflakeJsonConverter                      | 200     | 12                    | 40,000                 | 10         | < 4 Hours      | Weekly    |                                                      |
| travis_correct_confluent_protobuf_protobuf       | ProtobufConverter (Confluent)               | ProtobufConverter (Confluent)               | 1       | 1                     | 100                    | 1          | < 4 Mins       | Manually  | Only works with Confluent version >= 5.5.0.          |

### Daily Full Version Test

Full version test aims at providing Confluent and Apache Kafka version coverage. `End2EndTestFull.yml` and `End2EndTestFullAzure.yml` defines two workflows in GitHub Action that run daily. Master branch in checkout in the workflow and the following versions are tested:

| Confluent Version | Apache Version |
| ----------------- | -------------- |
| 5.2.0             | 2.2.0          |
| 5.4.0             | 2.4.0          |
| 5.5.0             | 2.5.0          |

### Stress Test

Stress test is a test case in end to end test framework. For stress test, the basic Kafka and Kafka Connect cluster configurations are the same with end to end test, which is single node Kafka and Kafka Connect cluster. The test case is run weekly with GitHub Action workflow defined in `StressTest.yml`. To run the stress test manually, follow the build step of end to end test, then execute:

```
./run_test_confluent.sh 5.5.0 ./apache_properties true
```

In the test we open 2400 partitions and send 40,000 records in total to each partition. 40,000 records are sent in 4 rounds. In each round 10,000 records are sent, the framework verify that the 10,000 records shows up in Snowflake before it starts to send the next round. 

## Regression Test

 Jenkins is used for regression test. The seed job is of name `ConnectorRegressRunner`. In regression test we checkout code from GitHub and run unit test and integration test. We build the connector and test against Snowflake deployed on preprod3. This test helps us to identify breaking change in Snowflake before Snowflake releases. There are three sub-jobs:

`ConnectorRegress_kafka_v1.4.3`: test Kafka Connector version 1.4.3.

`ConnectorRegress_kafka_v1.5.0`: test Kafka Connector version 1.5.0.

`ConnectorRegress_kafka_master`: test Kafka Connector most recent master branch.

## Related file short description

Files in directory .github/scripts

- decrypt_secret.sh: Decrypt `profile.json` for integration test and end to end test.

- profile.json.gpg: Encrypted `profile.json` for integration test and end to end test for AWS deployment

- profile_azure.json.gpg: Encrypted `profile.json` for integration test and end to end test for Azure deployment

Folders and files in directory test/

- apache_properties/: Configurations for Kafka, Zookeepers, Kafka Connect, etc.
- apache_properties_ssl/: Configurations for Kafka cluster with SSL enabled.
- helm_values/: Configuration for Kafka cluster deplyed with Kubernetes.
- rest_request_template/: Connector configuration file for end to end test.
- test_data/: Avro/Profobuf data for end to end test.
- test_suit/: End to end test cases.
- build_runtime_jar.sh: Build connector jar file.
- build_image.sh: Build a container with the connector jar file and push that to local Docker repo.
- generate_ssl_key.sh: Generate SSL key on the fly for testing with SSL enabled.
- run_test_apache.sh: Run end to end test on Apache Kafka.
- run_test_confluent.sh: Run end to end test on Confluent Kafka.
- run_test.sh: Run end to end test on Confluent Kafka deployed on Kubernetes.
- setup_k8s.sh: Setup Kubernetes environment.
- test_verify.py: Driver program for calling test cases defiled in folder test_suit/.
- utils.sh: Common shell functions.

## How to check code coverage

Visit this link for basic information: https://codecov.io/gh/snowflakedb/snowflake-kafka-connector

This link for compare two commit or compare master vs master@ 5 days ago. https://codecov.io/gh/snowflakedb/snowflake-kafka-connector/compare 