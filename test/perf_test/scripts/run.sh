#!/bin/bash -ex

#download resource
mkdir -p data
python3 scripts/download_data.py

#install kafka
curl https://packages.confluent.io/archive/5.4/confluent-5.4.1-2.12.tar.gz --output confluent.tar.gz
tar xzvf confluent.tar.gz > /dev/null
mv confluent-5.4.1 confluent

#plugin path
mkdir -p confluent/share/confluent-hub-components
c_dir=$(pwd)
plugin_path="plugin.path=share/java,$c_dir/confluent/share/confluent-hub-components"
echo $plugin_path >> confluent/etc/kafka/connect-distributed.properties
echo $plugin_path >> confluent/etc/kafka/connect-standalone.properties
echo $plugin_path >> confluent/etc/schema-registry/connect-avro-distributed.properties
echo $plugin_path >> confluent/etc/schema-registry/connect-avro-standalone.properties

#install kafka connector
pushd ../..
CONNECTOR_VERSION=$(sed -n 's/^    <version>\(.*\)<\/version>.*/\1/p' ./pom.xml)
mvn clean package -DskipTests
cp target/snowflake-kafka-connector-${CONNECTOR_VERSION}.jar test/perf_test/confluent/share/confluent-hub-components/
popd

#download data
pushd data
tar xzvf one_g_table.json.tar.gz
tar xzvf three_hundred_column_table.json.tar.gz
popd

export CONFLUENT_HOME="$c_dir/confluent"
#run test
mvn clean compile
mvn test

#stop kafka
./confluent/bin/confluent local destroy
