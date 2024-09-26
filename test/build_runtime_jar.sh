#!/bin/bash

# exit on error
set -e

# error printing function
function error_exit() {
    echo >&2 $1
    exit 1
}

# check argument number is 1 or 2 or 3
if [ $# -gt 4 ] || [ $# -lt 1 ]; then
    error_exit "Usage: ./build_runtime_jar.sh [<path to snowflake repo>] [verify/package/none] [apache/confluent] [AWS/AZURE/GCP].
    Default values are: verify, apache, AWS. Exiting script"
fi

SNOWFLAKE_CONNECTOR_PATH=$1
BUILD_METHOD=$2
BUILD_FOR_RUNTIME=$3
BUILD_FOR_CLOUD=$4

if [[ -z "${BUILD_METHOD}" ]]; then
    # Default build method verify
    BUILD_METHOD="verify"
fi

if [[ $BUILD_FOR_RUNTIME == "confluent" ]]; then
    POM_FILE_NAME="pom_confluent.xml"
else
  # Default build target is for Apache
  BUILD_FOR_RUNTIME="apache"
  POM_FILE_NAME="pom.xml"
fi

# Some of the integration tests use cloud vendor specific resources
if [[ -z "${BUILD_FOR_CLOUD}" ]]; then
    # Default
    BUILD_FOR_CLOUD="AWS"
fi

# check if connector path is set or checkout from github master
if [[ -z "${SNOWFLAKE_CONNECTOR_PATH}" ]]; then
    # Always re-pull code from github, no one should develop under the test_script folder
    echo -e "\n=== path to snowflake connector repo is not set, clone snowflake-kafka-connector from github and build ==="
    SNOWFLAKE_CONNECTOR_PATH="./snowflake-kafka-connector"
    echo -e "\n=== $SNOWFLAKE_CONNECTOR_PATH will be force deleted ==="
    rm -rf $SNOWFLAKE_CONNECTOR_PATH
    mkdir $SNOWFLAKE_CONNECTOR_PATH
    git clone https://github.com/snowflakedb/snowflake-kafka-connector $SNOWFLAKE_CONNECTOR_PATH
fi

# check if the provided snowflake connector folder exist
if [ ! -d $SNOWFLAKE_CONNECTOR_PATH ]; then
    error_exit "Provided path to snowflake connector repo $SNOWFLAKE_CONNECTOR_PATH does not exist.  Aborting."
fi

# require the environment variable for credentials
if [[ -z "${SNOWFLAKE_CREDENTIAL_FILE}" ]]; then
    error_exit "Require environment variable SNOWFLAKE_CREDENTIAL_FILE but it's not set.  Aborting."
fi

if [ ! -f "$SNOWFLAKE_CREDENTIAL_FILE" ]; then
    error_exit "Provided SNOWFLAKE_CREDENTIAL_FILE $SNOWFLAKE_CREDENTIAL_FILE does not exist.  Aborting."
fi

# check required commands
command -v mvn >/dev/null 2>&1 || error_exit "Require mvn but it's not installed.  Aborting."

# match all versions of built SF connector
SNOWFLAKE_PLUGIN_NAME_REGEX="snowflake-kafka-connector-[0-9]*\.[0-9]*\.[0-9]*\.jar$"
SNOWFLAKE_PLUGIN_PATH="$SNOWFLAKE_CONNECTOR_PATH/target"

KAFKA_CONNECT_PLUGIN_PATH="/usr/local/share/kafka/plugins"

# copy credential to SNOWFLAKE_CONNECTOR_PATH
cp -rf $SNOWFLAKE_CREDENTIAL_FILE $SNOWFLAKE_CONNECTOR_PATH || true

echo "Building Jar for Runtime: $BUILD_FOR_RUNTIME"

# build and test the local repo
pushd $SNOWFLAKE_CONNECTOR_PATH
case $BUILD_METHOD in
	verify)
	  # mvn clean should clean the target directory, hence using default pom.xml
	  mvn -f $POM_FILE_NAME clean

	  # skip Iceberg tests outside of AWS
	  if [[ $BUILD_FOR_CLOUD == "AWS" ]]; then
	    echo "Running integration tests against AWS cloud"
	    mvn -f $POM_FILE_NAME verify -Dgpg.skip=true -Dhttp.keepAlive=false -Dmaven.wagon.http.pool=false -Dmaven.wagon.httpconnectionManager.ttlSeconds=120 -P aws
	  else
	    echo "Running integration tests against non-AWS cloud"
	    mvn -f $POM_FILE_NAME verify -Dgpg.skip=true -Dhttp.keepAlive=false -Dmaven.wagon.http.pool=false -Dmaven.wagon.httpconnectionManager.ttlSeconds=120 -P non-aws
	  fi
		;;
	package)
	  # mvn clean should clean the target directory, hence using default pom.xml
	  mvn -f $POM_FILE_NAME clean
	  # mvn package with pom_confluent runs the kafka-connect-maven-plugin which creates a zip file
	  # More information: https://docs.confluent.io/platform/current/connect/kafka-connect-maven-plugin/site/plugin-info.html
    mvn -f $POM_FILE_NAME package -Dgpg.skip=true -Dhttp.keepAlive=false -Dmaven.wagon.http.pool=false -Dmaven.wagon.httpconnectionManager.ttlSeconds=120
		;;
	none)
		echo -e "\n=== skip building, please make sure built connector exist ==="
		;;
  *)
    error_exit "Usage: ./build_image.sh [<path to snowflake repo>] [verify/package/none] . Unknown build method $BUILD_METHOD.  Aborting."
  esac
popd

# get built image name
# only match the first line
SNOWFLAKE_PLUGIN_NAME=$(ls $SNOWFLAKE_PLUGIN_PATH | grep "$SNOWFLAKE_PLUGIN_NAME_REGEX" | head -n 1)
echo -e "\nbuilt connector name: $SNOWFLAKE_PLUGIN_NAME"

mkdir -m 777 -p $KAFKA_CONNECT_PLUGIN_PATH || \
sudo mkdir -m 777 -p $KAFKA_CONNECT_PLUGIN_PATH

if [[ $BUILD_FOR_RUNTIME == "confluent" ]]; then
    # For confluent, copy the zip file and unzip it later
    echo "For confluent RUNTIME: Copying Kafka Connect Maven Generated Zip file to a temporary location"
    cp $SNOWFLAKE_PLUGIN_PATH/components/packages/snowflakeinc-snowflake-kafka-connector-*.zip /tmp/sf-kafka-connect-plugin.zip
    ls /tmp/sf-kafka-connect-plugin*
else
    # Apache Kafka
    # Only copy built connector to plugin path
    cp $SNOWFLAKE_PLUGIN_PATH/$SNOWFLAKE_PLUGIN_NAME $KAFKA_CONNECT_PLUGIN_PATH || true
    echo -e "copied SF Plugin Connector to $KAFKA_CONNECT_PLUGIN_PATH"
fi

KAFKA_CONNECT_DOCKER_JAR_PATH="$SNOWFLAKE_CONNECTOR_PATH/docker-setup/snowflake-kafka-docker/jars"
mkdir -m 777 -p $KAFKA_CONNECT_DOCKER_JAR_PATH
cp $SNOWFLAKE_PLUGIN_PATH/$SNOWFLAKE_PLUGIN_NAME $KAFKA_CONNECT_DOCKER_JAR_PATH || true
echo -e "copied connector to $KAFKA_CONNECT_DOCKER_JAR_PATH for docker"