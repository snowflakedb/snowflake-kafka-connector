#!/bin/bash

# exit on error
set -e

# error printing function
function error_exit() {
    echo >&2 $1
    exit 1
}

# check argument number is 1 or 2
if [ $# -gt 1 ] ; then
    error_exit "Usage: ./build_apache.sh <path to snowflake repo>  or  ./build_apache.sh .  Aborting."
fi

KAFKA_CONNECT_TAG=$1
SNOWFLAKE_CONNECTOR_PATH=$1

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

# build and test the local repo
pushd $SNOWFLAKE_CONNECTOR_PATH
mvn verify -Dgpg.skip=true
popd

# get built image name
# only match the first line
SNOWFLAKE_PLUGIN_NAME=$(ls $SNOWFLAKE_PLUGIN_PATH | grep "$SNOWFLAKE_PLUGIN_NAME_REGEX" | head -n 1)
echo -e "\n=== built connector name: $SNOWFLAKE_PLUGIN_NAME ==="

# copy built connector to plugin path
mkdir -p $KAFKA_CONNECT_PLUGIN_PATH
cp $SNOWFLAKE_PLUGIN_PATH/$SNOWFLAKE_PLUGIN_NAME $KAFKA_CONNECT_PLUGIN_PATH || true
echo -e "\n=== copied connector to $KAFKA_CONNECT_PLUGIN_PATH ==="