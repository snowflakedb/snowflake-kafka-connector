#!/bin/bash

# exit on error
set -e

# check argument number is 1 or 0
if [ $# -gt 1 ]; then
    echo >&2 "Usage: ./build_image.sh <path to snowflake helm value>  or  ./build_image.sh .  Aborting."; exit 1;
fi

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
if [ ! -d $SNOWFLAKE_CONNECTOR_PATH ] ; then
    echo -e "Provided path to snowflake connector repo $SNOWFLAKE_CONNECTOR_PATH does not exist.  Aborting."; exit 1;
fi

# require the environment variable for credentials
if [[ -z "${SNOWFLAKE_CREDENTIAL_FILE}" ]]; then
    echo >&2 "Require environment variable SNOWFLAKE_CREDENTIAL_FILE but it's not set.  Aborting."; exit 1;
fi

if [ ! -f "$SNOWFLAKE_CREDENTIAL_FILE" ]; then
    echo >&2 "Provided SNOWFLAKE_CREDENTIAL_FILE $SNOWFLAKE_CREDENTIAL_FILE does not exist.  Aborting."; exit 1;
fi

# check required commands
command -v docker >/dev/null 2>&1 || { echo >&2 "Require docker but it's not installed.  Aborting."; exit 1; }
command -v minikube >/dev/null 2>&1 || { echo >&2 "Require minikube but it's not installed.  Aborting."; exit 1; }
command -v mvn >/dev/null 2>&1 || { echo >&2 "Require mvn but it's not installed.  Aborting."; exit 1; }

# match all versions of built SF connector
SNOWFLAKE_PLUGIN_NAME_REGEX="snowflake-kafka-connector-[0-9]*\.[0-9]*\.[0-9]*\.jar$"
SNOWFLAKE_PLUGIN_PATH="$SNOWFLAKE_CONNECTOR_PATH/target"

SNOWFLAKE_DOCKER_IMAGE="snowflakedb/kc-dev-build"
SNOWFLAKE_TAG="dev"
KAFKA_CONNECT_DOCKER_IMAGE="confluentinc/cp-kafka-connect"
KAFKA_CONNECT_TAG="5.4.0"
KAFKA_CONNECT_PLUGIN_PATH="/usr/share/confluent-hub-components"

DEV_CONTAINER_NAME="snow-dev-build"

# bind minikube to local docker image repo
if ! minikube status; then
    echo -e "\n=== minikube not running, try to start ==="
    minikube config set memory 8192
    minikube config set cpus 4
    minikube config set disk-size 20000MB
    minikube start
fi
eval $(minikube docker-env)


# copy credential to SNOWFLAKE_CONNECTOR_PATH
cp -rf $SNOWFLAKE_CREDENTIAL_FILE $SNOWFLAKE_CONNECTOR_PATH || true

# build and test the local repo
pushd $SNOWFLAKE_CONNECTOR_PATH
mvn package -Dgpg.skip=true
popd

# get built image name
# only match the first line
SNOWFLAKE_PLUGIN_NAME=$(ls $SNOWFLAKE_PLUGIN_PATH | grep "$SNOWFLAKE_PLUGIN_NAME_REGEX" | head -n 1 )
echo -e "\n=== built connector name: $SNOWFLAKE_PLUGIN_NAME ==="

# download Kafka connect docker image
echo -e "\n=== pull image from $KAFKA_CONNECT_DOCKER_IMAGE:$KAFKA_CONNECT_TAG ==="
docker pull $KAFKA_CONNECT_DOCKER_IMAGE:$KAFKA_CONNECT_TAG

# clean up
echo -e "\n=== try to delete container $DEV_CONTAINER_NAME if it exist ==="
$(docker rm $DEV_CONTAINER_NAME) || true

# copy built jar file to kafka connect image
echo -e "\n=== create docker container ==="
docker create --name $DEV_CONTAINER_NAME $KAFKA_CONNECT_DOCKER_IMAGE:$KAFKA_CONNECT_TAG

echo -e "\n=== copy built snowflake plugin into container ==="
docker cp $SNOWFLAKE_PLUGIN_PATH/$SNOWFLAKE_PLUGIN_NAME $DEV_CONTAINER_NAME:$KAFKA_CONNECT_PLUGIN_PATH/$SNOWFLAKE_PLUGIN_NAME

echo -e "\n=== commit the mocified container to snowflake image ==="
docker commit $DEV_CONTAINER_NAME $SNOWFLAKE_DOCKER_IMAGE:$SNOWFLAKE_TAG

# no need to push to docker hub since k8s can use local image
# push the image to our docker hub
# echo -e "\n=== push snowflake image to docker hub ==="
# docker push $SNOWFLAKE_DOCKER_IMAGE:$SNOWFLAKE_TAG

# clean up
echo -e "\n=== delete container $DEV_CONTAINER_NAME ==="
docker rm $DEV_CONTAINER_NAME
