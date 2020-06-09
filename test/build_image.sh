#!/bin/bash

# exit on error
set -e

# error printing function
function error_exit() {
    echo >&2 $1
    exit 1
}

# check argument number is 1 or 2 or 3
if [ $# -gt 3 ] || [ $# -lt 1 ]; then
    error_exit "Usage: ./build_image.sh <version> [<path to snowflake repo>] [verify/package/none] .  Aborting."
fi

KAFKA_CONNECT_TAG=$1
SNOWFLAKE_CONNECTOR_PATH=$2
BUILD_METHOD=$3

if [[ -z "${BUILD_METHOD}" ]]; then
    # Default build method verify
    BUILD_METHOD="verify"
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
command -v docker >/dev/null 2>&1 || error_exit "Require docker but it's not installed.  Aborting."
command -v minikube >/dev/null 2>&1 || error_exit "Require minikube but it's not installed.  Aborting."
command -v mvn >/dev/null 2>&1 || error_exit "Require mvn but it's not installed.  Aborting."

# match all versions of built SF connector
SNOWFLAKE_PLUGIN_NAME_REGEX="snowflake-kafka-connector-[0-9]*\.[0-9]*\.[0-9]*\.jar$"
SNOWFLAKE_PLUGIN_PATH="$SNOWFLAKE_CONNECTOR_PATH/target"

SNOWFLAKE_DOCKER_IMAGE="snowflakedb/kc-dev-build"
SNOWFLAKE_TAG="dev"
KAFKA_CONNECT_DOCKER_IMAGE="confluentinc/cp-kafka-connect"
KAFKA_CONNECT_PLUGIN_PATH="/usr/share/confluent-hub-components"
KAFKA_CONNECT_PLUGIN_PATH_5_0_0="/usr/share/java"

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
case $BUILD_METHOD in
	verify)
	  mvn clean
    mvn verify -Dgpg.skip=true
		;;
	package)
	  mvn clean
    mvn package -Dgpg.skip=true
		;;
	none)
		echo -e "\n=== skip building, please make sure built connector exist ==="
		;;
  *)
    error_exit "Usage: ./build_image.sh <version> [<path to snowflake repo>] [verify/package/none] . Unknown build method $BUILD_METHOD.  Aborting."
  esac
popd

# get built image name
# only match the first line
SNOWFLAKE_PLUGIN_NAME=$(ls $SNOWFLAKE_PLUGIN_PATH | grep "$SNOWFLAKE_PLUGIN_NAME_REGEX" | head -n 1)
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
docker cp $SNOWFLAKE_PLUGIN_PATH/$SNOWFLAKE_PLUGIN_NAME $DEV_CONTAINER_NAME:$KAFKA_CONNECT_PLUGIN_PATH/$SNOWFLAKE_PLUGIN_NAME || \
docker cp $SNOWFLAKE_PLUGIN_PATH/$SNOWFLAKE_PLUGIN_NAME $DEV_CONTAINER_NAME:$KAFKA_CONNECT_PLUGIN_PATH_5_0_0/$SNOWFLAKE_PLUGIN_NAME

echo -e "\n=== commit the mocified container to snowflake image ==="
docker commit $DEV_CONTAINER_NAME $SNOWFLAKE_DOCKER_IMAGE:$SNOWFLAKE_TAG

# no need to push to docker hub since k8s can use local image
# push the image to our docker hub
# echo -e "\n=== push snowflake image to docker hub ==="
# docker push $SNOWFLAKE_DOCKER_IMAGE:$SNOWFLAKE_TAG

# clean up
echo -e "\n=== delete container $DEV_CONTAINER_NAME ==="
docker rm $DEV_CONTAINER_NAME

# copy the jar to plugin path for apache kafka
APACHE_KAFKA_CONNECT_PLUGIN_PATH="/usr/local/share/kafka/plugins"
mkdir -m 777 -p $APACHE_KAFKA_CONNECT_PLUGIN_PATH || \
sudo mkdir -m 777 -p $APACHE_KAFKA_CONNECT_PLUGIN_PATH 
cp $SNOWFLAKE_PLUGIN_PATH/$SNOWFLAKE_PLUGIN_NAME $APACHE_KAFKA_CONNECT_PLUGIN_PATH || true
echo -e "\n=== copied connector to $APACHE_KAFKA_CONNECT_PLUGIN_PATH ==="