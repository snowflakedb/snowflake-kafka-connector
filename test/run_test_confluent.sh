#!/bin/bash

# exit on error
set -e

# error printing function
function error_exit() {
    echo >&2 $1
    exit 1
}

function random-string() {
    echo $RANDOM$RANDOM | base64 |  cut -c1-7
}

source ./utils.sh

# check argument number
if [ "$#" -lt 2 ] || [ "$#" -gt 6 ] ; then
    error_exit "Usage: ./run_test.sh <version> <path to apache config folder> <pressure> <ssl> [--skipProxy] [--tests=TestStringJson,TestStringAvro,...].  Aborting."
fi

CONFLUENT_VERSION=$1
SNOWFLAKE_APACHE_CONFIG_PATH=$2
if [ "$#" -gt 2 ] ; then
  PRESSURE=$3
else
  PRESSURE="false"
fi 
if [ "$#" -gt 3 ] ; then
  SSL=$4
else
  SSL="false"
fi

if [ "$#" -gt 4 ] && [[ $5 == "--skipProxy" ]] ; then
  SKIP_PROXY=true
else
  SKIP_PROXY=false
fi

tests_pattern="[^(--tests=).*]"
if [ "$#" -gt 5 ] && [[ $6 =~ $tests_pattern ]] ; then
  # skip initial '--tests='
  TESTS=`echo $6 | cut -c9-`
else
  TESTS=""
fi

SNOWFLAKE_ZOOKEEPER_CONFIG="zookeeper.properties"
SNOWFLAKE_KAFKA_CONFIG="server.properties"
SNOWFLAKE_KAFKA_CONNECT_CONFIG="connect-distributed.properties"
SNOWFLAKE_SCHEMA_REGISTRY_CONFIG="schema-registry.properties"
KAFKA_SERVER_JAAS="kafka_server_jaas.conf"
SCHEMA_REGISTRY_JAAS="schema_registry_jaas.conf"

if [ ! -d "$SNOWFLAKE_APACHE_CONFIG_PATH" ]; then
    error_exit "Provided snowflake apache config folder $SNOWFLAKE_APACHE_CONFIG_PATH does not exist.  Aborting."
fi

if [ ! -f "$SNOWFLAKE_APACHE_CONFIG_PATH/$SNOWFLAKE_ZOOKEEPER_CONFIG" ]; then
    error_exit "Zookeeper config $SNOWFLAKE_APACHE_CONFIG_PATH/$SNOWFLAKE_ZOOKEEPER_CONFIG does not exist.  Aborting."
fi

if [ ! -f "$SNOWFLAKE_APACHE_CONFIG_PATH/$SNOWFLAKE_KAFKA_CONFIG" ]; then
    error_exit "Kafka config $SNOWFLAKE_APACHE_CONFIG_PATH/$SNOWFLAKE_KAFKA_CONFIG does not exist.  Aborting."
fi

if [ ! -f "$SNOWFLAKE_APACHE_CONFIG_PATH/$SNOWFLAKE_KAFKA_CONNECT_CONFIG" ]; then
    error_exit "Kafka Connect config $SNOWFLAKE_APACHE_CONFIG_PATH/$SNOWFLAKE_KAFKA_CONNECT_CONFIG does not exist.  Aborting."
fi

# require two environment variables for credentials
if [[ -z "${SNOWFLAKE_CREDENTIAL_FILE}" ]]; then
    error_exit "Require environment variable SNOWFLAKE_CREDENTIAL_FILE but it's not set.  Aborting."
fi

if [ ! -f "$SNOWFLAKE_CREDENTIAL_FILE" ]; then
    error_exit "Provided SNOWFLAKE_CREDENTIAL_FILE $SNOWFLAKE_CREDENTIAL_FILE does not exist.  Aborting."
fi

TEST_SET="confluent"

# check if all required commands are installed
# assume that helm and kubectl are configured

command -v python3 >/dev/null 2>&1 || error_exit "Require python3 but it's not installed.  Aborting."

APACHE_LOG_PATH="./apache_log"

NAME_SALT=$(random-string)
NAME_SALT="_$NAME_SALT"
echo -e "=== Name Salt: $NAME_SALT ==="

# start apache kafka cluster
case $CONFLUENT_VERSION in
	6.2.*)
    DOWNLOAD_URL="https://packages.confluent.io/archive/6.2/confluent-community-$CONFLUENT_VERSION.tar.gz"
    ;;
  7.8.*)
    DOWNLOAD_URL="https://packages.confluent.io/archive/7.8/confluent-community-$CONFLUENT_VERSION.tar.gz"
    ;;
  *)
    error_exit "Usage: ./run_test.sh <version> <path to apache config folder>. Unknown version $CONFLUENT_VERSION Aborting."
esac

CONFLUENT_FOLDER_NAME="./confluent-$CONFLUENT_VERSION"

rm -rf $CONFLUENT_FOLDER_NAME || true
rm apache.tgz || true

echo "Downloading CONFLUENT VERSION using URL: $DOWNLOAD_URL"
curl $DOWNLOAD_URL --output apache.tgz
tar xzvf apache.tgz > /dev/null 2>&1

mkdir -p $APACHE_LOG_PATH
rm $APACHE_LOG_PATH/zookeeper.log $APACHE_LOG_PATH/kafka.log || true
rm $APACHE_LOG_PATH/kc.log || true
rm -rf /tmp/kafka-logs /tmp/zookeeper || true

KAFKA_CONNECT_PLUGIN_PATH="/usr/local/share/kafka/plugins"

# this is the built jar
echo "Built zip file using kafka connect maven plugin:"
ls /tmp/sf-kafka-connect-plugin*
# Plugin path is used by kafka connect to install plugin, in our case, SF Kafka Connector
unzip /tmp/sf-kafka-connect-plugin.zip -d $KAFKA_CONNECT_PLUGIN_PATH
echo "list KAFKA_CONNECT_PLUGIN_PATH: $KAFKA_CONNECT_PLUGIN_PATH"
ls $KAFKA_CONNECT_PLUGIN_PATH

# Copy the sample connect log4j properties file to appropriate directory
echo "Copying connect-log4j.properties file to confluent folder"
cp -fr ./connect-log4j.properties $CONFLUENT_FOLDER_NAME/"etc/kafka/"

compile_protobuf_converter_and_data $TEST_SET $CONFLUENT_FOLDER_NAME

trap "pkill -9 -P $$" SIGINT SIGTERM EXIT

if [ "$SSL" = "true" ]; then
    echo -e "\n=== using SSL ===="
    ./generate_ssl_key.sh
    SNOWFLAKE_KAFKA_ADDRESS="localhost:9094"
    export SCHEMA_REGISTRY_OPTS="-Djava.security.auth.login.config=$SNOWFLAKE_APACHE_CONFIG_PATH/$SCHEMA_REGISTRY_JAAS"
    export KAFKA_OPTS="-Djava.security.auth.login.config=$SNOWFLAKE_APACHE_CONFIG_PATH/$KAFKA_SERVER_JAAS"
else
    SNOWFLAKE_KAFKA_ADDRESS="localhost:9092"
fi
echo -e "\n=== Start Zookeeper ==="
$CONFLUENT_FOLDER_NAME/bin/zookeeper-server-start $SNOWFLAKE_APACHE_CONFIG_PATH/$SNOWFLAKE_ZOOKEEPER_CONFIG > $APACHE_LOG_PATH/zookeeper.log 2>&1 &
sleep 10
echo -e "\n=== Start Kafka ==="
$CONFLUENT_FOLDER_NAME/bin/kafka-server-start $SNOWFLAKE_APACHE_CONFIG_PATH/$SNOWFLAKE_KAFKA_CONFIG > $APACHE_LOG_PATH/kafka.log 2>&1 &
sleep 10
echo -e "\n=== Java version used ==="
java -version
echo -e "\n=== Start Kafka Connect ==="
KAFKA_HEAP_OPTS="-Xms512m -Xmx6g" $CONFLUENT_FOLDER_NAME/bin/connect-distributed $SNOWFLAKE_APACHE_CONFIG_PATH/$SNOWFLAKE_KAFKA_CONNECT_CONFIG > $APACHE_LOG_PATH/kc.log 2>&1 &
sleep 10
echo -e "\n=== Start Schema Registry ==="
$CONFLUENT_FOLDER_NAME/bin/schema-registry-start $SNOWFLAKE_APACHE_CONFIG_PATH/$SNOWFLAKE_SCHEMA_REGISTRY_CONFIG > $APACHE_LOG_PATH/sc.log 2>&1 &
sleep 30

# address of Kafka and KC
LOCAL_IP="localhost"
SC_PORT=8081
KC_PORT=8083

set +e -x
echo -e "\n=== Clean table stage and pipe ==="
python3 test_verify.py $SNOWFLAKE_KAFKA_ADDRESS http://$LOCAL_IP:$SC_PORT $LOCAL_IP:$KC_PORT clean $CONFLUENT_VERSION $NAME_SALT $PRESSURE $SSL $SKIP_PROXY $TESTS

# record_thread_count 2>&1 &
# Send test data and verify DB result from Python
python3 test_verify.py $SNOWFLAKE_KAFKA_ADDRESS http://$LOCAL_IP:$SC_PORT $LOCAL_IP:$KC_PORT $TEST_SET $CONFLUENT_VERSION $NAME_SALT $PRESSURE $SSL $SKIP_PROXY $TESTS
testError=$?

# delete_connectors_with_salt $NAME_SALT $LOCAL_IP $KC_PORT
python3 test_verify.py $SNOWFLAKE_KAFKA_ADDRESS http://$LOCAL_IP:$SC_PORT $LOCAL_IP:$KC_PORT clean $CONFLUENT_VERSION $NAME_SALT $PRESSURE $SSL $SKIP_PROXY $TESTS


##### Following commented code is used to track thread leak
#sleep 100
#
#delete_connectors_with_salt $NAME_SALT $LOCAL_IP $KC_PORT
#NAME_SALT=$(random-string)
#NAME_SALT="_$NAME_SALT"
#echo -e "=== Name Salt: $NAME_SALT ==="
#create_connectors_with_salt $SNOWFLAKE_CREDENTIAL_FILE $NAME_SALT $LOCAL_IP $KC_PORT
#python3 test_verify.py $SNOWFLAKE_KAFKA_ADDRESS http://$LOCAL_IP:$SC_PORT $TEST_SET $NAME_SALT $PRESSURE
#
#sleep 100
#
#delete_connectors_with_salt $NAME_SALT $LOCAL_IP $KC_PORT
#NAME_SALT=$(random-string)
#NAME_SALT="_$NAME_SALT"
#echo -e "=== Name Salt: $NAME_SALT ==="
#create_connectors_with_salt $SNOWFLAKE_CREDENTIAL_FILE $NAME_SALT $LOCAL_IP $KC_PORT
#python3 test_verify.py $SNOWFLAKE_KAFKA_ADDRESS http://$LOCAL_IP:$SC_PORT $TEST_SET $NAME_SALT $PRESSURE

if [ $testError -ne 0 ]; then
    RED='\033[0;31m'
    NC='\033[0m' # No Color
    echo -e "${RED} There is error above this line ${NC}"
    error_exit "=== test_verify.py failed ==="
fi
