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

APACHE_VERSION=$1
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
KAFKA_SERVER_JAAS="kafka_server_jaas.conf"

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

TEST_SET="apache"

# check if all required commands are installed
# assume that helm and kubectl are configured

command -v python3 >/dev/null 2>&1 || error_exit "Require python3 but it's not installed.  Aborting."

APACHE_LOG_PATH="./apache_log"

NAME_SALT=$(random-string)
NAME_SALT="_$NAME_SALT"
echo -e "=== Name Salt: $NAME_SALT ==="

# start apache kafka cluster
DOWNLOAD_URL="https://archive.apache.org/dist/kafka/$APACHE_VERSION/kafka_2.12-$APACHE_VERSION.tgz"
APACHE_FOLDER_NAME="./kafka_2.12-$APACHE_VERSION"

rm -rf $APACHE_FOLDER_NAME || true
rm apache.tgz || true

curl $DOWNLOAD_URL --output apache.tgz
tar xzvf apache.tgz > /dev/null 2>&1

mkdir -p $APACHE_LOG_PATH
rm $APACHE_LOG_PATH/zookeeper.log $APACHE_LOG_PATH/kafka.log $APACHE_LOG_PATH/kc.log || true
rm -rf /tmp/kafka-logs /tmp/zookeeper || true

# Fips Jar installation, required for encrypted private key
fipsInstallDirectory=$APACHE_FOLDER_NAME/"libs"
KAFKA_CONNECT_PLUGIN_PATH="/usr/local/share/kafka/plugins"
echo $fipsInstallDirectory
lsCommand=$(ls $fipsInstallDirectory | grep fips | wc -l)
echo $lsCommand

if [ $lsCommand == 0 ]; then
    echo "Installing fips Jars in:"$fipsInstallDirectory
    wget -P $fipsInstallDirectory https://repo1.maven.org/maven2/org/bouncycastle/bcpkix-fips/2.1.8/bcpkix-fips-2.1.8.jar
    wget -P $fipsInstallDirectory https://repo1.maven.org/maven2/org/bouncycastle/bc-fips/2.1.0/bc-fips-2.1.0.jar
    cp $fipsInstallDirectory/bcpkix-fips-2.1.8.jar $KAFKA_CONNECT_PLUGIN_PATH
    cp $fipsInstallDirectory/bc-fips-2.1.0.jar $KAFKA_CONNECT_PLUGIN_PATH
    echo "list KAFKA_CONNECT_PLUGIN_PATH: $KAFKA_CONNECT_PLUGIN_PATH"
    ls $KAFKA_CONNECT_PLUGIN_PATH
    echo "list apache test libs directory: $fipsInstallDirectory"
    ls $fipsInstallDirectory
else
    echo "No need to download Fips Libraries"
fi

# Copy the sample connect log4j properties file to appropriate directory
echo "Copying connect-log4j to apache folder"
cp -fr ./connect-log4j.properties $APACHE_FOLDER_NAME/"config/"

compile_protobuf_converter_and_data $TEST_SET $APACHE_FOLDER_NAME

trap "pkill -9 -P $$" SIGINT SIGTERM EXIT

if [ "$SSL" = "true" ]; then
    echo -e "\n=== using SSL ===="
    ./generate_ssl_key.sh
    SNOWFLAKE_KAFKA_ADDRESS="localhost:9094"
    export KAFKA_OPTS="-Djava.security.auth.login.config=$SNOWFLAKE_APACHE_CONFIG_PATH/$KAFKA_SERVER_JAAS"
else
    SNOWFLAKE_KAFKA_ADDRESS="localhost:9092"
fi
echo -e "\n=== Start Zookeeper ==="
$APACHE_FOLDER_NAME/bin/zookeeper-server-start.sh $SNOWFLAKE_APACHE_CONFIG_PATH/$SNOWFLAKE_ZOOKEEPER_CONFIG > $APACHE_LOG_PATH/zookeeper.log 2>&1 &
sleep 10
echo -e "\n=== Start Kafka ==="
$APACHE_FOLDER_NAME/bin/kafka-server-start.sh $SNOWFLAKE_APACHE_CONFIG_PATH/$SNOWFLAKE_KAFKA_CONFIG > $APACHE_LOG_PATH/kafka.log 2>&1 &
sleep 10
echo -e "\n=== Java version used ==="
java -version
echo -e "\n=== Start Kafka Connect ==="
$APACHE_FOLDER_NAME/bin/connect-distributed.sh $SNOWFLAKE_APACHE_CONFIG_PATH/$SNOWFLAKE_KAFKA_CONNECT_CONFIG > $APACHE_LOG_PATH/kc.log 2>&1 &
sleep 10

# address of kafka
SNOWFLAKE_KAFKA_PORT="9092"
LOCAL_IP="localhost"
SC_PORT=8081
KC_PORT=8083

set +e -x
echo -e "\n=== Clean table stage and pipe ==="
python3 test_verify.py $LOCAL_IP:$SNOWFLAKE_KAFKA_PORT http://$LOCAL_IP:$SC_PORT $LOCAL_IP:$KC_PORT clean $APACHE_VERSION $NAME_SALT $PRESSURE $SSL $SKIP_PROXY $TESTS

#create_connectors_with_salt $SNOWFLAKE_CREDENTIAL_FILE $NAME_SALT $LOCAL_IP $KC_PORT

# Send test data and verify DB result from Python
python3 test_verify.py $LOCAL_IP:$SNOWFLAKE_KAFKA_PORT http://$LOCAL_IP:$SC_PORT $LOCAL_IP:$KC_PORT $TEST_SET $APACHE_VERSION $NAME_SALT $PRESSURE $SSL $SKIP_PROXY $TESTS
testError=$?
# delete_connectors_with_salt $NAME_SALT $LOCAL_IP $KC_PORT
python3 test_verify.py $LOCAL_IP:$SNOWFLAKE_KAFKA_PORT http://$LOCAL_IP:$SC_PORT $LOCAL_IP:$KC_PORT clean $APACHE_VERSION $NAME_SALT $PRESSURE $SSL $SKIP_PROXY $TESTS

if [ $testError -ne 0 ]; then
    RED='\033[0;31m'
    NC='\033[0m' # No Color
    echo -e "${RED} There is error above this line ${NC}"
    cat $APACHE_LOG_PATH/kc.log
    error_exit "=== test_verify.py failed ==="
fi
