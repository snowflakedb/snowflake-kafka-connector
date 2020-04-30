#!/bin/bash
# tested with helm==3.1.1 and kubectl==1.17

# exit on error
set -e

# error printing function
function error_exit() {
    echo >&2 $1
    exit 1
}

function random-string() {
    cat /dev/urandom | env LC_CTYPE=C tr -cd 'a-z0-9' | head -c 4 
}

# check argument number
if [ ! "$#" -eq 2 ]; then
    error_exit "Usage: ./run_test.sh <version> <path to apache config folder>.  Aborting."
fi

APACHE_VERSION=$1
SNOWFLAKE_APACHE_CONFIG_PATH=$2
SNOWFLAKE_ZOOKEEPER_CONFIG="zookeeper.properties"
SNOWFLAKE_KAFKA_CONFIG="server.properties"
SNOWFLAKE_KAFKA_CONNECT_CONFIG="connect-distributed.properties"

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

REST_TEMPLATE_PATH="./rest_request_template"
REST_GENERATE_PATH="./rest_request_generated"
APACHE_LOG_PATH="./apache_log"

NAME_SALT=$(random-string)
NAME_SALT="_$NAME_SALT"
echo -e "=== Name Salt: $NAME_SALT ==="

# read private_key values from profile.json
SNOWFLAKE_PRIVATE_KEY=$(jq -r ".private_key" $SNOWFLAKE_CREDENTIAL_FILE)
SNOWFLAKE_USER=$(jq -r ".user" $SNOWFLAKE_CREDENTIAL_FILE)
SNOWFLAKE_HOST=$(jq -r ".host" $SNOWFLAKE_CREDENTIAL_FILE)
SNOWFLAKE_SCHEMA=$(jq -r ".schema" $SNOWFLAKE_CREDENTIAL_FILE)
SNOWFLAKE_DATABASE=$(jq -r ".database" $SNOWFLAKE_CREDENTIAL_FILE)
SNOWFLAKE_WAREHOUSE=$(jq -r ".warehouse" $SNOWFLAKE_CREDENTIAL_FILE)

# start apache kafka cluster
DOWNLOAD_URL="https://archive.apache.org/dist/kafka/$APACHE_VERSION/kafka_2.12-$APACHE_VERSION.tgz"
APACHE_FOLDER_NAME="./kafka_2.12-$APACHE_VERSION"

curl $DOWNLOAD_URL --output apache.tgz
tar xzvf apache.tgz > /dev/null 2>&1

mkdir -p $APACHE_LOG_PATH
rm $APACHE_LOG_PATH/zookeeper.log $APACHE_LOG_PATH/kafka.log $APACHE_LOG_PATH/kc.log || true
rm -rf /tmp/kafka-logs /tmp/zookeeper || true

echo -e "\n=== Start Zookeeper ==="
$APACHE_FOLDER_NAME/bin/zookeeper-server-start.sh $SNOWFLAKE_APACHE_CONFIG_PATH/$SNOWFLAKE_ZOOKEEPER_CONFIG > $APACHE_LOG_PATH/zookeeper.log 2>&1 &
sleep 10
echo -e "\n=== Start Kafka ==="
$APACHE_FOLDER_NAME/bin/kafka-server-start.sh $SNOWFLAKE_APACHE_CONFIG_PATH/$SNOWFLAKE_KAFKA_CONFIG > $APACHE_LOG_PATH/kafka.log 2>&1 &
sleep 10
echo -e "\n=== Start Kafka Connect ==="
$APACHE_FOLDER_NAME/bin/connect-distributed.sh $SNOWFLAKE_APACHE_CONFIG_PATH/$SNOWFLAKE_KAFKA_CONNECT_CONFIG > $APACHE_LOG_PATH/kc.log 2>&1 &
sleep 10

trap "pkill -9 -P $$" SIGINT SIGTERM EXIT

# address of kafka
SNOWFLAKE_KAFKA_PORT="9092"
LOCAL_IP="localhost"
SC_PORT=8081
KC_PORT=8083

echo -e "\n=== Clean table stage and pipe ==="
python3 test_verify.py $LOCAL_IP:$SNOWFLAKE_KAFKA_PORT http://$LOCAL_IP:$SC_PORT clean $NAME_SALT

echo -e "\n=== generate sink connector rest reqeuest from $REST_TEMPLATE_PATH ==="
mkdir -p $REST_GENERATE_PATH

for connector_json_file in $REST_TEMPLATE_PATH/*.json; do
    SNOWFLAKE_CONNECTOR_FILENAME=$(echo $connector_json_file | cut -d'/' -f3)
    SNOWFLAKE_CONNECTOR_NAME=$(echo $SNOWFLAKE_CONNECTOR_FILENAME | cut -d'.' -f1)
    SNOWFLAKE_CONNECTOR_NAME="$SNOWFLAKE_CONNECTOR_NAME$NAME_SALT"
    echo -e "\n=== Connector Config JSON: $SNOWFLAKE_CONNECTOR_FILENAME, Connector Name: $SNOWFLAKE_CONNECTOR_NAME ==="

    sed "s|SNOWFLAKE_PRIVATE_KEY|$SNOWFLAKE_PRIVATE_KEY|g" $REST_TEMPLATE_PATH/$SNOWFLAKE_CONNECTOR_FILENAME |
        sed "s|SNOWFLAKE_HOST|$SNOWFLAKE_HOST|g" |
        sed "s|SNOWFLAKE_USER|$SNOWFLAKE_USER|g" |
        sed "s|SNOWFLAKE_DATABASE|$SNOWFLAKE_DATABASE|g" |
        sed "s|SNOWFLAKE_SCHEMA|$SNOWFLAKE_SCHEMA|g" |
        sed "s|CONFLUENT_SCHEMA_REGISTRY|http://$CONFLUENT_SCHEMA_REGISTRY:8081|g" |
        sed "s|SNOWFLAKE_TEST_TOPIC|$SNOWFLAKE_CONNECTOR_NAME|g" |
        sed "s|SNOWFLAKE_CONNECTOR_NAME|$SNOWFLAKE_CONNECTOR_NAME|g" >$REST_GENERATE_PATH/$SNOWFLAKE_CONNECTOR_FILENAME

    # Retry logic to delete the connector
    MAX_RETRY=20 # wait for 10 mins
    retry=0
    while (($retry < $MAX_RETRY)); do
        if curl -X DELETE http://$LOCAL_IP:$KC_PORT/connectors/$SNOWFLAKE_CONNECTOR_NAME; then
            break
        fi
        echo -e "\n=== sleep for 30 secs to wait for kafka connect to accept connection ==="
        sleep 30
        retry=$((retry + 1))
    done
    if [ "$retry" = "$MAX_RETRY" ]; then
        error_exit "\n=== max retry exceeded, kafka connect not ready in 10 mins ==="
    fi

    # Create connector
    curl -X POST -H "Content-Type: application/json" --data @$REST_GENERATE_PATH/$SNOWFLAKE_CONNECTOR_FILENAME http://$LOCAL_IP:$KC_PORT/connectors | jq 'del(.config)'
done

echo -e "\n=== sleep for 10 secs to wait for connectors to load ==="
sleep 10

set +e
# Send test data and verify DB result from Python
python3 test_verify.py $LOCAL_IP:$SNOWFLAKE_KAFKA_PORT http://$LOCAL_IP:$SC_PORT $TEST_SET $NAME_SALT
testError=$?

if [ $testError -ne 0 ]; then
    cat $APACHE_LOG_PATH/zookeeper.log 
    cat $APACHE_LOG_PATH/kafka.log 
    cat $APACHE_LOG_PATH/kc.log
    error_exit "=== test_verify.py failed ==="
fi
