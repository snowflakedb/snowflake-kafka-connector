#!/bin/bash
# tested with helm==3.1.1 and kubectl==1.17

# exit on error
set -e

# check argument number
if [ ! "$#" -eq 1 ]; then
    echo >&2 "Usage: ./run_test.sh <path to snowflake helm value>.  Aborting."; exit 1;
fi

SNOWFLAKE_HELM_VALUE=$1

if [ ! -f "$SNOWFLAKE_HELM_VALUE" ]; then
    echo >&2 "Provided snowflake helm value file $SNOWFLAKE_HELM_VALUE does not exist.  Aborting."; exit 1;
fi

# require two environment variables for credentials
if [[ -z "${SNOWFLAKE_CREDENTIAL_FILE}" ]]; then
    echo >&2 "Require environment variable SNOWFLAKE_CREDENTIAL_FILE but it's not set.  Aborting."; exit 1;
fi

if [ ! -f "$SNOWFLAKE_CREDENTIAL_FILE" ]; then
    echo >&2 "Provided SNOWFLAKE_CREDENTIAL_FILE $SNOWFLAKE_CREDENTIAL_FILE does not exist.  Aborting."; exit 1;
fi

if [[ $SNOWFLAKE_HELM_VALUE == *"apache"* ]]; then
    TEST_SET="apache"
else
    TEST_SET="confluent"
fi

# check if all required commands are installed
# assume that helm and kubectl are configured

command -v helm >/dev/null 2>&1 || { echo >&2 "Require helm but it's not installed.  Aborting."; exit 1; }
command -v kubectl >/dev/null 2>&1 || { echo >&2 "Require kubectl but it's not installed.  Aborting."; exit 1; }
command -v jq >/dev/null 2>&1 || { echo >&2 "Require jq but it's not installed.  Aborting."; exit 1; }
command -v minikube >/dev/null 2>&1 || { echo >&2 "Require minikube but it's not installed.  Aborting."; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo >&2 "Require python3 but it's not installed.  Aborting."; exit 1; }

# prefix of all k8s pod names of the deployed kafka cluster
SNOWFLAKE_K8S_NAME="snow"
K8S_ERROR_NEVER_PULL="ErrImageNeverPull"
K8S_ERROR_TYPES="CrashLoopBackOff|Error|Terminating|Pending|Completed|ContainerCreating|ImagePullBackOff|ErrImagePull"

REST_TEMPLATE_PATH="./rest_request_template"
REST_GENERATE_PATH="./rest_request_generated"

SNOWFLAKE_TEST_JSON_TOPIC="testJsonTopic"
SNOWFLAKE_TEST_AVRO_TOPIC="testAvroTopic"
SNOWFLAKE_TEST_AVRO_SR_TOPIC="testAvroSRTopic"
SNOWFLAKE_JSON_CONNECTOR_NAME="sink_test_json"
SNOWFLAKE_JSON_CONNECTOR="$SNOWFLAKE_JSON_CONNECTOR_NAME.json"
SNOWFLAKE_AVRO_CONNECTOR_NAME="sink_test_avro"
SNOWFLAKE_AVRO_CONNECTOR="$SNOWFLAKE_AVRO_CONNECTOR_NAME.json"
SNOWFLAKE_AVRO_SR_CONNECTOR_NAME="sink_test_avro_sr"
SNOWFLAKE_AVRO_SR_CONNECTOR="$SNOWFLAKE_AVRO_SR_CONNECTOR_NAME.json"

# read private_key values from profile.json
SNOWFLAKE_PRIVATE_KEY=$(jq -r ".private_key" $SNOWFLAKE_CREDENTIAL_FILE)
SNOWFLAKE_USER=$(jq -r ".user" $SNOWFLAKE_CREDENTIAL_FILE)
SNOWFLAKE_HOST=$(jq -r ".host" $SNOWFLAKE_CREDENTIAL_FILE)
SNOWFLAKE_SCHEMA=$(jq -r ".schema" $SNOWFLAKE_CREDENTIAL_FILE)
SNOWFLAKE_DATABASE=$(jq -r ".database" $SNOWFLAKE_CREDENTIAL_FILE)
SNOWFLAKE_WAREHOUSE=$(jq -r ".warehouse" $SNOWFLAKE_CREDENTIAL_FILE)

SNOWFLAKE_KAFKA_PORT="31090"

helm repo add confluentinc https://confluentinc.github.io/cp-helm-charts/

helm repo update

# try to uninstall anyway
helm uninstall $SNOWFLAKE_K8S_NAME || true

# Sleep to let all containers terminate
# This sleep does not matter too much, just making sure that all pods started terminating
sleep 3
kubectl get pod
kubectl get svc

helm install $SNOWFLAKE_K8S_NAME confluentinc/cp-helm-charts -f $SNOWFLAKE_HELM_VALUE >/dev/null

# Sleep until all pods are running, we have a way to check whether a pod is running so 
# the sleep 30 here is pretty reliable.

MAX_RETRY=20 # wait for 10 mins
retry=0
MAX_STABLE=4 # if we find all pod are running twice, we can continue
stable=0

while (( $retry < $MAX_RETRY ))
do
    if $(kubectl get pod | awk '{print $3}' | grep -Eq "($K8S_ERROR_NEVER_PULL)") ; then
        echo -e "\n=== local docker image of snowflakedb/kc-dev-build not found, have you run ./build_image.sh ? ==="
        exit 1
    fi

    if ! $(kubectl get pod | awk '{print $3}' | grep -Eq "($K8S_ERROR_TYPES)") ; then
        stable=$(( stable + 1 ))
        if (( $stable > $MAX_STABLE )) ; then
            break
        fi
    fi
    echo -e "\n=== sleep for 30 secs to wait for containers ==="
    sleep 30
    kubectl get pod
    kubectl get svc
    retry=$(( retry + 1 ))
done

if [ "$retry" = "$MAX_RETRY" ]; then
    echo -e "\n=== max retry exceeded, kafka not ready in 10 mins ===" ; exit 1
fi

# Sleep for port forwarding. Added this sleep because sometime even the pod is shown as running
# Kafka Connect is still refusing connection. This is waiting for the server to accept connection
# TIME MIGHT VARY
# TODO: retry if Kafka Connect is refusing connection
echo -e "\n=== sleep 30 sec to stablize server ==="
sleep 30

# generate sink rest request
echo -e "=== generate sink connector rest reqeuest from $REST_TEMPLATE_PATH/$SNOWFLAKE_JSON_CONNECTOR ==="
mkdir -p $REST_GENERATE_PATH
sed "s|SNOWFLAKE_PRIVATE_KEY|$SNOWFLAKE_PRIVATE_KEY|g" $REST_TEMPLATE_PATH/$SNOWFLAKE_JSON_CONNECTOR | \
sed "s|SNOWFLAKE_TEST_JSON_TOPIC|$SNOWFLAKE_TEST_JSON_TOPIC|g" | \
sed "s|SNOWFLAKE_HOST|$SNOWFLAKE_HOST|g" | \
sed "s|SNOWFLAKE_USER|$SNOWFLAKE_USER|g" | \
sed "s|SNOWFLAKE_DATABASE|$SNOWFLAKE_DATABASE|g" | \
sed "s|SNOWFLAKE_SCHEMA|$SNOWFLAKE_SCHEMA|g" | \
sed "s|SNOWFLAKE_JSON_CONNECTOR_NAME|$SNOWFLAKE_JSON_CONNECTOR_NAME|g" > $REST_GENERATE_PATH/$SNOWFLAKE_JSON_CONNECTOR

echo -e "=== generate sink connector rest reqeuest from $REST_TEMPLATE_PATH/$SNOWFLAKE_AVRO_CONNECTOR ==="
sed "s|SNOWFLAKE_PRIVATE_KEY|$SNOWFLAKE_PRIVATE_KEY|g" $REST_TEMPLATE_PATH/$SNOWFLAKE_AVRO_CONNECTOR | \
sed "s|SNOWFLAKE_TEST_AVRO_TOPIC|$SNOWFLAKE_TEST_AVRO_TOPIC|g" | \
sed "s|SNOWFLAKE_HOST|$SNOWFLAKE_HOST|g" | \
sed "s|SNOWFLAKE_USER|$SNOWFLAKE_USER|g" | \
sed "s|SNOWFLAKE_DATABASE|$SNOWFLAKE_DATABASE|g" | \
sed "s|SNOWFLAKE_SCHEMA|$SNOWFLAKE_SCHEMA|g" | \
sed "s|SNOWFLAKE_AVRO_CONNECTOR_NAME|$SNOWFLAKE_AVRO_CONNECTOR_NAME|g" > $REST_GENERATE_PATH/$SNOWFLAKE_AVRO_CONNECTOR

CONFLUENT_SCHEMA_REGISTRY=$(kubectl get service/snow-cp-schema-registry -o jsonpath='{.spec.clusterIP}')

echo -e "=== generate sink connector rest reqeuest from $REST_TEMPLATE_PATH/$SNOWFLAKE_AVRO_SR_CONNECTOR ==="
sed "s|SNOWFLAKE_PRIVATE_KEY|$SNOWFLAKE_PRIVATE_KEY|g" $REST_TEMPLATE_PATH/$SNOWFLAKE_AVRO_SR_CONNECTOR | \
sed "s|SNOWFLAKE_TEST_AVRO_SR_TOPIC|$SNOWFLAKE_TEST_AVRO_SR_TOPIC|g" | \
sed "s|SNOWFLAKE_HOST|$SNOWFLAKE_HOST|g" | \
sed "s|SNOWFLAKE_USER|$SNOWFLAKE_USER|g" | \
sed "s|SNOWFLAKE_DATABASE|$SNOWFLAKE_DATABASE|g" | \
sed "s|SNOWFLAKE_SCHEMA|$SNOWFLAKE_SCHEMA|g" | \
sed "s|SNOWFLAKE_AVRO_SR_CONNECTOR_NAME|$SNOWFLAKE_AVRO_SR_CONNECTOR_NAME|g" | \
sed "s|CONFLUENT_SCHEMA_REGISTRY|http://$CONFLUENT_SCHEMA_REGISTRY:8081|g" > $REST_GENERATE_PATH/$SNOWFLAKE_AVRO_SR_CONNECTOR


kubectl delete svc/$SNOWFLAKE_K8S_NAME-cp-kafka-connect-nodeport || true
kubectl delete svc/$SNOWFLAKE_K8S_NAME-cp-schema-registry-nodeport || true
kubectl expose deployment $SNOWFLAKE_K8S_NAME-cp-kafka-connect --type=NodePort --name=$SNOWFLAKE_K8S_NAME-cp-kafka-connect-nodeport
kubectl expose deployment $SNOWFLAKE_K8S_NAME-cp-schema-registry --type=NodePort --name=$SNOWFLAKE_K8S_NAME-cp-schema-registry-nodeport

kubectl get svc

KC_PORT=$(kubectl get svc | grep -oh "8083:[0-9]*" | grep -oh ":[0-9]*" | cut -d ":" -f 2)
SC_PORT=$(kubectl get svc | grep -oh "8081:[0-9]*" | grep -oh ":[0-9]*" | cut -d ":" -f 2)
K_IP=$(minikube ip)

echo -e "\n=== $K_IP, $KC_PORT, $SC_PORT  ==="

echo -e "\n=== sending DELETE request to Kafka Connect  ==="
# Retry logic
MAX_RETRY=20 # wait for 10 mins
retry=0

while (( $retry < $MAX_RETRY ))
do
    if curl -X DELETE http://$K_IP:$KC_PORT/connectors/$SNOWFLAKE_JSON_CONNECTOR_NAME ; then
        break
    fi
    echo -e "\n=== sleep for 30 secs to wait for kafka connect to accept connection ==="
    sleep 30
    retry=$(( retry + 1 ))
done

if [ "$retry" = "$MAX_RETRY" ]; then
    echo -e "\n=== max retry exceeded, kafka connect not ready in 10 mins ===" ; exit 1
fi


curl -X DELETE http://$K_IP:$KC_PORT/connectors/$SNOWFLAKE_AVRO_CONNECTOR_NAME
curl -X DELETE http://$K_IP:$KC_PORT/connectors/$SNOWFLAKE_AVRO_SR_CONNECTOR_NAME

echo -e "\n=== sending POST request to Kafka Connect  ==="

curl -X POST -H "Content-Type: application/json" --data @$REST_GENERATE_PATH/$SNOWFLAKE_JSON_CONNECTOR http://$K_IP:$KC_PORT/connectors | jq 'del(.config)'

curl -X POST -H "Content-Type: application/json" --data @$REST_GENERATE_PATH/$SNOWFLAKE_AVRO_CONNECTOR http://$K_IP:$KC_PORT/connectors | jq 'del(.config)'

curl -X POST -H "Content-Type: application/json" --data @$REST_GENERATE_PATH/$SNOWFLAKE_AVRO_SR_CONNECTOR http://$K_IP:$KC_PORT/connectors | jq 'del(.config)'


set +e
# Send test data and verify DB result from Python 
python3 test_verify.py $K_IP:$SNOWFLAKE_KAFKA_PORT http://$K_IP:$SC_PORT $SNOWFLAKE_TEST_JSON_TOPIC \
                        $SNOWFLAKE_TEST_AVRO_SR_TOPIC $SNOWFLAKE_TEST_AVRO_TOPIC $TEST_SET
testError=$?

if [ $testError -ne 0 ] ; then
    echo -e "\n=== test_verify.py failed ===" ; exit 1
fi
