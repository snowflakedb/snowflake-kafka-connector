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

source ./utils.sh

# check argument number
if [ ! "$#" -eq 1 ]; then
    error_exit "Usage: ./run_test.sh <path to snowflake helm value>.  Aborting."
fi

SNOWFLAKE_HELM_VALUE=$1

if [ ! -f "$SNOWFLAKE_HELM_VALUE" ]; then
    error_exit "Provided snowflake helm value file $SNOWFLAKE_HELM_VALUE does not exist.  Aborting."
fi

# require two environment variables for credentials
if [[ -z "${SNOWFLAKE_CREDENTIAL_FILE}" ]]; then
    error_exit "Require environment variable SNOWFLAKE_CREDENTIAL_FILE but it's not set.  Aborting."
fi

if [ ! -f "$SNOWFLAKE_CREDENTIAL_FILE" ]; then
    error_exit "Provided SNOWFLAKE_CREDENTIAL_FILE $SNOWFLAKE_CREDENTIAL_FILE does not exist.  Aborting."
fi

if [[ $SNOWFLAKE_HELM_VALUE == *"apache"* ]]; then
    TEST_SET="apache"
else
    TEST_SET="confluent"
fi

# check if all required commands are installed
# assume that helm and kubectl are configured

command -v helm >/dev/null 2>&1 || error_exit "Require helm but it's not installed.  Aborting."
command -v kubectl >/dev/null 2>&1 || error_exit "Require kubectl but it's not installed.  Aborting."
command -v jq >/dev/null 2>&1 || error_exit "Require jq but it's not installed.  Aborting."
command -v minikube >/dev/null 2>&1 || error_exit "Require minikube but it's not installed.  Aborting."
command -v python3 >/dev/null 2>&1 || error_exit "Require python3 but it's not installed.  Aborting."

# prefix of all k8s pod names of the deployed kafka cluster
SNOWFLAKE_K8S_NAME="snow"
K8S_ERROR_NEVER_PULL="ErrImageNeverPull"
K8S_ERROR_TYPES="CrashLoopBackOff|Error|Terminating|Pending|Completed|ContainerCreating|ImagePullBackOff|ErrImagePull"

NAME_SALT=$(random-string)
NAME_SALT="_$NAME_SALT"
echo -e "=== Name Salt: $NAME_SALT ==="

SNOWFLAKE_KAFKA_PORT="31090"

helm repo add confluentinc https://confluentinc.github.io/cp-helm-charts/

helm repo update

# try to uninstall anyway
helm uninstall $SNOWFLAKE_K8S_NAME || true

# Sleep to let all containers terminate
# This sleep does not matter too much, just making sure that all pods started terminating
sleep 3
kubectl get pod

helm install $SNOWFLAKE_K8S_NAME confluentinc/cp-helm-charts -f $SNOWFLAKE_HELM_VALUE >/dev/null

# Sleep until all pods are running, we have a way to check whether a pod is running so
# the sleep 30 here is pretty reliable.

MAX_RETRY=20 # wait for 10 mins
retry=0
MAX_STABLE=4 # if we find all pod are running twice, we can continue
stable=0

while (($retry < $MAX_RETRY)); do
    if $(kubectl get pod | awk '{print $3}' | grep -Eq "($K8S_ERROR_NEVER_PULL)"); then
        error_exit "=== local docker image of snowflakedb/kc-dev-build not found, have you run ./build_image.sh ? ==="
    fi

    if ! $(kubectl get pod | awk '{print $3}' | grep -Eq "($K8S_ERROR_TYPES)"); then
        stable=$((stable + 1))
        if (($stable > $MAX_STABLE)); then
            break
        fi
    else
      stable=0
    fi
    echo -e "\n=== sleep for 30 secs to wait for containers ==="
    sleep 30
    kubectl get pod
    retry=$((retry + 1))
done

if [ "$retry" = "$MAX_RETRY" ]; then
    error_exit "=== max retry exceeded, kafka not ready in 10 mins ==="
fi

kubectl delete svc/$SNOWFLAKE_K8S_NAME-cp-kafka-connect-nodeport || true
kubectl delete svc/$SNOWFLAKE_K8S_NAME-cp-schema-registry-nodeport || true
kubectl expose deployment $SNOWFLAKE_K8S_NAME-cp-kafka-connect --type=NodePort --name=$SNOWFLAKE_K8S_NAME-cp-kafka-connect-nodeport
kubectl expose deployment $SNOWFLAKE_K8S_NAME-cp-schema-registry --type=NodePort --name=$SNOWFLAKE_K8S_NAME-cp-schema-registry-nodeport

kubectl get svc

KC_PORT=$(kubectl get svc | grep -oh "8083:[0-9]*" | grep -oh ":[0-9]*" | cut -d ":" -f 2)
SC_PORT=$(kubectl get svc | grep -oh "8081:[0-9]*" | grep -oh ":[0-9]*" | cut -d ":" -f 2)
K_IP=$(minikube ip)
CONFLUENT_SCHEMA_REGISTRY=$(kubectl get service/snow-cp-schema-registry -o jsonpath='{.spec.clusterIP}')
echo -e "\n=== K8S ip: $K_IP, Kafka Connect Port: $KC_PORT, Schema Registry Port: $SC_PORT  ==="

echo -e "\n=== Clean table stage and pipe ==="
python3 test_verify.py $K_IP:$SNOWFLAKE_KAFKA_PORT http://$K_IP:$SC_PORT clean $NAME_SALT

create_connectors_with_salt $SNOWFLAKE_CREDENTIAL_FILE $NAME_SALT $K_IP $KC_PORT

echo -e "\n=== sleep for 10 secs to wait for connectors to load ==="
sleep 10

set +e
# Send test data and verify DB result from Python
python3 test_verify.py $K_IP:$SNOWFLAKE_KAFKA_PORT http://$K_IP:$SC_PORT $TEST_SET $NAME_SALT
testError=$?

if [ $testError -ne 0 ]; then
    RED='\033[0;31m'
    NC='\033[0m' # No Color
    echo -e "${RED} There is error above this line ${NC}"
    for pod_name in $(kubectl get pod | awk '{print $1}' | grep "connect"); do
        echo -e "\n===================================================="
        echo -e "=== Log of $pod_name ==="
        echo -e "====================================================\n"
        kubectl logs pod/$pod_name cp-kafka-connect-server | tail -n 200 |
            grep "SF_KAFKA_CONNECTOR\|[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9] [0-9][0-9]:[0-9][0-9]:[0-9][0-9]"
    done
    error_exit "=== test_verify.py failed ==="
fi
