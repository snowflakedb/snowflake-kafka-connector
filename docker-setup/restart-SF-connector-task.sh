#!/bin/bash

result=
while [[ ${result} != 0 ]]; do
  echo "Waiting for kafka-connect..."
  sleep 1
  curl -XGET http://localhost:8083/connectors >> /dev/null 2>&1
  result=$?
done
getHttpResponse=$(curl --write-out '%{http_code}' --silent --output /dev/null -X GET http://localhost:8083/connectors)
if [ $getHttpResponse != 200 ]; then
    echo "Cant reach Kafka Connect through localhost:8083"
    exit 1
else
    echo 'Connectors Running:'
    kcctl get connectors | awk 'FNR == 3 {print $1}'
    count=0
    for (( ; ; ))
    do
       echo "Restart Count:"$count
       response=$(curl --write-out '%{http_code}' --silent --output /dev/null -X POST http://localhost:8083/connectors/<SF_Connector_Name>/tasks/0/restart)
       echo "Response code:"$response
       echo "Sleeping 100 seconds"
       sleep 100
       ((count=count+1))
    done
    exec "$@"
fi