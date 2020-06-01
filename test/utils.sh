#!/bin/bash

create_connectors_with_salt()
{
  REST_TEMPLATE_PATH="./rest_request_template"
  REST_GENERATE_PATH="./rest_request_generated"

  SNOWFLAKE_CREDENTIAL_FILE=$1
  NAME_SALT=$2
  LOCAL_IP=$3
  KC_PORT=$4
  # read private_key values from profile.json
  SNOWFLAKE_PRIVATE_KEY=$(jq -r ".private_key" $SNOWFLAKE_CREDENTIAL_FILE)
  SNOWFLAKE_USER=$(jq -r ".user" $SNOWFLAKE_CREDENTIAL_FILE)
  SNOWFLAKE_HOST=$(jq -r ".host" $SNOWFLAKE_CREDENTIAL_FILE)
  SNOWFLAKE_SCHEMA=$(jq -r ".schema" $SNOWFLAKE_CREDENTIAL_FILE)
  SNOWFLAKE_DATABASE=$(jq -r ".database" $SNOWFLAKE_CREDENTIAL_FILE)

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
}

delete_connectors_with_salt()
{
  REST_TEMPLATE_PATH="./rest_request_template"

  NAME_SALT=$1
  LOCAL_IP=$2
  KC_PORT=$3

  for connector_json_file in $REST_TEMPLATE_PATH/*.json; do
      SNOWFLAKE_CONNECTOR_FILENAME=$(echo $connector_json_file | cut -d'/' -f3)
      SNOWFLAKE_CONNECTOR_NAME=$(echo $SNOWFLAKE_CONNECTOR_FILENAME | cut -d'.' -f1)
      SNOWFLAKE_CONNECTOR_NAME="$SNOWFLAKE_CONNECTOR_NAME$NAME_SALT"
      echo -e "\n=== Connector Config JSON: $SNOWFLAKE_CONNECTOR_FILENAME, Connector Name: $SNOWFLAKE_CONNECTOR_NAME ==="

      curl -X DELETE http://$LOCAL_IP:$KC_PORT/connectors/$SNOWFLAKE_CONNECTOR_NAME
  done
}

record_thread_count()
{
  thread_count_log_dir="thread_count_log"
  mkdir -p $thread_count_log_dir
  thread_count_log="thread_count.log"
  thread_dump_log="thread_dump.log"
  echo "Iteration  CleanerThread#  TotalThread#" > $thread_count_log_dir/$thread_count_log
  iteration=0
  prev_cleaner_thread_count=0
  prev_total_thread_count=0
  while true
  do
    process_number=$(jps | grep "ConnectDistributed" | cut -d " " -f1)
    cleaner_thread_count=$(jstack $process_number | grep startC | wc -l)
    total_thread_count=$(jstack $process_number | grep "\"" | wc -l)
    all_thread=$(jstack $process_number)
    if [ $prev_cleaner_thread_count -ne $cleaner_thread_count ] ||
       [ $prev_total_thread_count -ne $total_thread_count ];
     then
      echo "$all_thread" > $thread_count_log_dir/$iteration$thread_dump_log
      prev_cleaner_thread_count=$cleaner_thread_count
      prev_total_thread_count=$total_thread_count
    fi
    echo "$iteration     $cleaner_thread_count         $total_thread_count" >> $thread_count_log_dir/$thread_count_log
    sleep 5
    iteration=$((iteration + 1))
  done
}