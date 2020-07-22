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

# Compiles protobuf data and converter, takes no argument
compile_protobuf_converter_and_data()
{
  TEST_SET=$1
  KAFKA_FOLDER_NAME=$2

  # Compile protobuf to java and python class
  pushd "./test_data"
  PRPTOBUF_GENERATED_CODE="protobuf/src/main/java"
  mkdir -p $PRPTOBUF_GENERATED_CODE
  protoc --java_out=$PRPTOBUF_GENERATED_CODE sensor.proto
  protoc --python_out=. sensor.proto
  echo -e "\n=== compiled protobuf ==="
  popd

  # Compile protobuf data to jar
  pushd "./test_data/protobuf"
  mvn clean package -q
  popd

  # Compile protobuf converter to jar
  CONVERTER_FOLDER="kafka-connect-protobuf-converter"
  rm -rf $CONVERTER_FOLDER
  git clone "https://github.com/blueapron/kafka-connect-protobuf-converter"
  pushd $CONVERTER_FOLDER
  git checkout tags/v3.1.0
  mvn clean package -q
  popd


  PROTOBUF_DATA_JAR="./test_data/protobuf/target/kafka-test-protobuf-1.0.0-jar-with-dependencies.jar"
  PROTOBUF_CONVERTER_JAR="./kafka-connect-protobuf-converter/target/kafka-connect-protobuf-converter-3.1.0-jar-with-dependencies.jar"
  if [ "$TEST_SET" == "confluent" ]; then
    TARGET_FOLDER="$KAFKA_FOLDER_NAME/share/java/kafka-serde-tools"

    cp $PROTOBUF_DATA_JAR $TARGET_FOLDER || true
    echo -e "\n=== copied protobuf data to $TARGET_FOLDER ==="

    cp $PROTOBUF_CONVERTER_JAR $TARGET_FOLDER || true
    echo -e "\n=== copied protobuf converter to $TARGET_FOLDER ==="
  else
    TARGET_FOLDER="$KAFKA_FOLDER_NAME/libs"

    export CLASSPATH=$CLASSPATH:$(pwd)/$PROTOBUF_DATA_JAR

    cp $PROTOBUF_CONVERTER_JAR $TARGET_FOLDER || true
    echo -e "\n=== copied protobuf converter to $TARGET_FOLDER ==="
  fi
}