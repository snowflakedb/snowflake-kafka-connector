#!/usr/bin/env bash


if ! VERSION=$(xmllint --xpath '/*[local-name()="project"]/*[local-name()="version"]/text()' pom.xml)
then
  echo "failed to read version from pom.xml"
  exit 1
fi
echo "version to upload: $VERSION"

if ! API_KEY_SECRET_ID=$(op item list --tags "connectors-nexus-api-key" --format json | jq -r '.[].id')
then
  echo "failed to find required api key in 1password"
  exit 1
fi

if ! USER_PASS=$(op item get $API_KEY_SECRET_ID --format json | jq -r '.fields[] | select(.type=="CONCEALED") | .value')
then
  echo 'failed to read user:password from 1password'
  exit 1
fi

FILE="https://nexus.int.snowflakecomputing.com/repository/connectors/snowflake-kafka-connector-$USER-$VERSION.jar"

echo trying to delete $FILE....
curl -X DELETE \
  -u $USER_PASS \
  $FILE

echo uploading new file to $FILE...
curl --fail \
  --upload-file ./target/snowflake-kafka-connector-$VERSION.jar \
  -u $USER_PASS \
  -w "\nHTTP Status: %{http_code}\n" \
  $FILE

