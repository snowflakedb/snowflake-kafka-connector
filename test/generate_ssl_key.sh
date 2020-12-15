#!/bin/bash
set -e

# generate SSL credential on the fly for Kafka encryption.

rm -rf ./crts || true
mkdir -p ./crts
cd crts
keytool -keystore kafka.server.keystore.jks -alias localhost -keyalg RSA -validity 300 -genkey -dname "cn=localhost, ou=IT, o=Snowflake, c=US" -storepass test1234 -keypass test1234
openssl req -new -x509 -keyout ca-key -out ca-cert -days 300 -passin pass:test1234 -passout pass:test1234 -subj "/C=US/ST=CA/L=Stockholm/O=Snowflake/OU=IT/CN=Snowflake/emailAddress=snowflake@snowflake.com"
keytool -keystore kafka.client.truststore.jks -alias CARoot -importcert -file ca-cert -trustcacerts -noprompt -storepass test1234 -keypass test1234
keytool -keystore kafka.server.truststore.jks -alias CARoot -importcert -file ca-cert -trustcacerts -noprompt -storepass test1234 -keypass test1234
keytool -keystore kafka.server.keystore.jks -alias localhost -certreq -file cert-file -storepass test1234 -keypass test1234
openssl x509 -req -CA ca-cert -CAkey ca-key -in cert-file -out cert-signed -days 300 -CAcreateserial -passin pass:test1234
keytool -keystore kafka.server.keystore.jks -alias CARoot -importcert -file ca-cert -noprompt -storepass test1234 -keypass test1234
keytool -keystore kafka.server.keystore.jks -alias localhost -importcert -file cert-signed -noprompt -storepass test1234 -keypass test1234