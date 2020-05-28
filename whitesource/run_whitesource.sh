#!/usr/bin/env bash
#
# Run whitesource for components which need versioning

# SCAN_DIRECTORIES is a comma-separated list (as a string) of file paths which contain all source code and build artifacts for this project
SCAN_DIRECTORIES=$PWD

# PRODUCT_NAME is your team's name or overarching project name
PRODUCT_NAME="snowflake-kafka-connector"

# PROJECT_NAME is your project's name or repo name if your project spans multiple repositories
PROJECT_NAME="snowflake-kafka-connector"

DATE=$(date +'%m-%d-%Y')

if [[ -z "${JOB_BASE_NAME}" ]]; then
   echo "[ERROR] No JOB_BASE_NAME is set. Run this on Jenkins"
   exit 0
fi

# Download the latest whitesource unified agent to do the scanning if there is no existing one
if [ ! -f "wss-unified-agent.jar" ]; then
   curl -LJO https://github.com/whitesource/unified-agent-distribution/releases/latest/download/wss-unified-agent.jar
fi

# whitesource will scan the folder and detect the corresponding configuration
# configuration file wss-generated-file.config will be generated under ${SCAN_DIRECTORIES}
# java -jar wss-unified-agent.jar -detect -d ${SCAN_DIRECTORIES}
# SCAN_CONFIG="${SCAN_DIRECTORIES}/wss-generated-file.config"

# SCAN_CONFIG is the path to your whitesource configuration file
SCAN_CONFIG="whitesource/wss-java-maven-agent.config"

java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
   -c ${SCAN_CONFIG} \
   -project ${PROJECT_NAME} \
   -product ${PRODUCT_NAME} \
   -d ${SCAN_DIRECTORIES} \
   -wss.url https://saas.whitesourcesoftware.com/agent \
   -offline true

if java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
   -c ${SCAN_CONFIG} \
   -project ${PROJECT_NAME} \
   -product ${PRODUCT_NAME} \
   -projectVersion baseline \
   -requestFiles whitesource/update-request.txt \
   -wss.url https://saas.whitesourcesoftware.com/agent ;
then echo "checkPolicies=false" >> ${SCAN_CONFIG} && java -jar wss-unified-agent.jar -apiKey ${WHITESOURCE_API_KEY} \
   -c ${SCAN_CONFIG} \
   -project ${PROJECT_NAME} \
   -product ${PRODUCT_NAME} \
   -projectVersion ${DATE} \
   -requestFiles whitesource/update-request.txt \
   -wss.url https://saas.whitesourcesoftware.com/agent
fi

exit 0