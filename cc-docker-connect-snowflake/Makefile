MVN_PROFILE ?= confluent-nexus
IMAGE_NAME := cc-connect-snowflake-sink
BASE_IMAGE := confluent-docker.jfrog.io/confluentinc/cc-built-base
BASE_VERSION := 0.0.2
BUILD_DOCKER_OVERRIDE := build-mvn-docker
PUSH_DOCKER_OVERRIDE := push-connect-docker
INIT_CI_TARGETS := install-mvn

include ./mk-include/cc-begin.mk
include ./mk-include/cc-semver.mk
include ./mk-include/cc-docker.mk
include ./mk-include/cc-end.mk


.PHONY: cc-connect-snowflake-sink
cc-connect-snowflake-sink:
	[ -d kafka-connect-snowflake ] || git clone git@github.com:confluentinc/snowflake-kafka-connector.git kafka-connect-snowflake
	git -C kafka-connect-snowflake checkout master
	git -C kafka-connect-snowflake pull origin master
	mvn -f kafka-connect-snowflake/pom_confluent.xml -am -Dconnect.component.version=ccloud -Dmaven.test.skip=true -Dgpg.skip clean package
	docker build \
		-t confluentinc/cc-connect-snowflake-sink:$(IMAGE_VERSION) \
		--build-arg BASE_VERSION=$(BASE_VERSION) \
		--build-arg DOCKER_REGISTRY=$(DOCKER_REPO)/ \
		--label SF_VERSION=$$(git -C kafka-connect-snowflake rev-parse --short HEAD) .

.PHONY: build-mvn-docker
build-mvn-docker: docker-pull-base cc-connect-snowflake-sink

.PHONY: push-connect-docker
push-connect-docker: push-docker-latest push-docker-version

.PHONY: install-mvn
install-mvn:
	sudo apt-get update && sudo apt-get -y install maven openjdk-8-jdk
