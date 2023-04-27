# cc-connect-docker-snowflake

[![Build Status](https://semaphoreci.com/api/v1/projects/4e42b15a-ec7b-486c-a4f2-b0e2d5dee073/2208451/shields_badge.svg)](https://semaphoreci.com/confluent/cc-docker-connect-s3)
![Release](release.svg)

This repo builds a Docker image extended from `cc-docker-connect` that contains the S3 connector.

## Building remotely

Using the [Semaphore CI project](https://semaphoreci.com/confluent/cc-docker-connect-s3), trigger a build on the desired branch (probably `master`).

## Building locally

To build:

```bash
make build
```

To deploy an image:

```bash
make build push-docker-latest
```

