#!/usr/bin/env bash
# Build the SPCS test image, push it to TESTDB_KAFKA.KC_SPCS_CI.IMAGES, and
# print its digest-pinned path for run_spcs_release.py --image. Needs the `spcs`
# snow connection (ci_connection.py). Build logs go to stderr.
set -euo pipefail
tag=${1:?usage: publish_image.sh TAG}
cd "$(dirname "$0")/.."
repository=testdb_kafka/kc_spcs_ci/images/kc-spcs-smoke
registry=$(snow spcs image-registry url -c spcs)
image="$registry/$repository:$tag"
trap 'docker logout "$registry" >/dev/null 2>&1 || true' EXIT
docker build --platform linux/amd64 -f docker/Dockerfile.apache-kafka \
  --build-arg KAFKA_VERSION=4.1.1 --build-arg SCALA_VERSION=2.13 \
  --build-arg JAVA_VERSION=17 -t kc-spcs-apache:4.1.1-java17 . >&2
docker build --platform linux/amd64 -f spcs/Dockerfile \
  --build-arg KAFKA_IMAGE=kc-spcs-apache:4.1.1-java17 -t "$image" . >&2
snow spcs image-registry login -c spcs >&2
docker push "$image" >&2
digest=$(docker inspect --format '{{index .RepoDigests 0}}' "$image")
case "$digest" in
"$registry/$repository@sha256:"*) printf '/%s\n' "${digest#"$registry"/}" ;;
*) echo "Pushed image has no digest in $repository" >&2; exit 1 ;;
esac
