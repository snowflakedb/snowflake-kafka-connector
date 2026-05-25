#!/bin/bash

# exit on error
set -e

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

if [ -z "$GPG_KEY_ID" ]; then
  echo "[ERROR] Key Id not specified!"
  exit 1
fi

if [ -z "$GPG_KEY_PASSPHRASE" ]; then
  echo "[ERROR] GPG passphrase is not specified for $GPG_KEY_ID!"
  exit 1
fi

if [ -z "$GPG_PRIVATE_KEY" ]; then
  echo "[ERROR] GPG private key file is not specified!"
  exit 1
fi

echo "[INFO] Import PGP Key"
if ! gpg --list-secret-key | grep "$GPG_KEY_ID"; then
  gpg --allow-secret-key-import --import "$GPG_PRIVATE_KEY"
fi

CENTRAL_DEPLOY_SETTINGS_XML="$THIS_DIR/mvn_settings_central_deploy.xml"

cat > $CENTRAL_DEPLOY_SETTINGS_XML << SETTINGS.XML
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
     xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
  <servers>
    <server>
      <id>central</id>
      <username>$SONATYPE_USER</username>
      <password>$SONATYPE_PWD</password>
    </server>
  </servers>
  <profiles>
      <profile>
        <id>central</id>
        <activation>
          <activeByDefault>true</activeByDefault>
        </activation>
        <properties>
          <gpg.executable>gpg2</gpg.executable>
          <gpg.keyname>$GPG_KEY_ID</gpg.keyname>
          <gpg.passphrase>$GPG_KEY_PASSPHRASE</gpg.passphrase>
        </properties>
      </profile>
    </profiles>
</settings>
SETTINGS.XML

mvn --settings $CENTRAL_DEPLOY_SETTINGS_XML -DskipTests clean deploy

#confluent release
mvn -f pom_confluent.xml --settings $CENTRAL_DEPLOY_SETTINGS_XML -DskipTests clean package
#white source
# whitesource/run_whitesource.sh

# Produce a SHA-256 checksum and a detached, ASCII-armored GPG signature
# next to the given artifact. Run sha256sum from inside the artifact's
# directory so the checksum file records only the basename, which keeps
# `sha256sum -c` working for downstream consumers.
sign_and_hash_artifact() {
  local artifact="$1"
  local artifact_dir
  local artifact_base
  artifact_dir="$(cd "$(dirname "$artifact")" && pwd)"
  artifact_base="$(basename "$artifact")"

  echo "[INFO] Generating SHA-256 checksum for $artifact_base"
  (
    cd "$artifact_dir"
    sha256sum "$artifact_base" > "${artifact_base}.sha256"
  )

  echo "[INFO] Generating GPG detached signature for $artifact_base"
  local passphrase_file
  umask 077
  passphrase_file="$(mktemp)"
  trap 'rm -f "$passphrase_file"' RETURN
  printf '%s' "$GPG_KEY_PASSPHRASE" > "$passphrase_file"

  gpg --detach-sign --armor \
      --batch --pinentry-mode loopback \
      --passphrase-file "$passphrase_file" \
      --local-user "$GPG_KEY_ID" \
      --output "${artifact}.asc" "$artifact"
}

# Sign and hash every Confluent zip produced by the package step above so
# the .asc and .sha256 sidecars are uploaded alongside the .zip.
for zip in target/components/packages/*.zip; do
  sign_and_hash_artifact "$zip"
done

aws s3 cp target/components/packages/*.zip s3://sfc-eng-jenkins/repository/kafka/
aws s3 cp target/components/packages/ s3://sfc-eng-jenkins/repository/kafka/ \
  --recursive --exclude "*" --include "*.zip.asc" --include "*.zip.sha256"
