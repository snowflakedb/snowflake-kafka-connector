#!/bin/bash


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

OSSRH_DEPLOY_SETTINGS_XML="$THIS_DIR/mvn_settings_ossrh_deploy.xml"

cat > $OSSRH_DEPLOY_SETTINGS_XML << SETTINGS.XML
<?xml version="1.0" encoding="UTF-8"?>
<settings xmlns="http://maven.apache.org/SETTINGS/1.0.0"
     xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
     xsi:schemaLocation="http://maven.apache.org/SETTINGS/1.0.0 http://maven.apache.org/xsd/settings-1.0.0.xsd">
  <servers>
    <server>
      <id>ossrh</id>
      <username>$SONATYPE_USER</username>
      <password>$SONATYPE_PWD</password>
    </server>
  </servers>
  <profiles>
      <profile>
        <id>ossrh</id>
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

mvn --settings $OSSRH_DEPLOY_SETTINGS_XML -DskipTests clean deploy
#white source
whitesource/run_whitesource.sh

#confluent release
mvn -f pom_confluent.xml --settings $OSSRH_DEPLOY_SETTINGS_XML -DskipTests clean package
#white source
whitesource/run_whitesource.sh

aws s3 cp target/components/packages/*.zip s3://sfc-jenkins/repository/kafka/
