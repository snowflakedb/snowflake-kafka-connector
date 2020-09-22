#!/bin/sh

# Decrypt the file
# mkdir $HOME/secrets
# --batch to prevent interactive command --yes to assume "yes" for questions
snowflake_deployment=$1

if [ $snowflake_deployment = 'aws' ]; then
  gpg --quiet --batch --yes --decrypt --passphrase="$SNOWFLAKE_TEST_PROFILE_SECRET" \
  --output profile.json .github/scripts/profile.json.gpg
else
  gpg --quiet --batch --yes --decrypt --passphrase="$SNOWFLAKE_TEST_PROFILE_SECRET" \
  --output profile.json .github/scripts/profile_azure.json.gpg
fi