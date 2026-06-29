#!/bin/sh

# Decrypt the file
# mkdir $HOME/secrets
# --batch to prevent interactive command --yes to assume "yes" for questions
snowflake_deployment=$1

if [ $snowflake_deployment = 'AWS' ]; then
  gpg --quiet --batch --yes --decrypt --passphrase="$SNOWFLAKE_TEST_PROFILE_SECRET" \
  --output profile.json .github/scripts/profile.json.gpg
elif [ $snowflake_deployment = 'GCS' ]; then
  gpg --quiet --batch --yes --decrypt --passphrase="$SNOWFLAKE_TEST_PROFILE_SECRET" \
  --output profile.json .github/scripts/profile_gcs.json.gpg
else
  gpg --quiet --batch --yes --decrypt --passphrase="$SNOWFLAKE_TEST_PROFILE_SECRET" \
  --output profile.json .github/scripts/profile_azure.json.gpg
fi

# Mask credentials in CI logs.
python3 -c "
import json
with open('profile.json') as f:
    profile = json.load(f)
safe_keys = {'host', 'database', 'schema', 'warehouse', 'private_key_passphrase'}
for key, value in profile.items():
    if key not in safe_keys and isinstance(value, str):
        print(f'::add-mask::{value}')
"
