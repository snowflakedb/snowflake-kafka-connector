#!/bin/sh
# Follow https://docs.github.com/en/actions/security-guides/encrypted-secrets#limits-for-secrets to encrypt/decrypt
gpg --quiet --batch --yes --decrypt --passphrase="$SNOWFLAKE_TEST_PROFILE_SECRET" \
--output profile_streaming_qa1.json .github/scripts/profile_streaming_qa1.json.gpg
