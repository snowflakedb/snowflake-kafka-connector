#!/bin/sh

# Decrypt the file
# mkdir $HOME/secrets
# --batch to prevent interactive command --yes to assume "yes" for questions
mkdir -p test/perf_test/config/
gpg --quiet --batch --yes --decrypt --passphrase="$SNOWFLAKE_TEST_PROFILE_SECRET" \
--output test/perf_test/config/snowflake.json .github/scripts/perf_test.json.gpg