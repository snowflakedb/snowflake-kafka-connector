#!/bin/bash

# find . -name "*.java" -exec gsed -i 's/net\.snowflake\.client\.jdbc\.internal\.fasterxml/com.fasterxml/g' {} \;
# find . -name "*.java" -exec gsed -i 's/net\.snowflake\.ingest\.internal\.com/com/g' {} \;
# find . -name "*.java" -exec gsed -i 's/net\.snowflake\.client\.jdbc\.internal\.apache/org.apache/g' {} \;
# find . -name "*.java" -exec gsed -i 's/net\.snowflake\.client\.jdbc\.internal\.google/com.google/g' {} \;
find . -name "*.java" -exec gsed -i 's/net\.snowflake\.client\.jdbc\.internal\.org\.bouncycastle/org.bouncycastle/g' {} \;
find . -name "*.java" -exec gsed -i 's/net\.snowflake\.ingest\.internal\.com\.github/com.github/g' {} \;
