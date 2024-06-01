import snowflake.connector
import re
import json
import os
import base64

if __name__ == "__main__":
    with open("config/snowflake.json") as f:
        credentialJson = json.load(f)

    testHost = credentialJson["test"]["url"]
    testUser = credentialJson["test"]["user"]
    testDatabase = credentialJson["test"]["database"]
    testSchema = credentialJson["test"]["schema"]
    testWarehouse = credentialJson["test"]["warehouse"]
    pk = credentialJson["test"]["private_key"]

    pk_decode = base64.b64decode(pk)

    reg = "[^\/]*snowflakecomputing"  # find the account name
    account = re.findall(reg, testHost)
    if len(account) != 1 or len(account[0]) < 20:
        print(
            "Format error in 'host' field at profile.json, expecting account.snowflakecomputing.com:443")

    snowflake_conn = snowflake.connector.connect(
            user=testUser,
            private_key=pk_decode,
            account=account[0][:-19],
            warehouse=testWarehouse,
            database=testDatabase,
            schema=testSchema
        )
    cwd = os.getcwd()
    sql = "get @DO_NOT_DELETE_KAFKA_PERF_TEST_DATA file://{}/data;".format(cwd)
    print(sql)
    snowflake_conn.cursor().execute(sql)

