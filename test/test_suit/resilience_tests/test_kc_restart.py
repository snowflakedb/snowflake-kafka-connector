import datetime

from test_suit.test_utils import RetryableError, NonRetryableError
import json
from time import sleep

class TestKcRestart:
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.nameSalt = nameSalt
        self.fileName = "resilience_kc_restart"
        self.table = self.fileName + nameSalt + "_table"
        self.connectorName = self.fileName + nameSalt
        self.recordNum = 200

        self.driver.snowflake_conn.cursor().execute("Create or replace table {} (PERFORMANCE_STRING STRING)".format(self.table))

        sleep(5)

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        # send data to connector
        self.__send_data()
        
        # restart connector
        self.driver.restartConnector(self.connectorName)

        # send data to connector
        self.__send_data()


    def verify(self, round):
        # verify two sets of data were ingested
        res = self.driver.snowflake_conn.cursor().execute("SELECT count(*) FROM {}".format(self.table)).fetchone()[0]

        if res != self.recordNum * (round + 1) * 2:
            raise RetryableError()

    def clean(self):        
        self.driver.cleanTableStagePipe(self.table)

    def __send_data(self):
        value = []
        for e in range(self.recordNum):
            value.append(json.dumps(
                {'numbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumbernumber': str(e)}
            ).encode('utf-8'))
        self.driver.sendBytesData(self.table, value)