
class TestExecutor:
    def execution(testSuitList, testSuitEnableList, driver, nameSalt, round=1):
        try:
            for i, test in enumerate(testSuitList):
                if testSuitEnableList[i]:
                    driver.createConnector(test.getConfigFileName(), nameSalt)

            driver.startConnectorWaitTime()

            for r in range(round):
                print(datetime.now().strftime("\n%H:%M:%S "), "=== round {} ===".format(r))
                for i, test in enumerate(testSuitList):
                    if testSuitEnableList[i]:
                        print(datetime.now().strftime("\n%H:%M:%S "),
                              "=== Sending " + test.__class__.__name__ + " data ===")
                        test.send()
                        print(datetime.now().strftime("%H:%M:%S "), "=== Done " + test.__class__.__name__ + " ===", flush=True)


                driver.verifyWaitTime()

                for i, test in enumerate(testSuitList):
                    if testSuitEnableList[i]:
                        print(datetime.now().strftime("\n%H:%M:%S "), "=== Verify " + test.__class__.__name__ + " ===")
                        driver.verifyWithRetry(test.verify, r, test.getConfigFileName())
                        print(datetime.now().strftime("%H:%M:%S "), "=== Passed " + test.__class__.__name__ + " ===", flush=True)

            print(datetime.now().strftime("\n%H:%M:%S "), "=== All test passed ===")
        except Exception as e:
            print(datetime.now().strftime("%H:%M:%S "), e)
            traceback.print_tb(e.__traceback__)
            print(datetime.now().strftime("%H:%M:%S "), "Error: ", sys.exc_info()[0], driver.connectorParameters)
            exit(1)