import datetime
import json
import subprocess
import re

import requests
from time import sleep
from test_suit.base_e2e import BaseE2eTest
from test_suit.test_utils import RetryableError, NonRetryableError


# Reproduces a memory leak observed when the connector is repeatedly paused and resumed.
# The reported issue: after many pause/resume cycles on Confluent Cloud, the connector hits
# MemoryThresholdExceededInContainer (HTTP 429) due to growing memory usage, likely from
# channel reopens in the SSv2 SDK.
#
# This test:
# 1. Runs N pause/resume cycles, each with a batch of records
# 2. Checks connector status/health after each cycle
# 3. Verifies all data was ingested correctly at the end
# 4. Logs connector task states to detect memory-related failures
class TestKcMemoryLeakPauseResume(BaseE2eTest):
    def __init__(self, driver, nameSalt):
        self.driver = driver
        self.nameSalt = nameSalt
        self.fileName = "test_kc_memory_leak_pause_resume"
        self.topic = self.fileName + nameSalt
        self.tableName = self.fileName + nameSalt
        self.connectorName = self.fileName + nameSalt

        # Increased for maximum pressure: 50 cycles x 50,000 records = 2.5M records
        self.pauseResumeCycles = 50
        self.sleepAfterPause = 5
        self.sleepAfterResume = 5

        self.topicNum = 1
        self.partitionNum = 1
        # Increased from 10000 to 50000 to put maximum memory pressure per cycle
        self.recordNum = 50000

        self.expectedsends = 0
        self.failedTasks = []

        # Memory monitoring thresholds (percentage)
        self.memoryThresholds = {
            "warning": 75,
            "high": 85,
            "critical": 90
        }
        self.memoryStats = []
        self.previousMemoryPct = 0

        self.driver.createTopics(self.topic, self.partitionNum, 1)
        self.driver.snowflake_conn.cursor().execute(
            f"create or replace table {self.tableName} (record_metadata variant, column1 varchar)"
        )

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        print(
            "\n=== Starting memory leak reproduction test ==="
        )
        print("Configuration:")
        print("  - Pause/Resume cycles: {}".format(self.pauseResumeCycles))
        print("  - Records per cycle: {:,}".format(self.recordNum))
        print("  - Total records to send: {:,}".format(self.recordNum * (self.pauseResumeCycles + 1)))
        print("  - Memory thresholds: Warning={}%, High={}%, Critical={}%".format(
            self.memoryThresholds["warning"],
            self.memoryThresholds["high"],
            self.memoryThresholds["critical"]
        ))
        print("Looking for: MemoryThresholdExceededInContainer, HTTP 429, memory growth")
        print("=" * 60)

        for cycle in range(self.pauseResumeCycles):
            print(
                "\n--- Cycle {}/{} ---".format(cycle + 1, self.pauseResumeCycles)
            )

            # Send a batch of records
            self.__sendbytes()

            # Pause the connector
            self.driver.pauseConnector(self.connectorName)
            print(
                "Paused connector, waiting {} seconds".format(self.sleepAfterPause)
            )
            sleep(self.sleepAfterPause)

            # Resume the connector
            self.driver.resumeConnector(self.connectorName)
            print(
                "Resumed connector, waiting {} seconds".format(self.sleepAfterResume)
            )
            sleep(self.sleepAfterResume)

            # Check connector health after each cycle
            self.__check_connector_status(cycle + 1)

            # Monitor memory usage after each cycle
            self.__monitor_memory_usage(cycle + 1)

        # Final send after all cycles complete
        self.__sendbytes()
        print(
            "\n=== Completed {} pause/resume cycles, sent {} total batches of {} records ===".format(
                self.pauseResumeCycles, self.expectedsends, self.recordNum
            )
        )

    def verify(self, round):
        goalCount = self.recordNum * self.expectedsends
        res = self.driver.select_number_of_records(self.topic)

        print(
            "Count records in table {}={}. Goal record count: {}".format(
                self.topic, str(res), str(goalCount)
            )
        )

        # Report memory usage summary
        self.__report_memory_summary()

        if self.failedTasks:
            print(
                "WARNING: {} task failures detected during pause/resume cycles: {}".format(
                    len(self.failedTasks), self.failedTasks
                )
            )

        if res < goalCount:
            print("Less records than expected, will retry")
            raise RetryableError()
        elif res > goalCount:
            print("Topic:" + self.topic + " count is more, duplicates detected")
            raise NonRetryableError(
                "Duplication occurred, number of record in table is larger than number of record sent"
            )
        else:
            print(
                "Success - expected number of records found after {} pause/resume cycles".format(
                    self.pauseResumeCycles
                )
            )

    def __report_memory_summary(self):
        """Report summary of memory usage over all cycles."""
        if not self.memoryStats:
            print("\n=== No memory stats collected ===")
            return

        print("\n=== Memory Usage Summary ===")
        print("Total cycles monitored: {}".format(len(self.memoryStats)))

        # Find min, max, and average memory usage
        mem_percentages = [s["memory_percent"] for s in self.memoryStats]
        min_mem = min(mem_percentages)
        max_mem = max(mem_percentages)
        avg_mem = sum(mem_percentages) / len(mem_percentages)

        print("Memory usage (% of container limit):")
        print("  Minimum: {:.1f}%".format(min_mem))
        print("  Maximum: {:.1f}%".format(max_mem))
        print("  Average: {:.1f}%".format(avg_mem))

        # Calculate memory growth
        if len(self.memoryStats) > 1:
            first_mem = self.memoryStats[0]["memory_percent"]
            last_mem = self.memoryStats[-1]["memory_percent"]
            growth = last_mem - first_mem
            print("  Growth: {:.1f}% (from {:.1f}% to {:.1f}%)".format(
                growth, first_mem, last_mem
            ))

            if growth > 10:
                print("  *** SIGNIFICANT MEMORY GROWTH DETECTED ({:.1f}%) ***".format(growth))

        # Check if we hit critical thresholds
        critical_cycles = [s for s in self.memoryStats if s["memory_percent"] >= self.memoryThresholds["critical"]]
        if critical_cycles:
            print("\n!!! CRITICAL: Memory exceeded 90% in {} cycles !!!".format(len(critical_cycles)))
            print("This matches the reported issue: MemoryThresholdExceededInContainer")

        high_cycles = [s for s in self.memoryStats if s["memory_percent"] >= self.memoryThresholds["high"]]
        if high_cycles:
            print("WARNING: Memory exceeded 85% in {} cycles".format(len(high_cycles)))

        # Print cycle-by-cycle breakdown
        print("\nCycle-by-cycle breakdown:")
        for stat in self.memoryStats:
            print("  Cycle {}: {} ({:.1f}%)".format(
                stat["cycle"], stat["memory_usage"], stat["memory_percent"]
            ))
        print("=========================\n")

    def clean(self):
        self.driver.cleanTableStagePipe(self.topic)
        return

    def __sendbytes(self):
        print("Sending {} records".format(str(self.recordNum)))
        key = []
        value = []
        for e in range(self.recordNum):
            value.append(json.dumps({"column1": str(e)}).encode("utf-8"))
        self.driver.sendBytesData(self.topic, value, key, 0)
        self.expectedsends = self.expectedsends + 1
        sleep(2)

    def __monitor_memory_usage(self, cycle):
        """Monitor kafka-connect container memory usage using docker stats."""
        try:
            # Get memory stats from kafka-connect container
            result = subprocess.run(
                ["docker", "stats", "test-kafka-connect", "--no-stream", "--format",
                 "{{.MemUsage}}|{{.MemPerc}}"],
                capture_output=True,
                text=True,
                timeout=5
            )

            if result.returncode == 0:
                output = result.stdout.strip()
                # Format: "1.5GiB / 2.5GiB|60.00%"
                if "|" in output:
                    mem_usage, mem_pct = output.split("|")
                    # Extract percentage value (e.g., "60.00%" -> 60.0)
                    mem_pct_value = float(mem_pct.replace("%", ""))

                    stats = {
                        "cycle": cycle,
                        "memory_usage": mem_usage.strip(),
                        "memory_percent": mem_pct_value
                    }
                    self.memoryStats.append(stats)

                    # Check if we crossed any thresholds
                    threshold_crossed = None
                    if mem_pct_value >= self.memoryThresholds["critical"] and self.previousMemoryPct < self.memoryThresholds["critical"]:
                        threshold_crossed = "CRITICAL (90%)"
                    elif mem_pct_value >= self.memoryThresholds["high"] and self.previousMemoryPct < self.memoryThresholds["high"]:
                        threshold_crossed = "HIGH (85%)"
                    elif mem_pct_value >= self.memoryThresholds["warning"] and self.previousMemoryPct < self.memoryThresholds["warning"]:
                        threshold_crossed = "WARNING (75%)"

                    print(
                        "Cycle {} - Memory: {} ({:.1f}%){}".format(
                            cycle,
                            mem_usage.strip(),
                            mem_pct_value,
                            " *** {} THRESHOLD CROSSED ***".format(threshold_crossed) if threshold_crossed else ""
                        )
                    )

                    # Alert on high memory usage
                    if mem_pct_value >= self.memoryThresholds["critical"]:
                        print(
                            "!!! CRITICAL MEMORY USAGE: {:.1f}% - Approaching reported issue threshold (90%) !!!".format(
                                mem_pct_value
                            )
                        )

                    self.previousMemoryPct = mem_pct_value
            else:
                print(
                    "Cycle {} - Could not get memory stats: {}".format(
                        cycle, result.stderr
                    )
                )
        except subprocess.TimeoutExpired:
            print("Cycle {} - Memory monitoring timed out".format(cycle))
        except Exception as e:
            print("Cycle {} - Error monitoring memory: {}".format(cycle, e))

    def __check_connector_status(self, cycle):
        """Query connector status to detect task failures that may indicate memory issues."""
        try:
            requestURL = "http://{}/connectors/{}/status".format(
                self.driver.kafkaConnectAddress,
                self.connectorName,
            )
            r = requests.get(requestURL, timeout=10)
            if r.ok:
                status = r.json()
                connector_state = status.get("connector", {}).get("state", "UNKNOWN")
                tasks = status.get("tasks", [])
                task_states = [
                    "{}:{}".format(t.get("id"), t.get("state")) for t in tasks
                ]
                print(
                    "Cycle {} - Connector: {}, Tasks: {}".format(
                        cycle, connector_state, task_states
                    )
                )

                # Check for failed tasks (potential memory issue indicator)
                for task in tasks:
                    if task.get("state") == "FAILED":
                        trace = task.get("trace", "")
                        error_info = {
                            "cycle": cycle,
                            "task_id": task.get("id"),
                            "trace_snippet": trace[:500] if trace else "no trace",
                        }
                        self.failedTasks.append(error_info)
                        print(
                            "TASK FAILURE at cycle {}: task={}, trace={}".format(
                                cycle, task.get("id"), trace[:500]
                            )
                        )

                        # Check for memory-related errors specifically
                        memory_error_detected = False
                        if "MemoryThresholdExceededInContainer" in trace:
                            print(
                                "*** EXACT ISSUE REPRODUCED: MemoryThresholdExceededInContainer at cycle {} ***".format(
                                    cycle
                                )
                            )
                            memory_error_detected = True
                        if "429" in trace or "Too Many Requests" in trace:
                            print(
                                "*** HTTP 429 (Too Many Requests) detected at cycle {} ***".format(
                                    cycle
                                )
                            )
                            memory_error_detected = True
                        if "OutOfMemory" in trace or "MemoryThreshold" in trace:
                            print(
                                "*** MEMORY-RELATED ERROR at cycle {} ***".format(
                                    cycle
                                )
                            )
                            memory_error_detected = True

                        if memory_error_detected:
                            print("This matches the original reported issue!")
                            error_info["memory_related"] = True
            else:
                print(
                    "Cycle {} - Failed to get connector status: HTTP {}".format(
                        cycle, r.status_code
                    )
                )
        except Exception as e:
            print("Cycle {} - Error checking connector status: {}".format(cycle, e))
