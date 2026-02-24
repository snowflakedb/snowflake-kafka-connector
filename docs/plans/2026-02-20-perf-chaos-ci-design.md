# Performance and Chaos Testing for CI/CD

> **For Claude:** Use Claude Opus 4.6 (model ID: claude-opus-4-6) for implementing this design.

**Date:** 2026-02-20
**Status:** Approved
**Goal:** Add performance regression detection and chaos testing to CI/CD with early feedback (5-15 minute runtime)

## Executive Summary

This design adds comprehensive performance and chaos testing to the e2e test suite, running in CI/CD on every PR. Tests use real infrastructure (Docker + toxiproxy + resource limits) with minimal fakes to exercise the actual Snowflake ingest SDK under realistic failure conditions.

**Key Requirements:**
- Detect both correctness issues (data loss/duplication) and performance regressions
- All fault injection types: connector lifecycle, simulated invalidation, network disruption, resource constraints
- Track throughput, latency (p50/p95/p99), resource usage, and error/recovery metrics
- Fail CI on correctness OR performance regression (>10% degradation)
- Runtime: 5-15 minutes per test run
- Extend existing `end-to-end-stress.yml` workflow

**Production Targets (for context):**
- Throughput: 10GB/s
- Latency: 5-10s end-to-end (p99)
- Stability: Zero data loss/duplication, equal or exceed KCv3

**CI Testing Strategy:**
- CI won't reach production throughput (Docker/GHA limits)
- Instead: detect **relative regressions** (>10% drop from baseline = CI failure)
- Latency targets ARE testable in CI with real Snowflake (p99 < 15s with buffer)

---

## Architecture

### Overall Design

Tests use real Kafka Connect + real Snowflake ingest SDK. Fault injection happens at the infrastructure layer (network, resources, containers) to induce authentic SDK exceptions and recovery behavior.

**Core Principle:** Minimize fakes. Use Docker infrastructure manipulation to cause real failures that trigger actual SDK error paths.

### Infrastructure Stack

```
┌─────────────────────────────────────────────────┐
│  toxiproxy (:8474 admin, :20443 HTTPS proxy)    │
│  ↓ intercepts all Snowflake traffic             │
├─────────────────────────────────────────────────┤
│  kafka-connect (with memory/CPU limits)         │
│  ↓ connector runs with real SDK                 │
├─────────────────────────────────────────────────┤
│  kafka + zookeeper + schema-registry            │
├─────────────────────────────────────────────────┤
│  test-runner (Python + metrics collector)       │
│  ↓ controls toxiproxy, monitors JMX             │
└─────────────────────────────────────────────────┘
                    ↓
            Real Snowflake
```

**How real failures are induced:**
- **Channel invalidation:** Network partition via toxiproxy → SDK can't reach Snowflake → channels become stale/invalid
- **Memory backpressure:** Docker memory limits → SDK buffers fill → `MemoryThresholdExceeded` exception
- **Rate limiting:** Bandwidth throttling → SDK timeouts → `appendRow()` throws SFException
- **Connection failures:** TCP RST packets → SDK sees incomplete commit → channel recovery triggered

### Components

**1. Chaos E2E Tests** (`test/test_suit/chaos_tests/`)
- Python test classes extending `BaseChaosTest`
- Use toxiproxy API to inject faults during test execution
- Verify correctness (no data loss/duplication) + performance (no regression)

**2. Performance E2E Tests** (`test/test_suit/performance_tests/`)
- Baseline throughput/latency tests with no chaos
- Establish performance characteristics under normal conditions
- Smaller scale than existing stress tests (for CI runtime)

**3. Metrics Collection Framework** (`test/chaos_framework/`)
- JMX scraping from Kafka Connect container
- End-to-end latency tracking (Kafka produce timestamp → Snowflake visibility)
- Resource monitoring (memory, CPU, GC stats)
- Baseline comparison and regression detection

**4. Baseline Management**
- `test/baselines/chaos-tests.json` - expected behavior under chaos
- `test/baselines/performance-tests.json` - throughput/latency thresholds
- Git-committed, updated via PR when legitimate improvements occur

---

## Test Scenarios

### Category 1: Channel Invalidation Tests

**Test: `TestChannelInvalidationOnAppendRow`**
- **Scenario:** Send 10K records → inject network partition via toxiproxy (drop all packets for 30s) → restore network
- **Expected Behavior:** SDK's `appendRow()` throws exception → connector closes old channel → reopens new channel → resets offset → resumes ingestion
- **Assertions:**
  - All 10K records in Snowflake (no data loss)
  - No duplicates (exactly-once semantics maintained)
  - Recovery time < 30s
- **Metrics:** Time to detect failure, time to recover, total retries

**Test: `TestChannelInvalidationDuringFlush`**
- **Scenario:** Send records → pause connector → inject connection reset during flush → resume connector
- **Expected Behavior:** SDK sees incomplete commit → offset token fetch fails → channel reopens with correct offset
- **Assertions:**
  - No data loss or duplication
  - Offset correctly recovered from Snowflake
- **Metrics:** Recovery time, offset reset latency

### Category 2: Backpressure and Rate Limiting Tests

**Test: `TestMemoryBackpressure`**
- **Scenario:** Set Kafka Connect container memory limit to 1GB → send high-volume data (50MB/sec) to saturate buffers
- **Expected Behavior:** SDK throws `MemoryThresholdExceeded` → connector retries with 5s backoff → eventually succeeds
- **Assertions:**
  - Connector retries (not fails immediately)
  - No data loss once memory pressure relieved
- **Metrics:** Retry count, throughput degradation, GC pressure, heap usage

**Test: `TestBandwidthThrottling`**
- **Scenario:** Toxiproxy limits bandwidth to Snowflake to 1MB/sec → send data faster than bandwidth allows
- **Expected Behavior:** SDK experiences slow writes, potential timeouts → connector handles gracefully with retries
- **Assertions:**
  - Connector doesn't crash or lose data
  - All data arrives eventually
- **Metrics:** Throughput achieved (should match 1MB/s limit), SDK timeout count, buffer utilization

### Category 3: Sequential Failure Tests

**Test: `TestAppendRowFailureThenOffsetTokenFailure`**
- **Scenario:** Send batch → inject network failure during `appendRow()` → connector recovers → immediately inject failure during `getOffsetToken()` on new channel
- **Expected Behavior:** Connector recovers from both failures sequentially, maintains correct offset throughout
- **Assertions:**
  - No data loss or duplication despite two consecutive failures
  - Offset remains correct across both recoveries
- **Metrics:** Total recovery time, retry attempts per API call

### Category 4: Performance Baseline Tests (No Chaos)

**Test: `TestBaselineThroughput`**
- **Scenario:** Send 1M records (100MB total) with no chaos injection
- **Expected Behavior:** Steady-state ingestion with baseline throughput/latency
- **Assertions:**
  - Throughput ≥ baseline (within 10% tolerance)
  - p99 latency ≤ 15s (target 10s + 5s CI buffer)
- **Metrics:** Records/sec, MB/sec, latency distribution (p50/p95/p99)

**Test: `TestBaselineUnderLoad`**
- **Scenario:** 10 topics × 3 partitions, 10K records each (300K total records)
- **Expected Behavior:** No throughput degradation vs single topic, linear scaling with partition count
- **Assertions:**
  - Per-partition throughput matches single-partition baseline
  - No excessive memory/CPU usage
- **Metrics:** Per-partition throughput, task CPU/memory usage, connector-level throughput

### Category 5: Resource Constraint Tests

**Test: `TestCpuStarvation`**
- **Scenario:** Limit Kafka Connect to 0.5 CPU cores → send normal data volume
- **Expected Behavior:** Connector remains functional, throughput degrades gracefully (no crash)
- **Assertions:**
  - Connector processes all data (slower but no loss)
  - No task failures or restarts
- **Metrics:** Throughput reduction %, task scheduling latency

**Test: `TestOomRecovery`**
- **Scenario:** Set memory limit that causes occasional OOM
- **Expected Behavior:** Connect restarts task → recovers from last committed offset → continues processing
- **Assertions:**
  - Task restarts (expected)
  - No data loss or duplication after restart
  - Offset recovery correct
- **Metrics:** OOM frequency, recovery time, data integrity check

---

## Test Scale Guidelines

Sized for 5-15 minute CI runtime window:

- **Small:** 1 topic, 1 partition, 10K records (~30s)
- **Medium:** 3 topics, 3 partitions, 30K records (~2 min)
- **Large:** 10 topics, 3 partitions, 100K records (~5 min)

**Estimated Total Runtime:**
- 2 channel invalidation tests (medium): 4 min
- 2 backpressure tests (small): 1 min
- 1 sequential failure test (small): 30s
- 2 performance baselines (large): 8 min
- 2 resource constraint tests (medium): 4 min
- **Total: ~17.5 minutes** (slightly over target, can trim scenarios)

**Optimization:** Run performance baselines only (no chaos), then run subset of chaos tests in rotation to stay under 15 min.

---

## Metrics and Baselines

### Metrics Tracked

**Performance Metrics:**
- `throughput_mbps`: MB/sec ingested (compare to baseline for regression detection)
- `throughput_records_per_sec`: Records/sec ingested
- `latency_p50_ms`, `latency_p95_ms`, `latency_p99_ms`: End-to-end latency (produce → Snowflake visible)
- `jvm_heap_used_mb`: Heap memory usage
- `jvm_gc_time_ms`: Total GC time during test
- `jvm_gc_count`: Number of GC events

**Stability Metrics:**
- `records_sent`: Total records sent to Kafka
- `records_in_snowflake`: Total records in Snowflake (MUST equal records_sent for correctness)
- `recovery_time_seconds`: Time from fault injection to full recovery
- `retry_count`: SDK retry attempts during chaos
- `task_restarts`: Number of task failures (should be 0 except OOM test)
- `data_loss`: records_sent - records_in_snowflake (MUST be 0)
- `duplicates`: Duplicate records detected via unique ID check (MUST be 0)

### Baseline Files

**`test/baselines/performance-tests.json`:**
```json
{
  "TestBaselineThroughput": {
    "throughput_mbps": 120.5,
    "throughput_records_per_sec": 15000,
    "latency_p50_ms": 2500,
    "latency_p95_ms": 6000,
    "latency_p99_ms": 8500,
    "jvm_heap_used_mb": 450,
    "jvm_gc_time_ms": 1200
  },
  "TestBaselineUnderLoad": {
    "throughput_mbps": 110.0,
    "throughput_records_per_sec": 14500,
    "latency_p99_ms": 9000,
    "jvm_heap_used_mb": 650
  }
}
```

**`test/baselines/chaos-tests.json`:**
```json
{
  "TestChannelInvalidationOnAppendRow": {
    "recovery_time_seconds": 12,
    "retry_count": 3,
    "task_restarts": 0,
    "data_loss": 0,
    "duplicates": 0
  },
  "TestMemoryBackpressure": {
    "recovery_time_seconds": 8,
    "retry_count": 5,
    "task_restarts": 0,
    "data_loss": 0,
    "duplicates": 0,
    "jvm_heap_used_mb": 950
  }
}
```

### Regression Detection Logic

**CI fails if:**
- **Correctness failure:** `data_loss > 0` OR `duplicates > 0` (hard failure)
- **Performance regression:** Throughput drops >10% from baseline OR latency increases >10% from baseline
- **Stability regression:** `recovery_time_seconds` increases >20% from baseline OR unexpected `task_restarts > 0`

**Updating baselines:**
1. PR introduces performance improvement
2. CI fails with "throughput 15% higher than baseline"
3. Developer runs: `./test/update_baseline.sh <test-class-name>`
4. Script updates baseline JSON with new metrics
5. Commit updated baseline in same PR
6. Reviewer verifies improvement is legitimate

---

## Technical Implementation

### Docker Compose Changes

**New file: `test/docker/docker-compose.chaos.yml`**
```yaml
version: '3.8'

services:
  toxiproxy:
    image: ghcr.io/shopify/toxiproxy:2.9.0
    container_name: test-toxiproxy
    ports:
      - "8474:8474"   # Admin API
      - "20443:20443" # HTTPS proxy for Snowflake traffic
    networks:
      - kafka-network
    healthcheck:
      test: ["CMD", "wget", "--spider", "-q", "http://localhost:8474/version"]
      interval: 5s
      timeout: 3s
      retries: 10

  kafka-connect:
    # ... existing config from docker-compose.confluent.yml ...
    environment:
      # Route Snowflake traffic through toxiproxy
      HTTPS_PROXY: "http://toxiproxy:20443"
      # Existing env vars...
    depends_on:
      toxiproxy:
        condition: service_healthy
    # Resource limits for backpressure tests
    deploy:
      resources:
        limits:
          memory: 2g
          cpus: '2.0'
    networks:
      - kafka-network
```

**Usage:**
```bash
# Existing e2e tests (no chaos)
docker compose -f docker-compose.base.yml -f docker-compose.confluent.yml up

# Chaos tests
docker compose -f docker-compose.base.yml -f docker-compose.confluent.yml -f docker-compose.chaos.yml up
```

### Toxiproxy Client

**New file: `test/chaos_framework/toxiproxy_client.py`**
```python
import requests
import time

class ToxiproxyClient:
    def __init__(self, admin_url="http://localhost:8474"):
        self.admin_url = admin_url
        self.proxy_name = "snowflake"

    def create_snowflake_proxy(self, snowflake_host):
        """
        Create proxy: localhost:20443 -> snowflake_host:443
        All Snowflake traffic routes through this proxy
        """
        proxy = {
            "name": self.proxy_name,
            "listen": "0.0.0.0:20443",
            "upstream": f"{snowflake_host}:443",
            "enabled": True
        }
        resp = requests.post(f"{self.admin_url}/proxies", json=proxy)
        resp.raise_for_status()

    def add_latency(self, ms, jitter_ms=0):
        """Add network latency (simulates slow network)"""
        toxic = {
            "type": "latency",
            "attributes": {"latency": ms, "jitter": jitter_ms}
        }
        self._add_toxic(toxic)

    def add_bandwidth_limit(self, rate_kbps):
        """Throttle bandwidth (simulates slow connection)"""
        toxic = {
            "type": "bandwidth",
            "attributes": {"rate": rate_kbps}
        }
        self._add_toxic(toxic)

    def partition_network(self):
        """Drop all packets (simulates network partition)"""
        toxic = {
            "type": "timeout",
            "attributes": {"timeout": 0}  # Drop immediately
        }
        self._add_toxic(toxic)

    def reset_connection(self):
        """Send TCP RST packets (simulates connection failure)"""
        toxic = {
            "type": "reset_peer",
            "attributes": {"timeout": 0}
        }
        self._add_toxic(toxic)

    def remove_all_toxics(self):
        """Clear all fault injection (restore normal operation)"""
        resp = requests.get(f"{self.admin_url}/proxies/{self.proxy_name}/toxics")
        toxics = resp.json()
        for toxic in toxics:
            requests.delete(f"{self.admin_url}/proxies/{self.proxy_name}/toxics/{toxic['name']}")

    def _add_toxic(self, toxic_config):
        toxic_config["name"] = f"toxic_{int(time.time() * 1000)}"
        toxic_config["toxicity"] = 1.0  # Apply to 100% of connections
        resp = requests.post(
            f"{self.admin_url}/proxies/{self.proxy_name}/toxics",
            json=toxic_config
        )
        resp.raise_for_status()
```

### Metrics Collection Framework

**New file: `test/chaos_framework/metrics_collector.py`**
```python
import requests
import time
import statistics
from collections import defaultdict

class MetricsCollector:
    def __init__(self, connect_url="http://localhost:8083", snowflake_conn=None):
        self.connect_url = connect_url
        self.snowflake_conn = snowflake_conn
        self.latencies = []
        self.start_time = None
        self.fault_injected_time = None
        self.fault_cleared_time = None

    def mark_test_start(self):
        """Mark test start time"""
        self.start_time = time.time()

    def mark_fault_injected(self):
        """Mark when fault was injected"""
        self.fault_injected_time = time.time()

    def mark_fault_cleared(self):
        """Mark when fault was cleared"""
        self.fault_cleared_time = time.time()

    def scrape_jmx_metrics(self, connector_name):
        """
        Scrape JMX metrics from Kafka Connect REST API
        Returns: dict with heap usage, GC stats, task metrics
        """
        # Connect exposes some metrics via REST API
        # For full JMX, would need JMX exporter or Jolokia agent
        resp = requests.get(f"{self.connect_url}/connectors/{connector_name}/status")
        status = resp.json()

        # Basic metrics from Connect API
        metrics = {
            "task_count": len(status.get("tasks", [])),
            "task_restarts": sum(1 for t in status.get("tasks", []) if t["state"] == "FAILED")
        }

        # Would enhance with JMX scraping for:
        # - java.lang:type=Memory (heap usage)
        # - java.lang:type=GarbageCollector (GC stats)
        # - kafka.connect:type=sink-task-metrics (throughput)

        return metrics

    def track_record_latency(self, produce_timestamp_ms, topic):
        """
        Track end-to-end latency for a single record
        produce_timestamp_ms: timestamp when record was produced to Kafka
        """
        # Query Snowflake for record with this timestamp
        query = f"""
            SELECT TO_NUMBER(RECORD_METADATA:CreateTime) as create_time
            FROM {topic}
            WHERE TO_NUMBER(RECORD_METADATA:CreateTime) = {produce_timestamp_ms}
            LIMIT 1
        """

        # Poll until record appears (with timeout)
        max_wait = 30
        start = time.time()
        while time.time() - start < max_wait:
            result = self.snowflake_conn.cursor().execute(query).fetchone()
            if result:
                snowflake_time_ms = int(time.time() * 1000)
                latency_ms = snowflake_time_ms - produce_timestamp_ms
                self.latencies.append(latency_ms)
                return latency_ms
            time.sleep(1)

        raise Exception(f"Record with timestamp {produce_timestamp_ms} never appeared in Snowflake")

    def compute_latency_percentiles(self):
        """Calculate p50, p95, p99 from tracked latencies"""
        if not self.latencies:
            return {}

        sorted_latencies = sorted(self.latencies)
        return {
            "latency_p50_ms": statistics.quantiles(sorted_latencies, n=100)[49],
            "latency_p95_ms": statistics.quantiles(sorted_latencies, n=100)[94],
            "latency_p99_ms": statistics.quantiles(sorted_latencies, n=100)[98],
            "latency_min_ms": min(sorted_latencies),
            "latency_max_ms": max(sorted_latencies)
        }

    def compute_recovery_time(self):
        """Calculate time from fault injection to recovery"""
        if not self.fault_injected_time or not self.fault_cleared_time:
            return None
        return self.fault_cleared_time - self.fault_injected_time

    def compare_to_baseline(self, current_metrics, baseline_file, test_name):
        """
        Compare current run metrics to baseline
        Returns: (passed: bool, details: str)
        """
        import json

        with open(baseline_file) as f:
            baselines = json.load(f)

        if test_name not in baselines:
            return (True, f"No baseline for {test_name}, skipping comparison")

        baseline = baselines[test_name]
        failures = []

        # Check each metric with 10% tolerance
        for metric, baseline_value in baseline.items():
            if metric not in current_metrics:
                continue

            current_value = current_metrics[metric]

            # For throughput, higher is better
            if "throughput" in metric:
                if current_value < baseline_value * 0.9:
                    pct_change = ((current_value - baseline_value) / baseline_value) * 100
                    failures.append(f"{metric}: {current_value:.2f} vs baseline {baseline_value:.2f} ({pct_change:+.1f}%)")

            # For latency/recovery time, lower is better
            elif "latency" in metric or "recovery" in metric:
                if current_value > baseline_value * 1.1:
                    pct_change = ((current_value - baseline_value) / baseline_value) * 100
                    failures.append(f"{metric}: {current_value:.2f} vs baseline {baseline_value:.2f} ({pct_change:+.1f}%)")

            # For correctness metrics, must be exact
            elif metric in ["data_loss", "duplicates"]:
                if current_value != baseline_value:
                    failures.append(f"{metric}: {current_value} (MUST be {baseline_value})")

        if failures:
            return (False, "Performance regression detected:\n" + "\n".join(failures))
        else:
            return (True, "All metrics within baseline thresholds")
```

### Enhanced Python Test Framework

**New file: `test/test_suit/base_chaos_test.py`**
```python
from test_suit.base_e2e import BaseE2eTest
from test_suit.test_utils import NonRetryableError
from chaos_framework.toxiproxy_client import ToxiproxyClient
from chaos_framework.metrics_collector import MetricsCollector
import time

class BaseChaosTest(BaseE2eTest):
    """Base class for chaos tests with fault injection and metrics"""

    def __init__(self, driver, nameSalt):
        super().__init__(driver, nameSalt)
        self.toxiproxy = ToxiproxyClient()
        self.metrics = MetricsCollector(snowflake_conn=driver.snowflake_conn)
        self.records_sent = 0

    def setup(self):
        """Initialize toxiproxy proxy for Snowflake traffic"""
        snowflake_host = self._get_snowflake_host()
        self.toxiproxy.create_snowflake_proxy(snowflake_host)
        self.metrics.mark_test_start()

    def inject_fault(self):
        """Override in subclass to define chaos scenario"""
        raise NotImplementedError("Subclass must implement inject_fault()")

    def send_records(self, count, partition=0):
        """Send records with timestamp tracking for latency measurement"""
        import json
        values = []
        for i in range(count):
            timestamp_ms = int(time.time() * 1000)
            record = {
                'data': f'value_{i}',
                'produce_timestamp': timestamp_ms
            }
            values.append(json.dumps(record).encode('utf-8'))

        self.driver.sendBytesData(self.topic, values, partition=partition)
        self.records_sent += count

    def verify_correctness(self):
        """Assert no data loss or duplication (HARD REQUIREMENT)"""
        actual_count = self.driver.select_number_of_records(self.topic)

        if actual_count < self.records_sent:
            data_loss = self.records_sent - actual_count
            raise NonRetryableError(
                f"DATA LOSS: Expected {self.records_sent} records, found {actual_count} "
                f"({data_loss} records lost)"
            )

        if actual_count > self.records_sent:
            duplicates = actual_count - self.records_sent
            raise NonRetryableError(
                f"DUPLICATES: Expected {self.records_sent} records, found {actual_count} "
                f"({duplicates} duplicate records)"
            )

        print(f"✓ Correctness verified: {actual_count} records, no data loss or duplication")

    def verify_performance(self, baseline_file):
        """Compare metrics to baseline, fail if regression detected"""
        # Collect current run metrics
        current_metrics = {
            "records_sent": self.records_sent,
            "records_in_snowflake": self.driver.select_number_of_records(self.topic),
            "data_loss": 0,  # Already verified in verify_correctness()
            "duplicates": 0,
            **self.metrics.compute_latency_percentiles()
        }

        recovery_time = self.metrics.compute_recovery_time()
        if recovery_time:
            current_metrics["recovery_time_seconds"] = recovery_time

        # Compare to baseline
        test_name = self.__class__.__name__
        passed, details = self.metrics.compare_to_baseline(
            current_metrics, baseline_file, test_name
        )

        if not passed:
            raise NonRetryableError(f"Performance regression:\n{details}")

        print(f"✓ Performance validated: {details}")

    def _get_snowflake_host(self):
        """Extract Snowflake host from connection"""
        # Would parse from driver.snowflake_conn or profile.json
        return "account.snowflakecomputing.com"
```

**Example test implementation:**

**New file: `test/test_suit/chaos_tests/test_channel_invalidation_append_row.py`**
```python
from test_suit.base_chaos_test import BaseChaosTest
import time

class TestChannelInvalidationOnAppendRow(BaseChaosTest):
    """
    Test channel invalidation during appendRow operation.
    Simulates network partition that causes channel to become invalid.
    """

    def __init__(self, driver, nameSalt):
        super().__init__(driver, nameSalt)
        self.fileName = "test_channel_invalidation_append_row"
        self.topic = self.fileName + nameSalt
        self.connectorName = self.fileName + nameSalt

        # Create topic and table
        self.driver.createTopics(self.topic, partitionNum=1, replicationNum=1)
        self.driver.create_table(self.topic)

    def getConfigFileName(self):
        return self.fileName + ".json"

    def send(self):
        # Phase 1: Send first batch of records (normal operation)
        print("Phase 1: Sending 5000 records (normal operation)")
        self.send_records(count=5000)
        time.sleep(5)  # Let some records flush

        # Phase 2: Inject network partition
        print("Phase 2: Injecting network partition for 30 seconds")
        self.metrics.mark_fault_injected()
        self.toxiproxy.partition_network()

        # Try to send more records during partition (will buffer in Kafka)
        print("Phase 2: Sending 5000 records during partition")
        self.send_records(count=5000)

        # Keep partition active for 30 seconds
        time.sleep(30)

        # Phase 3: Restore network
        print("Phase 3: Restoring network")
        self.toxiproxy.remove_all_toxics()
        self.metrics.mark_fault_cleared()

        # Wait for connector to recover
        time.sleep(10)

    def verify(self, round):
        # Verify correctness (no data loss/duplication)
        self.verify_correctness()

        # Verify performance against baseline
        self.verify_performance('test/baselines/chaos-tests.json')

        # Additional checks specific to this test
        recovery_time = self.metrics.compute_recovery_time()
        if recovery_time and recovery_time > 30:
            raise NonRetryableError(
                f"Recovery took {recovery_time:.1f}s, exceeds 30s threshold"
            )

        print(f"✓ Test passed: Recovery time {recovery_time:.1f}s")

    def clean(self):
        self.driver.cleanTableStagePipe(self.connectorName, self.topic, partitionNum=1)
        self.toxiproxy.remove_all_toxics()
```

### CI Integration

**Update `.github/workflows/end-to-end-stress.yml`:**
```yaml
name: Kafka Connector stress test

on:
  push:
    branches: [ master ]
  pull_request:
    branches: '**'
  workflow_dispatch:

jobs:
  build_and_test:
    runs-on: ubuntu-22.04
    name: ${{ matrix.test_type }}, ${{ matrix.platform }} ${{ matrix.platform_version }}, ${{ matrix.snowflake_cloud }}
    strategy:
      fail-fast: false
      matrix:
        include:
          # Existing stress tests
          - test_type: 'stress'
            platform: confluent
            platform_version: '7.6.0'
            snowflake_cloud: 'AWS'

          # NEW: Chaos tests
          - test_type: 'chaos'
            platform: confluent
            platform_version: '7.6.0'
            snowflake_cloud: 'AWS'

    steps:
    - uses: actions/checkout@v4

    - name: Decrypt profile.json in Snowflake Cloud ${{ matrix.snowflake_cloud }}
      run: ./.github/scripts/decrypt_secret.sh ${{ matrix.snowflake_cloud }}
      env:
        SNOWFLAKE_TEST_PROFILE_SECRET: ${{ secrets.SNOWFLAKE_TEST_PROFILE_SECRET }}

    - uses: ./.github/actions/build-connector
      with:
        platform: ${{ matrix.platform }}

    - uses: ./.github/actions/run-e2e-tests
      with:
        platform: ${{ matrix.platform }}
        platform-version: ${{ matrix.platform_version }}
        snowflake-cloud: ${{ matrix.snowflake_cloud }}
        test-type: ${{ matrix.test_type }}  # NEW: 'stress' or 'chaos'

    - name: Upload test metrics
      if: always()
      uses: actions/upload-artifact@v4
      with:
        name: ${{ matrix.test_type }}-metrics-${{ matrix.platform }}-${{ matrix.platform_version }}
        path: test/results/metrics-*.json
```

**Update `.github/actions/run-e2e-tests/action.yml`:**
```yaml
inputs:
  test-type:
    description: 'Test type: regular, stress, or chaos'
    required: false
    default: 'regular'

runs:
  using: 'composite'
  steps:
    - name: Run E2E tests
      shell: bash
      run: |
        cd test/docker

        if [ "${{ inputs.test-type }}" == "chaos" ]; then
          ./run_tests.sh \
            --platform=${{ inputs.platform }} \
            --version=${{ inputs.platform-version }} \
            --chaos
        elif [ "${{ inputs.test-type }}" == "stress" ]; then
          ./run_tests.sh \
            --platform=${{ inputs.platform }} \
            --version=${{ inputs.platform-version }} \
            --pressure
        else
          ./run_tests.sh \
            --platform=${{ inputs.platform }} \
            --version=${{ inputs.platform-version }}
        fi
```

**Update `test/docker/run_tests.sh`:**
```bash
#!/bin/bash

# ... existing argument parsing ...

CHAOS_MODE=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --chaos)
      CHAOS_MODE=true
      shift
      ;;
    # ... existing args ...
  esac
done

# ... existing logic ...

# Compose file selection
COMPOSE_FILES="-f docker-compose.base.yml -f docker-compose.${PLATFORM}.yml"

if [ "$CHAOS_MODE" = true ]; then
  COMPOSE_FILES="$COMPOSE_FILES -f docker-compose.chaos.yml"
  export TEST_SUITE="chaos"  # Tells Python test runner to load chaos tests
fi

# ... rest of script ...
```

**Update `test/test_selector.py`:**
```python
import os

def get_test_suite():
    """Return appropriate test suite based on environment"""
    suite_type = os.environ.get('TEST_SUITE', 'regular')

    if suite_type == 'chaos':
        return get_chaos_tests()
    elif suite_type == 'pressure':
        return get_stress_tests()
    else:
        return get_regular_e2e_tests()

def get_chaos_tests():
    """Return chaos test suite"""
    from test_suit.chaos_tests.test_channel_invalidation_append_row import TestChannelInvalidationOnAppendRow
    from test_suit.chaos_tests.test_memory_backpressure import TestMemoryBackpressure
    from test_suit.chaos_tests.test_bandwidth_throttling import TestBandwidthThrottling
    from test_suit.performance_tests.test_baseline_throughput import TestBaselineThroughput

    return [
        TestChannelInvalidationOnAppendRow,
        TestMemoryBackpressure,
        TestBandwidthThrottling,
        TestBaselineThroughput
    ]
```

---

## Baseline Management

### Initial Baseline Creation

When first implementing chaos tests, baselines don't exist yet. Process:

1. Run tests 3-5 times to establish stable baseline
2. Take average of metrics across runs
3. Commit initial baselines to git
4. CI now enforces these baselines going forward

**Script: `test/update_baseline.sh`**
```bash
#!/bin/bash

# Usage: ./test/update_baseline.sh chaos-tests

BASELINE_TYPE=$1
BASELINE_FILE="test/baselines/${BASELINE_TYPE}.json"
METRICS_FILE="test/results/metrics-latest.json"

if [ ! -f "$METRICS_FILE" ]; then
  echo "Error: No metrics file found at $METRICS_FILE"
  echo "Run tests first to generate metrics"
  exit 1
fi

# Backup existing baseline
if [ -f "$BASELINE_FILE" ]; then
  cp "$BASELINE_FILE" "${BASELINE_FILE}.backup"
  echo "Backed up existing baseline to ${BASELINE_FILE}.backup"
fi

# Update baseline with new metrics
cp "$METRICS_FILE" "$BASELINE_FILE"

echo "Updated baseline: $BASELINE_FILE"
echo "Review changes with: git diff $BASELINE_FILE"
echo "If correct, commit with: git add $BASELINE_FILE && git commit -m 'Update performance baseline'"
```

### Updating Baselines in PRs

**Scenario:** Developer optimizes connector, throughput improves by 20%

1. CI runs, detects "throughput 20% higher than baseline"
2. CI fails with message: "Performance improved! Update baseline with: ./test/update_baseline.sh performance-tests"
3. Developer runs script locally, reviews diff
4. Commits updated baseline in same PR
5. PR description explains optimization and baseline update
6. Reviewer validates improvement is legitimate, not a fluke

**Preventing Baseline Drift:**
- Baselines stored in git (version controlled, reviewable)
- Baseline updates require explicit commit (not automatic)
- CI artifacts include metrics JSON for each run (can investigate trends)
- Large baseline changes (>20%) require explanation in PR

---

## Implementation Phases

### Phase 1: Infrastructure Setup

**Tasks:**
1. Add `docker-compose.chaos.yml` with toxiproxy service
2. Implement `ToxiproxyClient` Python class
3. Implement `MetricsCollector` Python class
4. Update `run_tests.sh` to support `--chaos` flag
5. Create `test/baselines/` directory structure

**Deliverable:** Can run `./run_tests.sh --chaos` and toxiproxy intercepts traffic

### Phase 2: Base Framework & First Test

**Tasks:**
1. Implement `BaseChaosTest` class
2. Implement `TestChannelInvalidationOnAppendRow`
3. Create connector config template for chaos tests
4. Run test locally, verify channel recovery behavior
5. Establish initial baseline

**Deliverable:** One working chaos test with metrics collection

### Phase 3: Additional Chaos Tests

**Tasks:**
1. Implement `TestMemoryBackpressure`
2. Implement `TestBandwidthThrottling`
3. Implement `TestAppendRowFailureThenOffsetTokenFailure`
4. Run full chaos suite, establish baselines

**Deliverable:** Full chaos test suite with baselines

### Phase 4: Performance Tests

**Tasks:**
1. Implement `TestBaselineThroughput`
2. Implement `TestBaselineUnderLoad`
3. Implement `TestCpuStarvation`
4. Implement `TestOomRecovery`
5. Establish performance baselines

**Deliverable:** Performance test suite with baselines

### Phase 5: CI Integration

**Tasks:**
1. Update GitHub Actions workflows
2. Add chaos test matrix to `end-to-end-stress.yml`
3. Configure metrics artifact upload
4. Test CI runs on feature branch
5. Document baseline update process

**Deliverable:** Chaos tests running in CI on every PR

### Phase 6: Optimization & Documentation

**Tasks:**
1. Optimize test runtime (ensure <15 min total)
2. Add logging/debugging for test failures
3. Write user-facing documentation
4. Add README in `test/chaos_framework/`
5. Create troubleshooting guide

**Deliverable:** Production-ready chaos test suite with documentation

---

## Success Criteria

**Must Have:**
- ✅ Zero data loss or duplication in all chaos scenarios
- ✅ CI detects >10% performance regressions and fails PR
- ✅ CI runtime <15 minutes for full chaos suite
- ✅ Tests use real Snowflake ingest SDK (minimal fakes)
- ✅ Baselines stored in git, updated via explicit PR review

**Should Have:**
- ✅ End-to-end latency p99 <15s validated in CI
- ✅ All four fault injection types working (lifecycle, invalidation, network, resources)
- ✅ Recovery time <30s from all injected failures
- ✅ JMX metrics collected (heap, GC, throughput)

**Nice to Have:**
- ⚠️ Trend analysis across multiple CI runs
- ⚠️ Performance dashboard with historical metrics
- ⚠️ Automated baseline updates for non-controversial improvements

---

## Risks and Mitigations

**Risk:** Toxiproxy adds complexity, could introduce flakiness
- **Mitigation:** Use health checks, clear toxics between tests, retry logic for toxiproxy API calls

**Risk:** CI might be slower/less reliable than local Docker
- **Mitigation:** Establish separate baselines for CI vs local, allow higher tolerance in CI

**Risk:** Snowflake latency variability causes flaky failures
- **Mitigation:** Use percentiles (p99) not max, allow 5s buffer on latency thresholds

**Risk:** Baseline drift over time (hardware changes, Snowflake improvements)
- **Mitigation:** Quarterly baseline review, track trends in artifacts, allow recalibration

**Risk:** 15-minute runtime constraint too tight
- **Mitigation:** Run subset of chaos tests in rotation, or split into separate workflow for nightly runs

---

## Open Questions

1. **JMX Exporter:** Should we add Prometheus JMX exporter to Connect container for richer metrics? (Adds complexity)
2. **Toxiproxy vs Real Network:** Could we use Docker network manipulation (iptables) instead of toxiproxy? (More authentic but harder to control)
3. **Baseline Storage:** Should baselines live in separate repo for easier updates without code review? (Trades transparency for convenience)
4. **Test Rotation:** If 15 min too tight, which tests run on every PR vs nightly? (Need to prioritize)

---

## References

- Testing Kafka Connectors: https://www.morling.dev/blog/testing-kafka-connectors/
- Toxiproxy: https://github.com/Shopify/toxiproxy
- Current stress tests: `test/test_suit/test_pressure.py`, `test/test_suit/test_pressure_restart.py`
- Current resilience tests: `test/test_suit/resilience_tests/`
- Existing e2e framework: `test/README.md`
