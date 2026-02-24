# Performance and Chaos Testing Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.
>
> **Model Required:** Use Claude Opus 4.6 (model ID: claude-opus-4-6) for best results on this implementation.

**Goal:** Add performance regression detection and chaos testing to CI/CD with toxiproxy-based fault injection and real Snowflake SDK testing.

**Architecture:** Docker Compose chaos stack with toxiproxy intercepting Snowflake traffic, Python test framework with metrics collection (JMX + latency tracking), git-committed baselines for regression detection.

**Tech Stack:** Toxiproxy 2.9.0, Docker Compose, Python 3.9, pytest, Kafka Connect, Snowflake ingest SDK

**Design Document:** `docs/plans/2026-02-20-perf-chaos-ci-design.md`

---

## Phase 1: Infrastructure Setup

### Task 1: Add toxiproxy to Docker Compose

**Files:**
- Create: `test/docker/docker-compose.chaos.yml`
- Modify: `test/docker/run_tests.sh`

**Step 1: Create docker-compose.chaos.yml**

Create new file with toxiproxy service and resource limits:

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

networks:
  kafka-network:
    external: true
```

**Step 2: Update run_tests.sh to support --chaos flag**

Add chaos mode argument parsing:

```bash
# After existing argument parsing section
CHAOS_MODE=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --chaos)
      CHAOS_MODE=true
      shift
      ;;
    # ... existing cases ...
  esac
done

# In compose file selection section
COMPOSE_FILES="-f docker-compose.base.yml -f docker-compose.${PLATFORM}.yml"

if [ "$CHAOS_MODE" = true ]; then
  COMPOSE_FILES="$COMPOSE_FILES -f docker-compose.chaos.yml"
  export TEST_SUITE="chaos"
fi
```

**Step 3: Test toxiproxy starts**

Run:
```bash
cd test/docker
./run_tests.sh --platform=confluent --version=7.6.0 --chaos --keep
docker ps | grep toxiproxy
curl http://localhost:8474/version
```

Expected: toxiproxy container running, API responds with version

**Step 4: Commit**

```bash
git add test/docker/docker-compose.chaos.yml test/docker/run_tests.sh
git commit -m "feat: add toxiproxy to docker compose for chaos testing

Add toxiproxy service for network fault injection in chaos tests.
Update run_tests.sh to support --chaos flag.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

### Task 2: Create toxiproxy Python client

**Files:**
- Create: `test/chaos_framework/__init__.py`
- Create: `test/chaos_framework/toxiproxy_client.py`
- Create: `test/chaos_framework/test_toxiproxy_client.py` (unit test)

**Step 1: Write toxiproxy_client.py**

```python
"""
Client for controlling toxiproxy to inject network faults.
"""
import requests
import time
from typing import Optional


class ToxiproxyClient:
    """Client for toxiproxy REST API to inject network failures."""

    def __init__(self, admin_url: str = "http://localhost:8474"):
        self.admin_url = admin_url
        self.proxy_name = "snowflake"

    def create_snowflake_proxy(self, snowflake_host: str) -> None:
        """
        Create proxy: localhost:20443 -> snowflake_host:443
        All Snowflake traffic routes through this proxy.
        """
        proxy = {
            "name": self.proxy_name,
            "listen": "0.0.0.0:20443",
            "upstream": f"{snowflake_host}:443",
            "enabled": True
        }
        resp = requests.post(f"{self.admin_url}/proxies", json=proxy)
        resp.raise_for_status()

    def add_latency(self, ms: int, jitter_ms: int = 0) -> None:
        """Add network latency in milliseconds."""
        toxic = {
            "type": "latency",
            "attributes": {"latency": ms, "jitter": jitter_ms}
        }
        self._add_toxic(toxic)

    def add_bandwidth_limit(self, rate_kbps: int) -> None:
        """Throttle bandwidth in KB/sec."""
        toxic = {
            "type": "bandwidth",
            "attributes": {"rate": rate_kbps}
        }
        self._add_toxic(toxic)

    def partition_network(self) -> None:
        """Drop all packets (simulate network partition)."""
        toxic = {
            "type": "timeout",
            "attributes": {"timeout": 0}
        }
        self._add_toxic(toxic)

    def reset_connection(self) -> None:
        """Send TCP RST packets (simulate connection failure)."""
        toxic = {
            "type": "reset_peer",
            "attributes": {"timeout": 0}
        }
        self._add_toxic(toxic)

    def remove_all_toxics(self) -> None:
        """Clear all fault injection (restore normal operation)."""
        resp = requests.get(f"{self.admin_url}/proxies/{self.proxy_name}/toxics")
        toxics = resp.json()
        for toxic in toxics:
            requests.delete(
                f"{self.admin_url}/proxies/{self.proxy_name}/toxics/{toxic['name']}"
            )

    def _add_toxic(self, toxic_config: dict) -> None:
        """Add a toxic to the proxy."""
        toxic_config["name"] = f"toxic_{int(time.time() * 1000)}"
        toxic_config["toxicity"] = 1.0  # Apply to 100% of connections
        resp = requests.post(
            f"{self.admin_url}/proxies/{self.proxy_name}/toxics",
            json=toxic_config
        )
        resp.raise_for_status()
```

**Step 2: Write unit test**

```python
"""Unit tests for ToxiproxyClient (requires toxiproxy running)."""
import pytest
from chaos_framework.toxiproxy_client import ToxiproxyClient


@pytest.fixture
def client():
    """Create toxiproxy client for testing."""
    return ToxiproxyClient()


def test_create_proxy(client):
    """Test proxy creation."""
    # Cleanup any existing proxy first
    try:
        client.remove_all_toxics()
    except:
        pass

    client.create_snowflake_proxy("example.snowflakecomputing.com")
    # Verify proxy exists via API
    import requests
    resp = requests.get(f"{client.admin_url}/proxies/{client.proxy_name}")
    assert resp.status_code == 200


def test_add_latency(client):
    """Test latency toxic."""
    client.add_latency(100, jitter_ms=20)
    # Verify toxic added
    import requests
    resp = requests.get(f"{client.admin_url}/proxies/{client.proxy_name}/toxics")
    toxics = resp.json()
    assert any(t["type"] == "latency" for t in toxics)


def test_remove_toxics(client):
    """Test removing all toxics."""
    client.add_latency(100)
    client.remove_all_toxics()
    import requests
    resp = requests.get(f"{client.admin_url}/proxies/{client.proxy_name}/toxics")
    toxics = resp.json()
    assert len(toxics) == 0
```

**Step 3: Run test**

Run:
```bash
# Start toxiproxy first
cd test/docker
./run_tests.sh --platform=confluent --version=7.6.0 --chaos --keep

# Run unit test in test-runner container
docker exec -it test-runner pytest test/chaos_framework/test_toxiproxy_client.py -v
```

Expected: Tests pass

**Step 4: Commit**

```bash
git add test/chaos_framework/
git commit -m "feat: add toxiproxy Python client for fault injection

Implements ToxiproxyClient with methods for:
- Network partition (drop all packets)
- Latency injection
- Bandwidth throttling
- Connection resets

Includes unit tests.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

### Task 3: Create metrics collection framework

**Files:**
- Create: `test/chaos_framework/metrics_collector.py`
- Create: `test/baselines/.gitkeep`

**Step 1: Write metrics_collector.py**

```python
"""
Metrics collection for chaos tests: latency, throughput, recovery time.
"""
import time
import statistics
import json
from typing import List, Dict, Tuple, Optional


class MetricsCollector:
    """Collect and compare performance metrics against baselines."""

    def __init__(self, snowflake_conn=None):
        self.snowflake_conn = snowflake_conn
        self.latencies: List[int] = []
        self.start_time: Optional[float] = None
        self.fault_injected_time: Optional[float] = None
        self.fault_cleared_time: Optional[float] = None

    def mark_test_start(self) -> None:
        """Mark test start time."""
        self.start_time = time.time()

    def mark_fault_injected(self) -> None:
        """Mark when fault was injected."""
        self.fault_injected_time = time.time()

    def mark_fault_cleared(self) -> None:
        """Mark when fault was cleared."""
        self.fault_cleared_time = time.time()

    def track_record_latency(self, produce_timestamp_ms: int, topic: str) -> int:
        """
        Track end-to-end latency for a record.

        Args:
            produce_timestamp_ms: Timestamp when record was produced to Kafka
            topic: Snowflake table name

        Returns:
            Latency in milliseconds
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

        raise TimeoutError(
            f"Record with timestamp {produce_timestamp_ms} never appeared in Snowflake"
        )

    def compute_latency_percentiles(self) -> Dict[str, int]:
        """Calculate p50, p95, p99 from tracked latencies."""
        if not self.latencies:
            return {}

        sorted_latencies = sorted(self.latencies)
        return {
            "latency_p50_ms": int(statistics.quantiles(sorted_latencies, n=100)[49]),
            "latency_p95_ms": int(statistics.quantiles(sorted_latencies, n=100)[94]),
            "latency_p99_ms": int(statistics.quantiles(sorted_latencies, n=100)[98]),
            "latency_min_ms": min(sorted_latencies),
            "latency_max_ms": max(sorted_latencies)
        }

    def compute_recovery_time(self) -> Optional[float]:
        """Calculate time from fault injection to recovery in seconds."""
        if not self.fault_injected_time or not self.fault_cleared_time:
            return None
        return self.fault_cleared_time - self.fault_injected_time

    def compare_to_baseline(
        self,
        current_metrics: Dict[str, float],
        baseline_file: str,
        test_name: str
    ) -> Tuple[bool, str]:
        """
        Compare current run metrics to baseline.

        Args:
            current_metrics: Current test run metrics
            baseline_file: Path to baseline JSON file
            test_name: Name of test to compare

        Returns:
            (passed, details) tuple
        """
        try:
            with open(baseline_file) as f:
                baselines = json.load(f)
        except FileNotFoundError:
            return (True, f"No baseline file {baseline_file}, skipping comparison")

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
                    failures.append(
                        f"{metric}: {current_value:.2f} vs baseline {baseline_value:.2f} "
                        f"({pct_change:+.1f}%)"
                    )

            # For latency/recovery time, lower is better
            elif "latency" in metric or "recovery" in metric:
                if current_value > baseline_value * 1.1:
                    pct_change = ((current_value - baseline_value) / baseline_value) * 100
                    failures.append(
                        f"{metric}: {current_value:.2f} vs baseline {baseline_value:.2f} "
                        f"({pct_change:+.1f}%)"
                    )

            # For correctness metrics, must be exact
            elif metric in ["data_loss", "duplicates"]:
                if current_value != baseline_value:
                    failures.append(f"{metric}: {current_value} (MUST be {baseline_value})")

        if failures:
            return (False, "Performance regression detected:\n" + "\n".join(failures))
        else:
            return (True, "All metrics within baseline thresholds")
```

**Step 2: Create baselines directory**

Run:
```bash
mkdir -p test/baselines
touch test/baselines/.gitkeep
```

**Step 3: Commit**

```bash
git add test/chaos_framework/metrics_collector.py test/baselines/
git commit -m "feat: add metrics collection framework for chaos tests

Implements MetricsCollector with:
- End-to-end latency tracking (Kafka → Snowflake)
- Latency percentiles (p50/p95/p99)
- Recovery time measurement
- Baseline comparison with regression detection

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Phase 2: Base Framework & First Test

### Task 4: Create base chaos test class

**Files:**
- Create: `test/test_suit/base_chaos_test.py`

**Step 1: Write base_chaos_test.py**

```python
"""
Base class for chaos tests with fault injection and metrics.
"""
from test_suit.base_e2e import BaseE2eTest
from test_suit.test_utils import NonRetryableError
from chaos_framework.toxiproxy_client import ToxiproxyClient
from chaos_framework.metrics_collector import MetricsCollector
import time
import json


class BaseChaosTest(BaseE2eTest):
    """Base class for chaos tests with fault injection and metrics."""

    def __init__(self, driver, nameSalt):
        super().__init__(driver, nameSalt)
        self.toxiproxy = ToxiproxyClient()
        self.metrics = MetricsCollector(snowflake_conn=driver.snowflake_conn)
        self.records_sent = 0

    def setup(self):
        """Initialize toxiproxy proxy for Snowflake traffic."""
        snowflake_host = self._get_snowflake_host()
        self.toxiproxy.create_snowflake_proxy(snowflake_host)
        self.metrics.mark_test_start()

    def inject_fault(self):
        """Override in subclass to define chaos scenario."""
        raise NotImplementedError("Subclass must implement inject_fault()")

    def send_records(self, count: int, partition: int = 0) -> None:
        """
        Send records with timestamp tracking for latency measurement.

        Args:
            count: Number of records to send
            partition: Kafka partition number
        """
        values = []
        for i in range(count):
            timestamp_ms = int(time.time() * 1000)
            record = {
                'data': f'value_{i}',
                'produce_timestamp': timestamp_ms
            }
            values.append(json.dumps(record).encode('utf-8'))
            # Small delay to avoid duplicate timestamps
            if i % 100 == 0:
                time.sleep(0.01)

        self.driver.sendBytesData(self.topic, values, partition=partition)
        self.records_sent += count

    def verify_correctness(self) -> None:
        """Assert no data loss or duplication (HARD REQUIREMENT)."""
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

    def verify_performance(self, baseline_file: str) -> None:
        """
        Compare metrics to baseline, fail if regression detected.

        Args:
            baseline_file: Path to baseline JSON file
        """
        # Collect current run metrics
        current_metrics = {
            "records_sent": self.records_sent,
            "records_in_snowflake": self.driver.select_number_of_records(self.topic),
            "data_loss": 0,
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

    def _get_snowflake_host(self) -> str:
        """Extract Snowflake host from connection."""
        # Parse from profile.json or connection string
        # For now, return placeholder - will be populated from config
        return "account.snowflakecomputing.com"
```

**Step 2: Commit**

```bash
git add test/test_suit/base_chaos_test.py
git commit -m "feat: add base chaos test class

Implements BaseChaosTest extending BaseE2eTest with:
- Toxiproxy integration for fault injection
- Metrics collection (latency, recovery time)
- Correctness verification (zero data loss/duplication)
- Performance baseline comparison

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

### Task 5: Implement first chaos test - channel invalidation

**Files:**
- Create: `test/test_suit/chaos_tests/__init__.py`
- Create: `test/test_suit/chaos_tests/test_channel_invalidation_append_row.py`
- Create: `test/rest_request_template/test_channel_invalidation_append_row.json`

**Step 1: Write test implementation**

```python
"""
Test channel invalidation during appendRow operation.
Simulates network partition that causes channel to become invalid.
"""
from test_suit.base_chaos_test import BaseChaosTest
from test_suit.test_utils import NonRetryableError
import time


class TestChannelInvalidationOnAppendRow(BaseChaosTest):
    """Test channel invalidation via network partition during appendRow."""

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
        # Phase 1: Send first batch (normal operation)
        print("Phase 1: Sending 5000 records (normal operation)")
        self.send_records(count=5000)
        time.sleep(5)

        # Phase 2: Inject network partition
        print("Phase 2: Injecting network partition for 30 seconds")
        self.metrics.mark_fault_injected()
        self.toxiproxy.partition_network()

        # Send more records during partition (will buffer in Kafka)
        print("Phase 2: Sending 5000 records during partition")
        self.send_records(count=5000)

        # Keep partition active
        time.sleep(30)

        # Phase 3: Restore network
        print("Phase 3: Restoring network")
        self.toxiproxy.remove_all_toxics()
        self.metrics.mark_fault_cleared()

        # Wait for recovery
        time.sleep(10)

    def verify(self, round):
        # Verify correctness (no data loss/duplication)
        self.verify_correctness()

        # Verify performance against baseline
        self.verify_performance('test/baselines/chaos-tests.json')

        # Check recovery time
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

**Step 2: Create connector config template**

```json
{
  "name": "SNOWFLAKE_CONNECTOR_NAME",
  "config": {
    "connector.class": "com.snowflake.kafka.connector.SnowflakeSinkConnector",
    "tasks.max": "1",
    "topics": "SNOWFLAKE_TEST_TOPIC",
    "snowflake.url.name": "SNOWFLAKE_HOST",
    "snowflake.user.name": "SNOWFLAKE_USER",
    "snowflake.private.key": "SNOWFLAKE_PRIVATE_KEY",
    "snowflake.database.name": "SNOWFLAKE_DATABASE",
    "snowflake.schema.name": "SNOWFLAKE_SCHEMA",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter.schemas.enable": "false",
    "snowflake.ingestion.method": "SNOWPIPE_STREAMING",
    "snowflake.enable.schematization": "false",
    "errors.tolerance": "none",
    "errors.log.enable": "true"
  }
}
```

**Step 3: Update test selector to include chaos tests**

Modify `test/test_selector.py`:

```python
def get_chaos_tests():
    """Return chaos test suite."""
    from test_suit.chaos_tests.test_channel_invalidation_append_row import TestChannelInvalidationOnAppendRow

    return [
        TestChannelInvalidationOnAppendRow
    ]

def get_test_suite():
    """Return appropriate test suite based on environment."""
    import os
    suite_type = os.environ.get('TEST_SUITE', 'regular')

    if suite_type == 'chaos':
        return get_chaos_tests()
    elif suite_type == 'pressure':
        return get_stress_tests()  # existing
    else:
        return get_regular_e2e_tests()  # existing
```

**Step 4: Run test locally**

Run:
```bash
cd test/docker
./run_tests.sh --platform=confluent --version=7.6.0 --chaos --tests=TestChannelInvalidationOnAppendRow
```

Expected: Test runs, may fail initially (no baseline yet), but should verify connectivity and fault injection

**Step 5: Commit**

```bash
git add test/test_suit/chaos_tests/ test/rest_request_template/test_channel_invalidation_append_row.json test/test_selector.py
git commit -m "feat: add first chaos test - channel invalidation on appendRow

Implements TestChannelInvalidationOnAppendRow:
- Send 5K records normally
- Inject 30s network partition via toxiproxy
- Send 5K more records during partition
- Verify connector recovers with no data loss

Verifies:
- Zero data loss/duplication
- Recovery time <30s
- Correct offset reset after channel reopen

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

### Task 6: Create initial baseline

**Files:**
- Create: `test/baselines/chaos-tests.json`
- Create: `test/scripts/update_baseline.sh`

**Step 1: Run test 3 times to establish baseline**

Run:
```bash
cd test/docker
for i in 1 2 3; do
  echo "Run $i of 3"
  ./run_tests.sh --platform=confluent --version=7.6.0 --chaos --tests=TestChannelInvalidationOnAppendRow
done
```

Collect metrics from each run (recovery time, latency percentiles).

**Step 2: Create baseline JSON with average values**

Example (adjust with actual values from runs):

```json
{
  "TestChannelInvalidationOnAppendRow": {
    "recovery_time_seconds": 12.5,
    "latency_p50_ms": 2800,
    "latency_p95_ms": 6200,
    "latency_p99_ms": 8900,
    "data_loss": 0,
    "duplicates": 0
  }
}
```

**Step 3: Create baseline update script**

```bash
#!/bin/bash
# Update baseline from latest test run metrics

BASELINE_TYPE=$1
BASELINE_FILE="test/baselines/${BASELINE_TYPE}.json"
METRICS_FILE="test/results/metrics-latest.json"

if [ -z "$BASELINE_TYPE" ]; then
  echo "Usage: ./test/scripts/update_baseline.sh <chaos-tests|performance-tests>"
  exit 1
fi

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

# Update baseline
cp "$METRICS_FILE" "$BASELINE_FILE"

echo "Updated baseline: $BASELINE_FILE"
echo "Review changes with: git diff $BASELINE_FILE"
echo "If correct, commit with: git add $BASELINE_FILE && git commit -m 'Update performance baseline'"
```

**Step 4: Make script executable**

Run:
```bash
chmod +x test/scripts/update_baseline.sh
```

**Step 5: Commit baseline**

```bash
git add test/baselines/chaos-tests.json test/scripts/update_baseline.sh
git commit -m "feat: add initial baseline for chaos tests

Baseline established from 3 test runs:
- TestChannelInvalidationOnAppendRow

Includes update_baseline.sh script for future updates.

Co-Authored-By: Claude Sonnet 4.5 <noreply@anthropic.com>"
```

---

## Next Steps

**Phase 3-6 tasks** (Additional chaos tests, performance tests, CI integration, optimization) follow the same pattern:

1. Write test extending `BaseChaosTest`
2. Create connector config template
3. Update test selector
4. Run locally to validate
5. Establish baseline
6. Commit

**To continue implementation:**
- Use @superpowers:executing-plans skill in this session
- Or: create new worktree and use @superpowers:subagent-driven-development

**Key files to reference:**
- Design: `docs/plans/2026-02-20-perf-chaos-ci-design.md`
- Base test: `test/test_suit/base_chaos_test.py`
- Example test: `test/test_suit/chaos_tests/test_channel_invalidation_append_row.py`
- Toxiproxy client: `test/chaos_framework/toxiproxy_client.py`
- Metrics: `test/chaos_framework/metrics_collector.py`
