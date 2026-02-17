# Memory Leak Reproduction Test

## Overview

`test_kc_memory_leak_pause_resume.py` reproduces a memory leak issue observed on Confluent Cloud where repeated pause/resume operations cause the connector to hit memory limits and fail with `MemoryThresholdExceededInContainer` (HTTP 429).

## Original Issue

After multiple pause/resume cycles on Confluent Cloud:
- Memory usage grows to 90% of pod limits
- Snowflake backend returns HTTP 429 (Too Many Requests)
- Error: `MemoryThresholdExceededInContainer`
- Suspected cause: Memory leak in SSv2 SDK channel reopen logic

## Test Configuration

The test runs **20 pause/resume cycles** with:
- **10,000 records per cycle** (increased from 1,000 for more memory pressure)
- **Total: 210,000 records** across all cycles
- **Memory monitoring** after each cycle using Docker stats
- **Threshold alerts** at 75%, 85%, and 90% memory usage

### Key Parameters (in `__init__`)

```python
self.pauseResumeCycles = 20        # Number of pause/resume cycles
self.recordNum = 10000              # Records sent per cycle
self.memoryThresholds = {
    "warning": 75,                  # Warn at 75% memory
    "high": 85,                     # High at 85% memory
    "critical": 90                  # Critical at 90% memory (matches reported issue)
}
```

## How to Run

### Basic Run

```bash
cd test/docker
./run_tests.sh --platform=confluent --version=7.8.0 --tests=TestKcMemoryLeakPauseResume
```

### With Memory Constraints (Recommended)

Use the memory-constrained docker-compose override to accelerate reproduction:

```bash
./run_tests.sh --platform=confluent --version=7.8.0 \
  --tests=TestKcMemoryLeakPauseResume \
  --memory-constrained
```

This limits kafka-connect to:
- **2GB heap** (`-Xmx2g`)
- **2.5GB container limit**

### Keep Containers Running for Inspection

```bash
./run_tests.sh --platform=confluent --version=7.8.0 \
  --tests=TestKcMemoryLeakPauseResume \
  --memory-constrained \
  --keep
```

Then inspect:
```bash
# View logs
docker logs test-kafka-connect

# Check memory usage
docker stats test-kafka-connect

# Cleanup when done
docker compose -f docker-compose.base.yml -f docker-compose.confluent.yml \
  -f docker-compose.memory-constrained.yml down -v
```

## What the Test Does

1. **Creates connector** with 3 tasks on 1 topic/partition
2. **For each cycle:**
   - Sends 10,000 records to Kafka
   - **Pauses** connector (5 sec wait)
   - **Resumes** connector (5 sec wait)
   - Checks connector/task status for failures
   - **Monitors memory usage** via `docker stats`
   - Reports threshold crossings (75%, 85%, 90%)
3. **Sends final batch** after all cycles
4. **Verifies** all 210,000 records ingested correctly
5. **Reports summary:**
   - Memory growth (min/max/avg/growth %)
   - Task failures
   - Memory-related errors detected

## Expected Output

### Success (No Memory Leak)

```
=== Memory Usage Summary ===
Total cycles monitored: 20
Memory usage (% of container limit):
  Minimum: 45.2%
  Maximum: 52.8%
  Average: 48.9%
  Growth: 7.6% (from 45.2% to 52.8%)

Success - expected number of records found after 20 pause/resume cycles
```

### Failure (Memory Leak Reproduced)

```
Cycle 15 - Memory: 1.8GiB / 2.0GiB (90.2%) *** CRITICAL (90%) THRESHOLD CROSSED ***
!!! CRITICAL MEMORY USAGE: 90.2% - Approaching reported issue threshold (90%) !!!

*** EXACT ISSUE REPRODUCED: MemoryThresholdExceededInContainer at cycle 15 ***
*** HTTP 429 (Too Many Requests) detected at cycle 15 ***
This matches the original reported issue!

=== Memory Usage Summary ===
Total cycles monitored: 20
Memory usage (% of container limit):
  Minimum: 45.2%
  Maximum: 92.5%
  Average: 71.3%
  Growth: 47.3% (from 45.2% to 92.5%)
  *** SIGNIFICANT MEMORY GROWTH DETECTED (47.3%) ***

!!! CRITICAL: Memory exceeded 90% in 6 cycles !!!
This matches the reported issue: MemoryThresholdExceededInContainer
```

## Monitoring & Debugging

### Check Connector Status

The test queries `/connectors/{name}/status` after each cycle and logs:
- Connector state (RUNNING/PAUSED/FAILED)
- Task states and IDs
- Task failure traces

### Memory-Related Error Detection

The test specifically checks for:
- `MemoryThresholdExceededInContainer` - exact error from issue
- `HTTP 429` / `Too Many Requests` - backend throttling
- `OutOfMemory` - JVM heap exhaustion
- `MemoryThreshold` - general memory errors

### Manual Memory Inspection

While test is running (use `--keep`):

```bash
# Real-time memory monitoring
docker stats test-kafka-connect

# Check JVM heap usage
docker exec test-kafka-connect jcmd 1 VM.native_memory summary

# View connector logs
docker logs test-kafka-connect 2>&1 | grep -i "memory\|429\|threshold"
```

## Customization

### Adjust Cycles or Records

Edit `test_kc_memory_leak_pause_resume.py`:

```python
self.pauseResumeCycles = 50        # Run more cycles
self.recordNum = 50000             # Send more data per cycle
```

### Change Memory Thresholds

```python
self.memoryThresholds = {
    "warning": 70,   # Alert earlier
    "high": 80,
    "critical": 85   # Match different pod limits
}
```

### Adjust Timing

```python
self.sleepAfterPause = 10    # Wait longer after pause
self.sleepAfterResume = 10   # Wait longer after resume
```

## Files Created

- `test/test_suit/resilience_tests/test_kc_memory_leak_pause_resume.py` - Test implementation
- `test/rest_request_template/test_kc_memory_leak_pause_resume.json` - Connector config
- `test/docker/docker-compose.memory-constrained.yml` - Memory-constrained override
- `test/docker/run_tests.sh` - Updated with `--memory-constrained` flag

## Troubleshooting

### Test Fails Immediately with FFI Errors

If you see `NoClassDefFoundError: FFIClient` from the start:
- The connector plugin may not be built correctly
- Run: `./test/build_runtime_jar.sh . package confluent`
- Ensure `/tmp/sf-kafka-connect-plugin.zip` exists

### No Memory Stats Collected

If memory monitoring fails:
- Ensure Docker is accessible from inside test-runner container
- Check: `docker stats test-kafka-connect` works from host
- The test-runner container may need Docker socket access

### Memory Never Exceeds Thresholds

- Try `--memory-constrained` flag to lower limits
- Increase `recordNum` to 50000+ per cycle
- Increase `pauseResumeCycles` to 50+
- The memory leak may be gradual and need more iterations
