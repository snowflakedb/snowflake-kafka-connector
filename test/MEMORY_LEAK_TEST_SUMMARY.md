# Memory Leak Test Implementation - Summary

## What Was Created

A comprehensive e2e test to reproduce a memory leak issue observed on Confluent Cloud where repeated pause/resume operations cause memory exhaustion.

### Files Created/Modified

1. **Test Implementation**: `test/test_suit/resilience_tests/test_kc_memory_leak_pause_resume.py`
   - 20 pause/resume cycles with 10,000 records each
   - Real-time memory monitoring using `docker stats`
   - Threshold alerts at 75%, 85%, 90%
   - Detects `MemoryThresholdExceededInContainer`, HTTP 429 errors
   - Comprehensive memory growth analysis

2. **Connector Config**: `test/rest_request_template/test_kc_memory_leak_pause_resume.json`
   - SSv2 (SnowflakeStreamingSinkConnector) with 3 tasks
   - JSON converter, DLQ enabled

3. **Memory-Constrained Docker Override**: `test/docker/docker-compose.memory-constrained.yml`
   - Limits kafka-connect to 2GB heap, 2.5GB container memory
   - Accelerates memory pressure reproduction

4. **Test Runner Updates**:
   - `test/docker/run_tests.sh` - Added `--memory-constrained` flag
   - `test/docker/Dockerfile.test-runner` - Added Docker CLI for memory monitoring
   - `test/docker/docker-compose.base.yml` - Mounted Docker socket for stats access

5. **Documentation**: `test/test_suit/resilience_tests/README_MEMORY_LEAK_TEST.md`
   - Complete usage guide
   - Customization instructions
   - Troubleshooting tips

6. **Test Registration**: `test/test_suites.py`
   - Registered `TestKcMemoryLeakPauseResume` for both Confluent and Apache platforms

## How to Run

### Quick Start

```bash
cd test/docker

# Basic run
./run_tests.sh --platform=confluent --version=7.8.0 --tests=TestKcMemoryLeakPauseResume

# With memory constraints (recommended)
./run_tests.sh --platform=confluent --version=7.8.0 \
  --tests=TestKcMemoryLeakPauseResume \
  --memory-constrained

# Keep containers for inspection
./run_tests.sh --platform=confluent --version=7.8.0 \
  --tests=TestKcMemoryLeakPauseResume \
  --memory-constrained \
  --keep
```

## Test Results (Initial Run)

### Configuration
- **Cycles**: 20 pause/resume operations
- **Records**: 10,000 per cycle (210,000 total)
- **Memory Limit**: 2.5 GB container, 2 GB heap
- **Duration**: ~6 minutes

### Outcome
✅ **All Tests Passed**
- 210,000 records ingested successfully
- All tasks stayed RUNNING
- No errors detected

✅ **Memory Monitoring Working**
```
=== Memory Usage Summary ===
Memory usage (% of container limit):
  Minimum: 48.4%
  Maximum: 49.6%
  Average: 49.2%
  Growth: 1.2% (from 48.4% to 49.6%)
```

**Conclusion**: No memory leak detected in this test configuration. Memory remained stable around 49% throughout all 20 cycles.

## Original Issue (Not Yet Reproduced)

The reported issue showed:
```
"message": "Rust FFI call failed with error code: MemoryThresholdExceededInContainer,
message: Memory throttling limits reached. Process Memory Used=1254 MB (26%).
Pod Memory Used=1855 MB (90%). System Memory Used=1324 MB (59%)."
```

**Key differences**:
- Original: Confluent Cloud with specific pod limits
- Test: Local Docker with 2.5GB limit
- Original: Memory hit 90%
- Test: Memory stayed at 49%

## How to Stress Test Further

To increase likelihood of reproducing the issue:

### 1. Increase Cycles and Data

Edit `test_kc_memory_leak_pause_resume.py`:

```python
self.pauseResumeCycles = 100        # More cycles (was 20)
self.recordNum = 50000              # More data per cycle (was 10,000)
```

This would send **5,050,000 records** over 100 cycles (~30 minutes).

### 2. Tighter Memory Constraints

Edit `docker-compose.memory-constrained.yml`:

```yaml
services:
  kafka-connect:
    environment:
      KAFKA_HEAP_OPTS: "-Xms256m -Xmx1g"  # Reduce from 2GB
    deploy:
      resources:
        limits:
          memory: 1280m  # Reduce from 2.5GB
```

### 3. More Partitions/Tasks

Edit the connector config template to use more partitions:

```python
self.partitionNum = 12              # More partitions (was 1)
```

And in the JSON config:
```json
"tasks.max": "12"
```

### 4. Longer Runtime

Let the test run for hours to detect gradual leaks:

```python
self.pauseResumeCycles = 500        # Run for several hours
self.sleepAfterPause = 30           # Longer waits
self.sleepAfterResume = 30
```

## Memory Monitoring Features

The test tracks and reports:

### Real-time Per-Cycle
```
Cycle 15 - Memory: 1.8GiB / 2.0GiB (90.2%) *** CRITICAL (90%) THRESHOLD CROSSED ***
!!! CRITICAL MEMORY USAGE: 90.2% - Approaching reported issue threshold (90%) !!!
```

### Summary Statistics
- Minimum, maximum, average memory usage
- Memory growth (first cycle → last cycle)
- Number of cycles exceeding each threshold
- Cycle-by-cycle breakdown

### Error Detection
Specifically looks for:
- `MemoryThresholdExceededInContainer` (exact error from issue)
- `HTTP 429` / `Too Many Requests` (backend throttling)
- `OutOfMemory` (JVM heap exhaustion)
- Any `MemoryThreshold` errors

When detected:
```
*** EXACT ISSUE REPRODUCED: MemoryThresholdExceededInContainer at cycle 15 ***
This matches the original reported issue!
```

## Test Architecture

### Flow
1. **Setup**: Create connector with 3 tasks
2. **Loop (20x)**:
   - Send 10,000 records to Kafka
   - Pause connector (wait 5s)
   - Resume connector (wait 5s)
   - Check connector/task status
   - **Monitor memory via docker stats**
   - Log failures and memory metrics
3. **Final send**: 10,000 more records
4. **Verify**: Check all 210,000 records in Snowflake
5. **Report**: Memory summary and any issues

### Memory Monitoring Implementation
```python
def __monitor_memory_usage(self, cycle):
    # Uses: docker stats test-kafka-connect --no-stream
    # Extracts: "1.5GiB / 2.5GiB|60.00%"
    # Tracks: memory_usage, memory_percent per cycle
    # Alerts: when crossing 75%, 85%, 90% thresholds
```

## Known Limitations

1. **Docker-only**: Memory monitoring requires Docker socket access
2. **Container-level**: Tracks container memory, not individual process memory
3. **Not Confluent Cloud**: Different environment than original issue
4. **Sampling**: Monitors after each cycle, not continuously

## Future Enhancements

Possible improvements:
1. **Continuous monitoring**: Background thread tracking memory every 10s
2. **Heap dump on threshold**: Automatically capture heap dump at 85%+
3. **JMX metrics**: Query JVM memory pools directly
4. **Longer soak test**: Run for 24h+ with periodic reporting
5. **Comparison mode**: Run same test with/without memory constraints

## Validation

The test framework is **production-ready** and:
- ✅ Successfully runs 20 pause/resume cycles
- ✅ Ingests 210,000 records correctly
- ✅ Monitors memory in real-time with threshold alerts
- ✅ Detects memory-related errors
- ✅ Provides detailed statistics and growth analysis
- ✅ Can be easily customized for different stress levels

## Contact

If you reproduce the memory leak issue:
1. Save the full test output (especially Memory Usage Summary)
2. Check kafka-connect logs: `docker logs test-kafka-connect`
3. Note the cycle where memory exceeded 90%
4. Capture any error traces mentioning MemoryThreshold or 429

This will help correlate the reproduction with the original Confluent Cloud issue.
