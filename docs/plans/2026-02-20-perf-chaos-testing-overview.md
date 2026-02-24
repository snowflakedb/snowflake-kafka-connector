# Performance and Chaos Testing - Overview

**Date:** 2026-02-20
**Purpose:** High-level summary for team alignment

---

## Test Scenarios

**Scenario 1: Channel Invalidation During AppendRow**
- **What:** Network partition causes SDK channel to become invalid mid-ingestion
- **How:** Toxiproxy drops all packets for 30s while connector is sending data
- **Expectations:** Connector reopens channel, resets offset, zero data loss/duplication, recovery <30s

**Scenario 2: Channel Invalidation During Flush**
- **What:** Connection failure during offset commit
- **How:** Toxiproxy sends TCP RST during flush operation
- **Expectations:** Offset token recovery succeeds, zero data loss/duplication

**Scenario 3: Memory Backpressure**
- **What:** SDK buffer saturation triggers retry logic
- **How:** Docker memory limit (1GB) on Connect container with high-volume data
- **Expectations:** SDK retries with backoff, no data loss, graceful throughput degradation

**Scenario 4: Bandwidth Throttling**
- **What:** Slow network simulates rate limiting
- **How:** Toxiproxy limits bandwidth to 1MB/s
- **Expectations:** Connector handles slow writes, all data arrives, no crashes

**Scenario 5: Sequential API Failures**
- **What:** Multiple failures in single batch
- **How:** Network failure during appendRow, then again during getOffsetToken recovery
- **Expectations:** Connector recovers from both, maintains correct offset

**Scenario 6: Performance Baseline - Throughput**
- **What:** Establish throughput characteristics
- **How:** Send 1M records (100MB) with no chaos, measure MB/s and records/s
- **Expectations:** Throughput within 10% of baseline

**Scenario 7: Performance Baseline - Latency**
- **What:** Validate end-to-end latency SLA
- **How:** Track timestamps from Kafka produce to Snowflake visibility
- **Expectations:** p99 latency <15s

**Scenario 8: CPU Starvation**
- **What:** Limited CPU resources
- **How:** Docker CPU limit (0.5 cores) on Connect container
- **Expectations:** Connector remains functional, throughput degrades gracefully, no crashes

**Scenario 9: OOM Recovery**
- **What:** Task restart after memory exhaustion
- **How:** Memory limit triggers OOM, Kafka Connect restarts task
- **Expectations:** Recovers from last committed offset, zero data loss/duplication

---

## CI Integration

- Runs on every PR via `end-to-end-stress.yml`
- Target runtime: 5-15 minutes
- Baselines stored in git (`test/baselines/`)
- CI fails if: data loss, duplicates, >10% perf regression, recovery >30s
