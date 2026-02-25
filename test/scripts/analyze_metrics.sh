#!/bin/bash
#
# Analyze scraped JMX metrics from a JSONL file produced by scrape_metrics.sh.
#
# Usage:
#   ./analyze_metrics.sh <file>              Aggregate summary (default)
#   ./analyze_metrics.sh <file> detail       Per-task breakdown
#   ./analyze_metrics.sh <file> lag          Per-channel offset lag
#
# If <file> is omitted, uses the most recent /tmp/sf-metrics-*.jsonl.

set -e

FILE="${1}"
MODE="${2:-summary}"

if [ -z "$FILE" ]; then
    FILE=$(ls -t /tmp/sf-metrics-*.jsonl 2>/dev/null | head -1)
    if [ -z "$FILE" ]; then
        echo "No metrics file found. Provide a path or run scrape_metrics.sh first." >&2
        exit 1
    fi
fi

if [ ! -f "$FILE" ]; then
    echo "File not found: $FILE" >&2
    exit 1
fi

exec python3 - "$FILE" "$MODE" <<'PYEOF'
import json, sys, re
from collections import defaultdict

file_path = sys.argv[1]
mode = sys.argv[2]

snapshots = []
with open(file_path) as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        data = json.loads(line)
        if data.get("metrics"):
            snapshots.append(data)

if not snapshots:
    print("No metric snapshots with data found.")
    sys.exit(0)

last = snapshots[-1]
first = snapshots[0]
metrics = last["metrics"]

# ── helpers ──────────────────────────────────────────────────────────────────

def parse_mbean(key):
    """Extract connector, task/channel, category, name from an MBean key."""
    parts = {}
    _, _, attrs = key.partition(":")
    for token in attrs.split(","):
        k, _, v = token.partition("=")
        parts[k] = v
    return parts

def fmt_dur(seconds):
    if seconds is None or seconds == 0:
        return "     -"
    if seconds < 0.001:
        return f"{seconds*1e6:6.1f}us"
    if seconds < 1:
        return f"{seconds*1e3:6.1f}ms"
    return f"{seconds:6.2f}s "

def fmt_count(n):
    return f"{n:,}"

def fmt_rate(r):
    if r < 1:
        return f"{r:.3f}"
    if r < 1000:
        return f"{r:.1f}"
    return f"{r:,.0f}"

def aggregate_timer(timer_name, timers_by_task):
    """Aggregate a Timer across all tasks: weighted mean/p50/p95 and worst-case max."""
    total_count = 0
    weighted_mean = 0.0
    weighted_p50 = 0.0
    weighted_p95 = 0.0
    worst_max = 0.0
    for task, timers in sorted(timers_by_task.items()):
        t = timers.get(timer_name)
        if not t:
            continue
        c = t.get("Count", 0)
        total_count += c
        if c > 0:
            weighted_mean += t.get("Mean", 0) * c
            weighted_p50 += t.get("50thPercentile", 0) * c
            weighted_p95 += t.get("95thPercentile", 0) * c
            worst_max = max(worst_max, t.get("Max", 0))
    if total_count > 0:
        weighted_mean /= total_count
        weighted_p50 /= total_count
        weighted_p95 /= total_count
    return total_count, weighted_mean, weighted_p50, weighted_p95, worst_max

def print_timer_row(label, timer_name, timers_by_task):
    count, mean, p50, p95, mx = aggregate_timer(timer_name, timers_by_task)
    print(f"  {label:<24} {fmt_count(count):>8}  "
          f"{fmt_dur(mean)}  {fmt_dur(p50)}  "
          f"{fmt_dur(p95)}  {fmt_dur(mx)}")

# ── classify metrics ─────────────────────────────────────────────────────────

task_timers = defaultdict(dict)
task_counters = defaultdict(dict)
task_meters = defaultdict(dict)
task_gauges = defaultdict(dict)
channel_gauges = defaultdict(dict)
channel_counters = defaultdict(dict)

# Accept both old (latency) and new (duration) names for backward compat
TIMER_NAMES = {
    "put-duration", "precommit-duration",
    "put-latency", "precommit-latency",
    "open-duration", "close-duration", "start-duration",
    "channel-open-duration", "sdk-client-create-duration",
    "precommit-offset-fetch-duration",
}
METER_NAMES = {"put-records"}
COUNTER_NAMES = {"open-count", "close-count", "precommit-partitions-skipped", "channel-open-count"}
GAUGE_NAMES = {"assigned-partitions", "sdk-client-count"}
OFFSET_NAMES = {"latest-consumer-offset", "persisted-in-snowflake-offset", "processed-offset"}
CHANNEL_COUNTER_NAMES = {"channel-recovery-count"}

for key, val in metrics.items():
    p = parse_mbean(key)
    task = p.get("task", "")
    channel = p.get("channel", "")
    name = p.get("name", "")
    cat = p.get("category", "")

    if task.startswith("task-"):
        if name in TIMER_NAMES:
            task_timers[task][name] = val
        elif name in METER_NAMES:
            task_meters[task][name] = val
        elif name in COUNTER_NAMES:
            task_counters[task][name] = val
        elif name in GAUGE_NAMES:
            task_gauges[task][name] = val
    elif cat == "offsets":
        if name in OFFSET_NAMES:
            channel_gauges[channel][name] = val
        elif name in CHANNEL_COUNTER_NAMES:
            channel_counters[channel][name] = val

num_tasks = len(set(list(task_timers.keys()) + list(task_counters.keys())
                    + list(task_meters.keys()) + list(task_gauges.keys())))
num_channels = len(channel_gauges)
num_snapshots = len(snapshots)

# Detect whether we have the new "duration" names or old "latency" names
has_new_names = any("put-duration" in t for t in task_timers.values())
PUT_TIMER = "put-duration" if has_new_names else "put-latency"
PRECOMMIT_TIMER = "precommit-duration" if has_new_names else "precommit-latency"

# ── header ───────────────────────────────────────────────────────────────────

print("=" * 72)
print("  Snowflake Kafka Connector - Metrics Analysis")
print("=" * 72)
print(f"  File:       {file_path}")
print(f"  Snapshots:  {num_snapshots}  ({first['timestamp']} .. {last['timestamp']})")
print(f"  Tasks:      {num_tasks}")
print(f"  Channels:   {num_channels}")
print()

# ── summary mode (default) ───────────────────────────────────────────────────

if mode == "summary":

    # --- method durations ---
    print("-" * 72)
    print("  Method Durations  (aggregated across all tasks, last snapshot)")
    print("-" * 72)
    hdr = f"  {'method':<24} {'calls':>8}  {'mean':>8}  {'p50':>8}  {'p95':>8}  {'max':>8}"
    print(hdr)

    print_timer_row("put()", PUT_TIMER, task_timers)
    print_timer_row("preCommit()", PRECOMMIT_TIMER, task_timers)
    print_timer_row("  offset fetch (SDK)", "precommit-offset-fetch-duration", task_timers)
    print()

    # --- lifecycle durations ---
    print("-" * 72)
    print("  Lifecycle Durations  (aggregated across all tasks)")
    print("-" * 72)
    print(hdr)

    print_timer_row("start()", "start-duration", task_timers)
    print_timer_row("open()", "open-duration", task_timers)
    print_timer_row("close()", "close-duration", task_timers)
    print_timer_row("channel open (SDK)", "channel-open-duration", task_timers)
    print_timer_row("SDK client create", "sdk-client-create-duration", task_timers)
    print()

    # --- throughput ---
    print("-" * 72)
    print("  Throughput")
    print("-" * 72)
    total_records = 0
    total_mean_rate = 0.0
    total_1m_rate = 0.0
    for task, meters in sorted(task_meters.items()):
        m = meters.get("put-records")
        if m:
            total_records += m.get("Count", 0)
            total_mean_rate += m.get("MeanRate", 0)
            total_1m_rate += m.get("OneMinuteRate", 0)

    print(f"  Total records ingested:  {fmt_count(total_records)}")
    print(f"  Mean rate:               {fmt_rate(total_mean_rate)} records/sec")
    print(f"  1-minute rate:           {fmt_rate(total_1m_rate)} records/sec")

    total_skipped = sum(
        c.get("precommit-partitions-skipped", {}).get("Count", 0)
        for c in task_counters.values()
    )
    print(f"  preCommit partitions skipped: {fmt_count(total_skipped)}")
    print()

    # --- lifecycle counts ---
    print("-" * 72)
    print("  Lifecycle Counts  (across all tasks)")
    print("-" * 72)
    total_open = sum(c.get("open-count", {}).get("Count", 0) for c in task_counters.values())
    total_close = sum(c.get("close-count", {}).get("Count", 0) for c in task_counters.values())
    total_assigned = sum(g.get("assigned-partitions", {}).get("Value", 0) for g in task_gauges.values())
    total_channel_open = sum(c.get("channel-open-count", {}).get("Count", 0) for c in task_counters.values())
    total_sdk_clients = sum(g.get("sdk-client-count", {}).get("Value", 0) for g in task_gauges.values())

    print(f"  open() calls:            {fmt_count(total_open)}")
    print(f"  close() calls:           {fmt_count(total_close)}")
    print(f"  assigned partitions:     {fmt_count(total_assigned)}  (current)")
    print(f"  channel opens (total):   {fmt_count(total_channel_open)}")
    print(f"  SDK clients (current):   {fmt_count(total_sdk_clients)}")

    total_recovery = sum(
        c.get("channel-recovery-count", {}).get("Count", 0)
        for c in channel_counters.values()
    )
    print(f"  channel recoveries:      {fmt_count(total_recovery)}")
    print()

    # --- offset lag (computed from raw offsets) ---
    print("-" * 72)
    print("  Offset Lag Summary  (last snapshot)")
    print("-" * 72)
    lags = []
    for ch, gauges in channel_gauges.items():
        consumer = gauges.get("latest-consumer-offset", {}).get("Value")
        persisted = gauges.get("persisted-in-snowflake-offset", {}).get("Value")
        if consumer is not None and persisted is not None and consumer >= 0 and persisted >= 0:
            lags.append(max(0, consumer - persisted))

    if lags:
        with_lag = sum(1 for l in lags if l > 0)
        print(f"  Channels:          {len(lags)}")
        print(f"  With lag > 0:      {with_lag}")
        print(f"  Max lag:           {max(lags)}")
        print(f"  Mean lag:          {sum(lags)/len(lags):.1f}")
    else:
        print("  (no offset metrics found)")
    print()

# ── detail mode ──────────────────────────────────────────────────────────────

elif mode == "detail":
    all_tasks = sorted(
        set(list(task_timers.keys()) + list(task_counters.keys())
            + list(task_meters.keys()) + list(task_gauges.keys())),
        key=lambda t: int(re.search(r"(\d+)$", t).group(1)) if re.search(r"(\d+)$", t) else 0,
    )

    DETAIL_TIMERS = [
        ("put()", PUT_TIMER),
        ("preCommit()", PRECOMMIT_TIMER),
        ("  offset fetch", "precommit-offset-fetch-duration"),
        ("open()", "open-duration"),
        ("close()", "close-duration"),
        ("start()", "start-duration"),
        ("channel open", "channel-open-duration"),
        ("SDK client create", "sdk-client-create-duration"),
    ]

    print("-" * 72)
    print("  Per-Task Breakdown  (last snapshot)")
    print("-" * 72)

    for task in all_tasks:
        assigned = task_gauges.get(task, {}).get("assigned-partitions", {}).get("Value", 0)
        opens = task_counters.get(task, {}).get("open-count", {}).get("Count", 0)
        closes = task_counters.get(task, {}).get("close-count", {}).get("Count", 0)
        ch_opens = task_counters.get(task, {}).get("channel-open-count", {}).get("Count", 0)
        sdk_clients = task_gauges.get(task, {}).get("sdk-client-count", {}).get("Value", 0)

        print(f"\n  {task}  (partitions={assigned}, opens={opens}, closes={closes},"
              f" ch_opens={ch_opens}, sdk_clients={sdk_clients})")

        for label, timer_name in DETAIL_TIMERS:
            t = task_timers.get(task, {}).get(timer_name)
            if not t or t.get("Count", 0) == 0:
                continue
            c = t.get("Count", 0)
            print(f"    {label:<20}  calls={fmt_count(c):>6}"
                  f"  mean={fmt_dur(t.get('Mean',0))}"
                  f"  p50={fmt_dur(t.get('50thPercentile',0))}"
                  f"  p95={fmt_dur(t.get('95thPercentile',0))}"
                  f"  max={fmt_dur(t.get('Max',0))}")

        m = task_meters.get(task, {}).get("put-records")
        if m:
            print(f"    {'records':<20}  total={fmt_count(m.get('Count',0)):>6}"
                  f"  mean_rate={fmt_rate(m.get('MeanRate',0))} rec/s"
                  f"  1m_rate={fmt_rate(m.get('OneMinuteRate',0))} rec/s")

        skipped = task_counters.get(task, {}).get("precommit-partitions-skipped", {}).get("Count", 0)
        if skipped > 0:
            print(f"    precommit-skipped: {skipped}")

    print()

# ── lag mode ─────────────────────────────────────────────────────────────────

elif mode == "lag":
    print("-" * 72)
    print("  Offset Lag by Channel  (last snapshot)")
    print("-" * 72)

    if not channel_gauges:
        print("  (no channel metrics found)")
    else:
        rows = []
        for ch, gauges in sorted(channel_gauges.items()):
            processed = gauges.get("processed-offset", {}).get("Value", -1)
            consumer = gauges.get("latest-consumer-offset", {}).get("Value", -1)
            persisted = gauges.get("persisted-in-snowflake-offset", {}).get("Value", -1)
            lag = max(0, consumer - persisted) if consumer >= 0 and persisted >= 0 else 0
            recovery = channel_counters.get(ch, {}).get("channel-recovery-count", {}).get("Count", 0)

            m = re.search(r"_(\d+)$", ch)
            part_id = m.group(1) if m else "?"
            short = ch if len(ch) <= 40 else f"...{ch[-37:]}"
            rows.append((part_id, short, lag, processed, consumer, persisted, recovery))

        hdr = f"  {'channel':<42} {'lag':>5}  {'processed':>10}  {'consumer':>10}  {'persisted':>10}  {'recover':>7}"
        print(hdr)
        for part_id, short, lag, processed, consumer, persisted, recovery in sorted(rows, key=lambda r: int(r[0]) if r[0].isdigit() else 0):
            flag = " *" if lag > 0 else "  "
            rec_flag = f"  {recovery:>7}" if recovery > 0 else f"  {recovery:>7}"
            print(f"  {short:<42} {lag:>5}{flag}"
                  f"  {processed:>10}  {consumer:>10}  {persisted:>10}{rec_flag}")

    print()

else:
    print(f"Unknown mode: {mode}", file=sys.stderr)
    print("Usage: analyze_metrics.sh <file> [summary|detail|lag]", file=sys.stderr)
    sys.exit(1)
PYEOF
