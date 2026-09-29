"""Pass oracle for the KC SPCS release test (SNOW-4202412).

Pure functions only: no Snowflake, no I/O, so it is unit-testable offline.
"""
import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

ERR_CODES = ("390422", "395090")

# A = no network policy, B = policy with COMPUTE_POOL rule,
# C = policy with IPv4 rule only (documented failure: 390422, zero rows).
EXPECT_ROWS = "rows"
EXPECT_390422 = "390422"
CELL_EXPECTATIONS = {"A": EXPECT_ROWS, "B": EXPECT_ROWS, "C": EXPECT_390422}
NON_TERMINAL = ("RUNNING", "PENDING", "UNKNOWN")


@dataclass
class Observation:
    cell: str
    rows: int
    job_status: Optional[str]  # DONE / FAILED / RUNNING / ... (None = unknown)
    logs: str = ""


@dataclass
class Verdict:
    cell: str
    passed: bool
    reasons: List[str] = field(default_factory=list)


def parse_logs(logs: str) -> Dict[str, Optional[int]]:
    """Extract E2E_EXIT and error-code counts from harness output.

    Counts are the max of the harness's E2E_ERR summary and raw occurrences
    in the text, so a missing summary (container killed) cannot hide errors.
    """
    out: Dict[str, Optional[int]] = {"exit": None}
    m = re.findall(r"E2E_EXIT=(-?\d+)", logs)
    if m:
        out["exit"] = int(m[-1])
    for code in ERR_CODES:
        summary = [int(n) for n in re.findall(r"E2E_ERR %s=(\d+)" % code, logs)]
        raw = len(re.findall(r"(?<!E2E_ERR )%s(?!=)" % code, logs))
        out[code] = max(summary + [raw])
    return out


def evaluate(obs: Observation, nrecords: int) -> Verdict:
    """Decide pass/fail for one cell. A non-terminal job is never a pass."""
    expect = CELL_EXPECTATIONS.get(obs.cell)
    if expect is None:
        return Verdict(obs.cell, False, ["unknown cell %r" % obs.cell])
    p = parse_logs(obs.logs)
    status = (obs.job_status or "UNKNOWN").upper()
    reasons: List[str] = []
    if status in NON_TERMINAL:
        reasons.append("job status %s is not terminal" % status)

    if expect == EXPECT_ROWS:
        if obs.rows < nrecords:
            reasons.append("rows %d < %d" % (obs.rows, nrecords))
        for code in ERR_CODES:
            if p[code]:
                reasons.append("%s seen %d times" % (code, p[code]))
        if p["exit"] != 0:
            reasons.append("E2E_EXIT=%s (want 0)" % p["exit"])
        if status not in NON_TERMINAL and status != "DONE":
            reasons.append("job status %s (want DONE)" % status)
    else:
        if obs.rows != 0:
            reasons.append("rows %d (want 0): possible GS behavior change" % obs.rows)
        if not p["390422"]:
            reasons.append("390422 not seen: possible GS behavior change")

    return Verdict(obs.cell, not reasons, reasons)
