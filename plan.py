"""Split a month range into half-month chunks for the backfill matrix.

Usage: python plan.py 2017-01 2026-10  ->  prints chunks=[{"start":..,"end":..},...]
Half months keep each job well under the 6-hour runner limit.
"""

from __future__ import annotations

import json
import sys

import pandas as pd


def main() -> None:
    first = pd.Period(sys.argv[1], "M")
    last = pd.Period(sys.argv[2], "M")
    chunks = []
    for m in pd.period_range(first, last, freq="M"):
        mid = m.start_time + pd.Timedelta(days=14)
        chunks.append({"start": m.start_time.strftime("%Y-%m-%d"), "end": mid.strftime("%Y-%m-%d")})
        chunks.append({"start": (mid + pd.Timedelta(days=1)).strftime("%Y-%m-%d"), "end": m.end_time.strftime("%Y-%m-%d")})
    if len(chunks) > 256:
        raise SystemExit("too many chunks for one matrix (max 256); split the range")
    print("chunks=" + json.dumps(chunks))


if __name__ == "__main__":
    main()
