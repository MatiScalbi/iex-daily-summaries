"""Build daily IEX trade summaries for a date range.

Usage: python summarize.py START END [OUT_DIR]
Dates are YYYY-MM-DD. Days already present in OUT_DIR are skipped, so the
command is resumable and safe to re-run. Weekends are skipped; exchange
holidays simply have no HIST files.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import pandas as pd
import requests

from iex_reader import summarize_day


def main() -> None:
    start, end = sys.argv[1], sys.argv[2]
    out = Path(sys.argv[3] if len(sys.argv) > 3 else "data/daily")
    out.mkdir(parents=True, exist_ok=True)
    session = requests.Session()
    done, failed = 0, []
    for day in pd.bdate_range(start, end):
        key = day.strftime("%Y%m%d")
        if (out / f"{key}.parquet").exists():
            continue
        t0 = time.time()
        try:
            df = summarize_day(key, out, session)
        except Exception as exc:  # keep going; the day is retried on the next run
            failed.append(key)
            print(f"{key} FAILED: {exc}", flush=True)
            continue
        if df is None:
            print(f"{key} no files (holiday)", flush=True)
            continue
        done += 1
        print(f"{key} {len(df)} symbols in {time.time() - t0:.0f}s", flush=True)
    print(f"done={done} failed={failed}")


if __name__ == "__main__":
    main()
