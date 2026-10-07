# IEX daily summaries

Daily per-symbol summaries of trades on the IEX exchange, built from IEX's free
HIST (TOPS) packet captures: first, high, low and last regular-session round-lot
trade price and IEX volume, one Parquet file per session in `data/daily/`.

Data provided for free by IEX. View IEX's Terms of Use:
https://iextrading.com/api-exhibit-a/

Notes:
- Prices are IEX-venue trades only: the last trade approximates, but is not, the
  official consolidated close. Volume is IEX volume only.
- Prices are not adjusted for splits or dividends.

Workflows: `backfill` (manual, half-month chunks in parallel) and `daily`
(scheduled, re-checks the last 7 days so gaps fill themselves).

Code is MIT licensed.
