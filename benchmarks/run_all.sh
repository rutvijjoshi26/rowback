#!/bin/sh
# Runs every benchmark sequentially; raw JSON goes to benchmarks/results/.
set -e
cd "$(dirname "$0")"
PY=${PY:-../.venv/bin/python}
$PY overhead.py --duration 60 --rows 1000000 --reps 3 --out overhead_saturated.json
$PY overhead.py --duration 60 --rows 1000000 --reps 3 --rate 5000 --out overhead_rate5000.json
$PY undo_vs_restore.py --sizes 1000000 5000000 10000000 --deletes 10000 50000 100000 --out undo_vs_restore.json
$PY capture_throughput.py --txns 300000 --bulk-rows 500000 --out capture_throughput.json
$PY summarize.py
