#!/usr/bin/env python3
"""Turns bench/results.jsonl into markdown tables (best run per port and scenario)."""
import json
import sys
from collections import defaultdict

path = sys.argv[1] if len(sys.argv) > 1 else "bench/results.jsonl"
rows = [json.loads(l) for l in open(path) if l.strip().startswith("{")]

best = {}
for r in rows:
    r.setdefault("recv_ms", r["elapsed_ms"])
    key = (r["lang"], r["scenario"])
    if key not in best or r["recv_ms"] < best[key]["recv_ms"]:
        best[key] = r

langs = [l for l in ["go", "java", "js", "rust"] if any(k[0] == l for k in best)]
scenarios = []
for r in rows:
    if r["scenario"] not in scenarios:
        scenarios.append(r["scenario"])

names = {"go": "Go", "java": "Java", "js": "JS/TS", "rust": "Rust"}

def rate(r):
    return r["msgs"] / (r["recv_ms"] / 1000.0)

print("### Delivery throughput (messages/s, best of runs)\n")
print("| Scenario | " + " | ".join(names[l] for l in langs) + " |")
print("|---|" + "---:|" * len(langs))
for s in scenarios:
    cells = []
    for l in langs:
        r = best.get((l, s))
        cells.append(f"{rate(r):,.0f}" if r else "n/a")
    print(f"| {s} | " + " | ".join(cells) + " |")

print("\n### Throughput relative to the no-commit baseline of the same port\n")
print("| Scenario | " + " | ".join(names[l] for l in langs) + " |")
print("|---|" + "---:|" * len(langs))
for s in scenarios:
    cells = []
    for l in langs:
        r = best.get((l, s)); b = best.get((l, "none"))
        cells.append(f"{100 * rate(r) / rate(b):.0f}%" if r and b else "n/a")
    print(f"| {s} | " + " | ".join(cells) + " |")

print("\n### Wall time for 2M messages including final commit and close (seconds), commits performed\n")
print("| Scenario | " + " | ".join(names[l] for l in langs) + " |")
print("|---|" + "---:|" * len(langs))
for s in scenarios:
    cells = []
    for l in langs:
        r = best.get((l, s))
        if not r:
            cells.append("n/a"); continue
        commits = "timer" if s.startswith("auto") else f"{r['commits']:,}"
        cells.append(f"{r['elapsed_ms'] / 1000:.2f}s / {commits}")
    print(f"| {s} | " + " | ".join(cells) + " |")

print("\n### Commit cost (microseconds of added wall time per commit, versus the no-commit baseline)\n")
print("| Scenario | " + " | ".join(names[l] for l in langs) + " |")
print("|---|" + "---:|" * len(langs))
for s in scenarios:
    if s == "none" or s.startswith("auto"):
        continue
    cells = []
    for l in langs:
        r = best.get((l, s)); b = best.get((l, "none"))
        if not r or not b or r["commits"] == 0:
            cells.append("n/a"); continue
        added_us = (r["recv_ms"] - b["recv_ms"]) * 1000.0 / r["commits"]
        cells.append(f"{added_us:,.0f}")
    print(f"| {s} | " + " | ".join(cells) + " |")
