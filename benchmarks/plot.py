"""Charts from benchmark results.

    pip install matplotlib
    python benchmarks/plot.py            # results/results.json vs results/baseline-0.14.1.json

Writes PNG files next to the results. Light surface, one validated palette:
blue for the current version, a muted gray for the baseline.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
from matplotlib.ticker import FuncFormatter  # noqa: E402

HERE = Path(__file__).resolve().parent / "results"

SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
GRID = "#e4e3df"
NEW = "#2a78d6"       # categorical slot 1
OTHER = "#eb6834"     # categorical slot 2
OLD = "#a3a29c"       # muted: the baseline is context, not the subject

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK_2, "text.color": INK,
    "xtick.color": INK_2, "ytick.color": INK_2, "axes.grid": True, "grid.color": GRID,
    "grid.linewidth": 0.8, "axes.spines.top": False, "axes.spines.right": False,
    "axes.titlesize": 13, "axes.titleweight": "semibold", "axes.titlelocation": "left",
    "axes.titlepad": 14, "font.size": 10.5, "lines.linewidth": 2, "lines.markersize": 7,
    "legend.frameon": False, "axes.axisbelow": True,
})


def load(path: Path) -> dict:
    return json.loads(path.read_text())["results"]


def row(rows, **match):
    return next(r for r in rows if all(r.get(k) == v for k, v in match.items()))


def save(fig, name: str) -> None:
    fig.tight_layout()
    fig.savefig(HERE / name, dpi=200)
    plt.close(fig)
    print(HERE / name)


def speedup(new: dict, old: dict) -> None:
    cases = [
        ("Straight chain, per node",
         row(old["chain"], nodes=500)["us_per_node"], row(new["chain"], nodes=500)["us_per_node"]),
        ("With declared types, per node",
         row(old["overhead"], variant="declared types")["us_per_node"],
         row(new["overhead"], variant="declared types")["us_per_node"]),
        ("With policy and limits, per node",
         row(old["overhead"], variant="policy + limits")["us_per_node"],
         row(new["overhead"], variant="policy + limits")["us_per_node"]),
        ("Parallel, per branch (1024)",
         row(old["parallel"], branches=1024)["us_per_branch"],
         row(new["parallel"], branches=1024)["us_per_branch"]),
        ("Load + validate, per node",
         row(old["load"], nodes=500)["us_per_node"], row(new["load"], nodes=500)["us_per_node"]),
        ("500 × 1 ms I/O at concurrency 64",
         row(old["concurrency"], concurrency=64)["median_s"] * 1e3,
         row(new["concurrency"], concurrency=64)["median_s"] * 1e3),
    ]
    cases.sort(key=lambda c: c[1] / c[2])
    fig, ax = plt.subplots(figsize=(8, 3.8))
    factors = [o / n for _, o, n in cases]
    bars = ax.barh([c[0] for c in cases], factors, color=NEW, height=0.55)
    for bar, (_, o, n), f in zip(bars, cases, factors):
        ax.text(bar.get_width() * 1.08, bar.get_y() + bar.get_height() / 2,
                f"×{f:.1f}   {o:.0f} → {n:.1f}", va="center", color=INK_2, fontsize=9.5)
    ax.set_xscale("log")
    ax.set_xlim(1, max(factors) * 12)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"×{v:g}"))
    ax.set_xlabel("times faster than 0.14.1 (log scale)")
    ax.grid(axis="y", visible=False)
    ax.set_title("Speed-up after profiling")
    save(fig, "speedup.png")


def overhead(new: dict) -> None:
    rows = sorted(new["overhead"], key=lambda r: r["us_per_node"])
    fig, ax = plt.subplots(figsize=(8, 3.6))
    bars = ax.barh([r["variant"] for r in rows], [r["us_per_node"] for r in rows],
                   color=NEW, height=0.55)
    for bar, r in zip(bars, rows):
        ax.text(bar.get_width() + 0.4, bar.get_y() + bar.get_height() / 2,
                f"{r['us_per_node']:.1f} µs", va="center", color=INK_2, fontsize=9.5)
    ax.set_xlabel("microseconds per node, 200-node chain")
    ax.set_xlim(0, max(r["us_per_node"] for r in rows) * 1.18)
    ax.grid(axis="y", visible=False)
    ax.set_title("What each feature costs")
    save(fig, "overhead.png")


def concurrency(new: dict, old: dict) -> None:
    def series(rows):
        limited = [r for r in rows if r["concurrency"] != "unlimited"]
        return [r["concurrency"] for r in limited], [r["items_per_s"] for r in limited]

    fig, ax = plt.subplots(figsize=(7, 4))
    x, y = series(new["concurrency"])
    xo, yo = series(old["concurrency"])
    ideal = [limit / 0.001 for limit in x]
    ax.plot(x, ideal, color=INK_2, linewidth=1, linestyle=(0, (4, 3)), label="ideal: limit / 1 ms")
    ax.plot(xo, yo, color=OLD, marker="o", label="0.14.1")
    ax.plot(x, y, color=NEW, marker="o", label="current")
    ax.set_xscale("log", base=2)
    ax.set_yscale("log")
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.set_xlabel("concurrency limit")
    ax.set_ylabel("items per second")
    ax.legend(loc="upper left")
    ax.set_title("A parallel map of 500 × 1 ms calls")
    save(fig, "concurrency.png")


def map_scaling(new: dict) -> None:
    fig, ax = plt.subplots(figsize=(7, 4))
    for mode, color in (("sequential", NEW), ("parallel", OTHER)):
        rows = [r for r in new["map"] if r["mode"] == mode]
        ax.plot([r["items"] for r in rows], [r["us_per_item"] for r in rows],
                color=color, marker="o", label=mode)
    ax.set_xscale("log")
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.set_ylim(bottom=0)
    ax.set_xlabel("items")
    ax.set_ylabel("microseconds per item")
    ax.legend()
    ax.set_title("Cost of one map iteration")
    save(fig, "map.png")


def frame(new: dict) -> None:
    rows = new["frame"]
    fig, ax = plt.subplots(figsize=(7, 3.6))
    ax.plot([r["variables"] for r in rows], [r["ns_per_write"] for r in rows],
            color=NEW, marker="o")
    ax.set_xscale("log")
    ax.set_ylim(0, max(r["ns_per_write"] for r in rows) * 1.3)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.set_xlabel("variables already in the frame")
    ax.set_ylabel("nanoseconds per write")
    ax.set_title("Writing into an immutable frame")
    save(fig, "frame.png")


def memory(new: dict) -> None:
    rows = [r for r in new.get("memory", []) if r["case"] == "chain"]
    if not rows:
        return
    fig, ax = plt.subplots(figsize=(7, 3.8))
    xs = [r["size"] for r in rows]
    ys = [r["peak_kib"] / 1024 for r in rows]
    ax.plot(xs, ys, color=NEW, marker="o")
    for x, y in zip(xs, ys):
        ax.annotate(f"{y:.1f} MiB", (x, y), xytext=(0, 9), textcoords="offset points",
                    ha="center", color=INK_2)
    cap = max(ys)
    ax.axhline(cap, color=OLD, linewidth=1, linestyle="--", zorder=0)
    ax.text(xs[0], cap * 1.04, "event history full: 10,000 events", color=INK_2, va="bottom")
    ax.set_xscale("log")
    ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:,.0f}"))
    ax.set_ylim(0, cap * 1.35)
    ax.set_xlabel("nodes walked in one run")
    ax.set_ylabel("peak memory of the run, MiB")
    ax.set_title("A long run's memory stops growing")
    save(fig, "memory.png")


def main() -> int:
    new = load(HERE / "results.json")
    baseline = HERE / "baseline-0.14.1.json"
    old = load(baseline) if baseline.exists() else None
    if old:
        speedup(new, old)
        concurrency(new, old)
    overhead(new)
    map_scaling(new)
    frame(new)
    memory(new)
    return 0


if __name__ == "__main__":
    sys.exit(main())
