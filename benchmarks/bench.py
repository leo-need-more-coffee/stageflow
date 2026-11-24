"""StageFlow benchmarks.

    python benchmarks/bench.py                 # the full set, a few minutes
    python benchmarks/bench.py --quick         # smaller sizes, for a smoke run
    python benchmarks/bench.py --only map      # benchmarks whose name contains "map"

Writes `benchmarks/results/results.json` and `results.md`. Every number is
the median of several repeats after a warm-up, measured with
`time.perf_counter` inside one event loop.

What each benchmark answers:

- chain        how fast the walker goes: nodes per second along a straight line
- overhead     what each feature costs on the same 200-node chain
- map          a loop over N items, sequential vs parallel
- concurrency  1 ms of simulated I/O per item, against the concurrency limit
- parallel     a fan-out of N branches
- frame        writing into a frame that already holds N variables
- load         Pipeline.from_dict + validate for graphs of N nodes
- cel          condition evaluation, with the compiled-expression cache
"""
from __future__ import annotations

import argparse
import asyncio
import json
import platform
import statistics
import sys
import time
from pathlib import Path
from typing import Any, Awaitable, Callable

import stageflow
from stageflow import (
    BaseStage,
    Context,
    Limits,
    Pipeline,
    Policy,
    Session,
    StepDebugger,
    register_stage,
)
try:
    from stageflow.observability import Recorder
except ImportError:  # an older StageFlow, measured for comparison
    Recorder = None

HERE = Path(__file__).resolve().parent

#: The largest straight graph to build (`--max-graph`). Releases before 0.15
#: hit Python's recursion limit validating a chain of about a thousand nodes.
MAX_GRAPH = 10**9


def sizes(*candidates: int) -> list[int]:
    return [n for n in candidates if n <= MAX_GRAPH]


# ------------------------------------------------------------------ stages

@register_stage("BenchNoop")
class BenchNoop(BaseStage):
    """
    description: "Writes its argument back"
    arguments:
      v: any
    outputs:
      v: any
    """

    timeout = None

    async def run(self):
        self.set_outputs({"v": self.arguments.get("v")})


@register_stage("BenchTimed")
class BenchTimed(BenchNoop):
    """
    description: "BenchNoop with the default 30-second stage timeout"
    arguments:
      v: any
    outputs:
      v: any
    """

    timeout = 30


@register_stage("BenchIO")
class BenchIO(BaseStage):
    """
    description: "Waits a millisecond, like a quick network call"
    outputs:
      v: any
    """

    timeout = None

    async def run(self):
        await asyncio.sleep(0.001)
        self.set_outputs({"v": 1})


# ------------------------------------------------------------------ graphs

def chain(n: int, typed: bool = False, stage: str = "BenchNoop") -> dict:
    nodes = [{"id": "start", "type": "entry", "variables": {"x": 0}, "next": "n0"}]
    for i in range(n):
        nodes.append({"id": f"n{i}", "type": "stage", "stage": stage,
                      "arguments": {"vars": {"v": "x"}}, "outputs": {"v": "x"},
                      "next": f"n{i + 1}" if i < n - 1 else "end"})
    nodes.append({"id": "end", "type": "terminal", "artifacts": ["x"]})
    data = {"nodes": nodes}
    if typed:
        data["variables"] = {"x": "int"}
    return data


def map_graph(n: int, mode: str, stage: str = "BenchNoop") -> dict:
    return {"nodes": [
        {"id": "start", "type": "entry", "variables": {"xs": list(range(n))}, "next": "m"},
        {"id": "m", "type": "map", "items": "vars.xs", "body": "b", "mode": mode,
         "collect": {"v": "out"}, "next": "end"},
        {"id": "b", "type": "stage", "stage": stage,
         "arguments": {"vars": {"v": "item"}}, "outputs": {"v": "v"}},
        {"id": "end", "type": "terminal"},
    ]}


def parallel_graph(n: int) -> dict:
    nodes = [{"id": "start", "type": "entry", "next": "p"},
             {"id": "p", "type": "parallel", "branches": [f"b{i}" for i in range(n)],
              "next": "end"}]
    nodes += [{"id": f"b{i}", "type": "stage", "stage": "BenchNoop",
               "arguments": {"const": {"v": i}}, "outputs": {"v": f"v{i}"}}
              for i in range(n)]
    nodes.append({"id": "end", "type": "terminal"})
    return {"nodes": nodes}


# ------------------------------------------------------------------ timing

async def measure(make: Callable[[], Awaitable[Any]], repeat: int,
                  warmup: int = 1) -> dict[str, float]:
    for _ in range(warmup):
        await make()
    samples = []
    for _ in range(repeat):
        started = time.perf_counter()
        await make()
        samples.append(time.perf_counter() - started)
    return {"median_s": statistics.median(samples), "min_s": min(samples),
            "max_s": max(samples), "repeat": repeat}


def runner(data: dict, **session_kwargs) -> Callable[[], Awaitable[Any]]:
    pipeline = Pipeline.from_dict(data)

    async def go():
        result = await Session(id="bench", pipeline=pipeline, **session_kwargs).run()
        if result.result and result.result.get("status") == "budget_exceeded":
            raise RuntimeError(f"a benchmark ran out of budget: {result.result}")
        return result

    return go


# ------------------------------------------------------------------ benchmarks

async def bench_chain(quick: bool) -> list[dict]:
    rows = []
    for n in (sizes(10, 100, 1000) if quick else sizes(10, 100, 500, 1000, 5000)):
        timing = await measure(runner(chain(n)), repeat=5 if quick else 9)
        steps = n + 2
        rows.append({"nodes": n, **timing,
                     "us_per_node": timing["median_s"] / steps * 1e6,
                     "nodes_per_s": steps / timing["median_s"]})
    return rows


async def bench_overhead(quick: bool) -> list[dict]:
    data, typed = chain(200), chain(200, typed=True)
    limits = Limits(counters={"steps": 10_000, "seconds": 60},
                    gauges={"concurrency": 8, "depth": 4})
    variants: list[tuple[str, dict, dict]] = [
        ("bare", data, {}),
        ("stage timeout (the 30 s default)", chain(200, stage="BenchTimed"), {}),
        ("policy + limits", data, {"policy": Policy(
            stages={"BenchNoop"}, node_types={"entry", "stage", "terminal"}, limits=limits)}),
        ("frame_bytes limit", data, {"limits": Limits(gauges={"frame_bytes": 10_000_000})}),
        ("declared types", typed, {}),
        ("debugger (run mode)", data, {"debugger": None}),
    ]
    if Recorder is not None:
        variants.append(("Recorder", data, {"instrument": None}))
    try:
        from opentelemetry.sdk.trace import TracerProvider
        from stageflow.observability.otel import OpenTelemetryInstrument
        otel = OpenTelemetryInstrument(tracer_provider=TracerProvider())
        variants.append(("OpenTelemetry (spans, no exporter)", data, {"instrument": otel}))
    except ImportError:  # pragma: no cover
        pass

    rows = []
    base = None
    for name, graph, kwargs in variants:
        pipeline = Pipeline.from_dict(graph)

        async def go(kwargs=kwargs, pipeline=pipeline, name=name):
            extra = dict(kwargs)
            if name.startswith("debugger"):
                extra["debugger"] = StepDebugger(mode="run")
            if name == "Recorder":
                extra["instrument"] = Recorder()
            await Session(id="b", pipeline=pipeline, **extra).run()

        timing = await measure(go, repeat=7 if quick else 15)
        base = base or timing["median_s"]
        rows.append({"variant": name, **timing,
                     "us_per_node": timing["median_s"] / 202 * 1e6,
                     "relative": timing["median_s"] / base})
    return rows


async def bench_map(quick: bool) -> list[dict]:
    rows = []
    for n in ([10, 100, 1000] if quick else [10, 100, 1000, 10_000]):
        for mode in ("sequential", "parallel"):
            timing = await measure(runner(map_graph(n, mode)), repeat=3 if quick else 5)
            rows.append({"items": n, "mode": mode, **timing,
                         "us_per_item": timing["median_s"] / n * 1e6})
    return rows


async def bench_concurrency(quick: bool) -> list[dict]:
    n = 200 if quick else 500
    rows = []
    for limit in [1, 4, 16, 64, None]:
        limits = Limits(gauges={"concurrency": limit}) if limit else None
        timing = await measure(runner(map_graph(n, "parallel", "BenchIO"), limits=limits),
                               repeat=3, warmup=1)
        rows.append({"items": n, "concurrency": limit or "unlimited", **timing,
                     "ideal_s": n * 0.001 / limit if limit else 0.001,
                     "items_per_s": n / timing["median_s"]})
    return rows


async def bench_parallel(quick: bool) -> list[dict]:
    rows = []
    for n in ([2, 16, 128] if quick else [2, 16, 128, 1024]):
        timing = await measure(runner(parallel_graph(n)), repeat=5)
        rows.append({"branches": n, **timing, "us_per_branch": timing["median_s"] / n * 1e6})
    return rows


async def bench_frame(quick: bool) -> list[dict]:
    rows = []
    writes = 10_000
    for size in ([10, 1000, 100_000] if quick else [10, 1000, 10_000, 100_000, 1_000_000]):
        ctx = Context({f"v{i}": i for i in range(size)})

        async def go(ctx=ctx):
            c = ctx
            for i in range(writes):
                c = c.with_var("hot", i)
            return c

        timing = await measure(go, repeat=5)
        rows.append({"variables": size, "writes": writes, **timing,
                     "ns_per_write": timing["median_s"] / writes * 1e9})
    return rows


async def bench_load(quick: bool) -> list[dict]:
    rows = []
    for n in (sizes(10, 100, 1000) if quick else sizes(10, 100, 500, 1000, 5000)):
        data = chain(n)

        async def parse(data=data):
            return Pipeline.from_dict(data)

        async def parse_validate(data=data):
            Pipeline.from_dict(data).validate()

        parsed = await measure(parse, repeat=3 if n >= 1000 else 7)
        full = await measure(parse_validate, repeat=3 if n >= 1000 else 7)
        rows.append({"nodes": n, "from_dict_ms": parsed["median_s"] * 1e3,
                     "from_dict_validate_ms": full["median_s"] * 1e3,
                     "validate_ms": (full["median_s"] - parsed["median_s"]) * 1e3,
                     "us_per_node": full["median_s"] / n * 1e6})
    return rows


async def bench_cel(quick: bool) -> list[dict]:
    from stageflow import CelEngine
    engine = CelEngine()
    rows = []
    cases = {
        "comparison": ("vars.x > 10", {"x": 5}),
        "macro over 100": ("vars.xs.exists(i, i == 99)", {"xs": list(range(100))}),
        "string ops": ("vars.s.startsWith('a') && size(vars.s) > 3", {"s": "abcdef"}),
    }
    evals = 2000 if quick else 10_000
    for name, (expr, vars_) in cases.items():
        ctx = Context(vars_)

        async def go(expr=expr, ctx=ctx):
            for _ in range(evals):
                engine.eval(expr, ctx)

        timing = await measure(go, repeat=5)
        rows.append({"expression": name, "evals": evals, **timing,
                     "us_per_eval": timing["median_s"] / evals * 1e6})
    return rows


BENCHMARKS = {
    "chain": bench_chain,
    "overhead": bench_overhead,
    "map": bench_map,
    "concurrency": bench_concurrency,
    "parallel": bench_parallel,
    "frame": bench_frame,
    "load": bench_load,
    "cel": bench_cel,
}


# ------------------------------------------------------------------ output

def as_markdown(results: dict[str, list[dict]], machine: dict) -> str:
    lines = ["# StageFlow benchmark results", "",
             f"StageFlow {machine['stageflow']}, Python {machine['python']}, "
             f"{machine['cpu']}, {machine['platform']}.", "",
             "Medians of several repeats after a warm-up.", ""]
    for name, rows in results.items():
        if not rows:
            continue
        lines += [f"## {name}", ""]
        keys = [k for k in rows[0] if k not in ("min_s", "max_s", "repeat")]
        lines.append("| " + " | ".join(keys) + " |")
        lines.append("|" + "---|" * len(keys))
        for row in rows:
            lines.append("| " + " | ".join(fmt(row[k]) for k in keys) + " |")
        lines.append("")
    return "\n".join(lines)


def fmt(value: Any) -> str:
    if isinstance(value, float):
        if value >= 1000:
            return f"{value:,.0f}"
        if value >= 10:
            return f"{value:.1f}"
        return f"{value:.4g}"
    return str(value)


def machine_info() -> dict:
    cpu = platform.processor() or platform.machine()
    try:
        for line in Path("/proc/cpuinfo").read_text().splitlines():
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    except OSError:
        pass
    return {"stageflow": stageflow.__version__, "python": platform.python_version(),
            "cpu": cpu, "platform": platform.platform(terse=True)}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--quick", action="store_true", help="smaller sizes")
    parser.add_argument("--only", help="run only benchmarks whose name contains this")
    parser.add_argument("--out", type=Path, default=HERE / "results")
    parser.add_argument("--max-graph", type=int, help="skip straight graphs larger than this")
    args = parser.parse_args()
    global MAX_GRAPH
    if args.max_graph:
        MAX_GRAPH = args.max_graph

    chosen = {k: v for k, v in BENCHMARKS.items() if not args.only or args.only in k}
    results: dict[str, list[dict]] = {}

    async def run_all():
        for name, bench in chosen.items():
            print(f"{name} ...", file=sys.stderr, flush=True)
            results[name] = await bench(args.quick)

    asyncio.run(run_all())
    machine = machine_info()
    args.out.mkdir(parents=True, exist_ok=True)
    (args.out / "results.json").write_text(
        json.dumps({"machine": machine, "quick": args.quick, "results": results}, indent=2))
    report = as_markdown(results, machine)
    (args.out / "results.md").write_text(report)
    print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
