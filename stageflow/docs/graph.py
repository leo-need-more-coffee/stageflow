"""
Простой визуализатор пайплайна: JSON -> Mermaid -> самодостаточный HTML.

Читает текущий формат узлов: бакеты arguments/outputs, consume, expose,
retry, try/except, switch, parallel.branches, subpipeline.

    python -m stageflow.docs.graph pipeline.json -o graph.html
    python -m stageflow.docs.graph pipeline.json --mermaid   # только текст диаграммы
    python -m stageflow.docs.graph                           # демо-пайплайн

Что рисуется:
  - форма узла = его тип (entry / stage / condition / switch / parallel / try /
    subpipeline / terminal);
  - сплошная стрелка  — обычный переход `next` и вход в тело `try` (`body`);
  - подписанная       — ветка условия (`then`/`else`, `when`, `default`);
  - пунктир           — вход в ветку `parallel` и прыжок в обработчик `except`;
  - отдельный subgraph на каждый вложенный subpipeline.
Ниже графа — таблица потока данных: что узел читает, пишет, удаляет (`consume`)
и переименовывает (`expose`).
"""
from __future__ import annotations

import argparse
import html
import json
import sys
from typing import Any

# ------------------------------------------------------------------ mermaid

_SHAPES = {
    "entry": ('(("', '"))'),
    "stage": ('["', '"]'),
    "condition": ('{"', '"}'),
    "switch": ('{{"', '"}}'),
    "parallel": ('[/"', '"/]'),
    "try": ('>"', '"]'),
    "subpipeline": ('[["', '"]]'),
    "terminal": ('(["', '"])'),
}


def _esc(text: str) -> str:
    """Метка внутри mermaid-кавычек. HTML-сущности тут не годятся: страница
    отдаёт mermaid уже декодированный textContent, и `&quot;` доехал бы до
    диаграммы буквально — поэтому кавычку просто меняем на одинарную."""
    return str(text).replace('"', "'").replace("\n", " ")


def _node_label(node: dict) -> str:
    node_type = node.get("type", "?")
    lines = [f"{node['id']}"]
    if node_type == "entry":
        names = [k[:-2] if k.endswith(".$") else k for k in node.get("variables", {})]
        lines.append(", ".join(names) if names else "entry")
    elif node_type == "stage":
        lines.append(node.get("stage", "?"))
    elif node_type == "condition":
        lines.append(node.get("condition", ""))
    elif node_type == "switch":
        lines.append(f"{len(node.get('cases', []))} cases")
    elif node_type == "parallel":
        lines.append(", ".join(b.get("id", "?") for b in node.get("branches", [])))
    elif node_type == "try":
        lines.append(f"try · {len(node.get('except', []))} except")
    elif node_type == "subpipeline":
        lines.append(f"→ {node.get('subpipeline_id', '?')}")
    elif node_type == "terminal":
        lines.append("terminal")

    marks = []
    if node.get("consume"):
        marks.append("✂ consume")
    if node.get("expose"):
        marks.append("⇄ expose")
    if node.get("retry"):
        marks.append("↻ retry")
    if marks:
        lines.append(" ".join(marks))

    return "<br/>".join(_esc(x) for x in lines if x)


def _declare(node: dict, prefix: str = "") -> str:
    open_br, close_br = _SHAPES.get(node.get("type"), ('["', '"]'))
    return f'  {prefix}{node["id"]}{open_br}{_node_label(node)}{close_br}'


def _edges(node: dict, prefix: str = "") -> list[str]:
    """prefix — префикс идентификаторов внутри subgraph субпайплайна,
    чтобы одинаковые id в разных пайплайнах не слипались."""
    nid = f'{prefix}{node["id"]}'
    node_type = node.get("type")
    out: list[str] = []

    if node_type == "condition":
        if node.get("then"):
            out.append(f'  {nid} -->|then| {prefix}{node["then"]}')
        if node.get("else"):
            out.append(f'  {nid} -->|else| {prefix}{node["else"]}')
    elif node_type == "switch":
        for case in node.get("cases", []):
            out.append(f'  {nid} -->|"{_esc(case.get("when", ""))}"| {prefix}{case["next"]}')
        if node.get("default"):
            out.append(f'  {nid} -->|default| {prefix}{node["default"]}')
    elif node_type == "parallel":
        for branch in node.get("branches", []):
            out.append(
                f'  {nid} -.->|"branch {_esc(branch.get("id", ""))}"| {prefix}{branch["entry"]}'
            )
    elif node_type == "try":
        # тело — обычный переход: с него исполнение и продолжается, а `next`
        # ниже рисуется как выход из блока (туда же сходится и обработчик)
        if node.get("body"):
            out.append(f'  {nid} -->|body| {prefix}{node["body"]}')
        for handler in node.get("except", []):
            errs = ", ".join(handler.get("error_equals", [])) or "*"
            out.append(f'  {nid} -.->|"except {_esc(errs)}"| {prefix}{handler["next"]}')

    if node.get("next"):
        out.append(f'  {nid} --> {prefix}{node["next"]}')

    return out


def _entry_of(graph: dict) -> str | None:
    """Точка входа: поле ``entry`` или единственный узел типа ``entry``
    (с 0.7.0 поле необязательно — начало видно по графу)."""
    if graph.get("entry"):
        return graph["entry"]
    found = [n["id"] for n in graph.get("nodes", []) if n.get("type") == "entry"]
    return found[0] if len(found) == 1 else None


def to_mermaid(pipeline: dict) -> str:
    lines = ["flowchart TD"]

    for node in pipeline.get("nodes", []):
        lines.append(_declare(node))
    for node in pipeline.get("nodes", []):
        lines.extend(_edges(node))

    start = _entry_of(pipeline)
    if start:
        lines.append(f"  START(( )) --> {start}")

    for sub_id, sub in pipeline.get("subpipelines", {}).items():
        prefix = f"{sub_id}__"
        lines.append(f'  subgraph sub_{sub_id}["subpipeline: {_esc(sub_id)}"]')
        for node in sub.get("nodes", []):
            lines.append("  " + _declare(node, prefix))
        for node in sub.get("nodes", []):
            lines.extend("  " + e for e in _edges(node, prefix))
        lines.append("  end")

    subs = pipeline.get("subpipelines", {})
    for node in pipeline.get("nodes", []):
        if node.get("type") == "subpipeline" and node.get("subpipeline_id") in subs:
            sub_id = node["subpipeline_id"]
            entry = _entry_of(pipeline["subpipelines"][sub_id])
            if entry:
                lines.append(f'  {node["id"]} -.->|inputs| {sub_id}__{entry}')

    return "\n".join(lines)


# --------------------------------------------------------------- data table

def _bucket_names(bucket: Any) -> list[str]:
    if isinstance(bucket, list):
        return list(bucket)
    if isinstance(bucket, dict):
        return list(bucket.values())
    return []


def _reads(node: dict) -> list[str]:
    args = node.get("arguments", {})
    reads = list(_bucket_names(args.get("vars")))
    if node.get("type") == "subpipeline":
        reads += list(node.get("inputs", {}).values())
    return reads


def _writes(node: dict) -> list[str]:
    if node.get("type") == "entry":
        return [
            key[:-2] if key.endswith(".$") else key
            for key in node.get("variables", {})
        ]
    bucket = node.get("outputs", {})
    writes = []
    if isinstance(bucket, dict):
        for key, spec in bucket.items():
            writes.append(key[:-2] if key.endswith(".$") else spec)
    if node.get("type") == "subpipeline":
        writes += list(node.get("artifact_outputs", {}))
        if node.get("result_output"):
            writes.append(node["result_output"])
    if node.get("type") == "try":
        writes += [h["result_var"] for h in node.get("except", []) if h.get("result_var")]
    return writes


def _rows(pipeline: dict) -> list[dict]:
    rows = []
    for node in pipeline.get("nodes", []):
        rows.append({
            "id": node["id"],
            "type": node.get("type", "?"),
            "impl": node.get("stage") or node.get("subpipeline_id") or "",
            "reads": ", ".join(_reads(node)) or "—",
            "writes": ", ".join(_writes(node)) or "—",
            "consume": ", ".join(node.get("consume", [])) or "—",
            "expose": ", ".join(f"{k} → {v}" for k, v in node.get("expose", {}).items()) or "—",
        })
    return rows


# --------------------------------------------------------------------- html

_HTML = """<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
  :root {{
    --bg: #fbfaf9; --surface: #fff; --ink: #1a1a19; --ink-soft: #6b6b68;
    --line: #e4e2dd; --accent: #c45e34;
  }}
  @media (prefers-color-scheme: dark) {{
    :root:not([data-theme="light"]) {{
      --bg: #14140f; --surface: #1c1c18; --ink: #f0efea; --ink-soft: #9a9a94;
      --line: #32322c; --accent: #e08a5f;
    }}
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; padding: 32px 16px; background: var(--bg); color: var(--ink);
    font: 15px/1.6 ui-sans-serif, system-ui, -apple-system, "Segoe UI", sans-serif;
  }}
  main {{ max-width: 1100px; margin: 0 auto; }}
  h1 {{ font-size: 22px; margin: 0 0 4px; }}
  .sub {{ color: var(--ink-soft); font-size: 13px; margin-bottom: 28px; }}
  .card {{
    background: var(--surface); border: 1px solid var(--line);
    border-radius: 10px; padding: 20px; margin-bottom: 24px; overflow-x: auto;
  }}
  h2 {{ font-size: 14px; text-transform: uppercase; letter-spacing: .06em;
       color: var(--ink-soft); margin: 0 0 16px; font-weight: 600; }}
  table {{ border-collapse: collapse; width: 100%; font-size: 13px; }}
  th, td {{ text-align: left; padding: 8px 10px; border-bottom: 1px solid var(--line);
            vertical-align: top; }}
  th {{ color: var(--ink-soft); font-weight: 600; white-space: nowrap; }}
  td code {{ font: 12px/1.5 ui-monospace, SFMono-Regular, Menlo, monospace; }}
  .type {{ color: var(--accent); font-weight: 600; }}
  .legend {{ font-size: 12px; color: var(--ink-soft); margin-top: 12px; }}
  .legend span {{ margin-right: 14px; white-space: nowrap; }}
</style>
</head>
<body>
<main>
  <h1>{title}</h1>
  <div class="sub">entry: <code>{entry}</code> · узлов: {node_count}{sub_note}</div>

  <div class="card">
    <h2>Граф</h2>
    <pre class="mermaid">{mermaid}</pre>
    <div class="legend">
      <span>→ next</span>
      <span>→|подпись| ветка условия</span>
      <span>⇢ пунктир: вход в parallel-ветку / except / inputs субпайплайна</span>
      <span>✂ consume</span><span>⇄ expose</span><span>↻ retry</span>
    </div>
  </div>

  <div class="card">
    <h2>Поток данных</h2>
    <table>
      <thead><tr>
        <th>узел</th><th>тип</th><th>читает</th><th>пишет</th><th>consume</th><th>expose</th>
      </tr></thead>
      <tbody>{rows}</tbody>
    </table>
  </div>
</main>
<script type="module">
  import mermaid from "https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs";
  const dark = matchMedia("(prefers-color-scheme: dark)").matches;
  mermaid.initialize({{ startOnLoad: true, theme: dark ? "dark" : "default",
                       flowchart: {{ curve: "basis", nodeSpacing: 40, rankSpacing: 55 }} }});
</script>
</body>
</html>
"""


def to_html(pipeline: dict, title: str = "StageFlow pipeline") -> str:
    rows = "".join(
        "<tr>"
        f'<td><code>{html.escape(r["id"])}</code></td>'
        f'<td class="type">{html.escape(r["type"])}'
        + (f'<br><code>{html.escape(r["impl"])}</code>' if r["impl"] else "")
        + "</td>"
        f'<td><code>{html.escape(r["reads"])}</code></td>'
        f'<td><code>{html.escape(r["writes"])}</code></td>'
        f'<td><code>{html.escape(r["consume"])}</code></td>'
        f'<td><code>{html.escape(r["expose"])}</code></td>'
        "</tr>"
        for r in _rows(pipeline)
    )
    subs = pipeline.get("subpipelines", {})
    return _HTML.format(
        title=html.escape(title),
        entry=html.escape(pipeline.get("entry", "?")),
        node_count=len(pipeline.get("nodes", [])),
        sub_note=f" · субпайплайнов: {len(subs)}" if subs else "",
        mermaid=html.escape(to_mermaid(pipeline)),
        rows=rows,
    )


# --------------------------------------------------------------------- demo

DEMO_PIPELINE = {
    "entry": "auth",
    "subpipelines": {
        "publish_video": {
            "entry": "child_hash",
            "nodes": [
                {"id": "child_hash", "type": "stage", "stage": "HashStage",
                 "arguments": {"vars": {"data": "payload"}},
                 "outputs": {"digest": "child_digest"},
                 "next": "child_done"},
                {"id": "child_done", "type": "terminal", "result": {"status": "published"}},
            ],
        },
    },
    "nodes": [
        {"id": "auth", "type": "stage", "stage": "AuthStage",
         "arguments": {"const": {"creds": "***"}},
         "outputs": {"token": "token"},
         "next": "guard"},
        # обработка ошибок — блочная: `try` накрывает тело (`fetch_data`), а
        # выход из блока и обработчики сходятся дальше по графу
        {"id": "guard", "type": "try", "body": "fetch_data", "next": "fan_out",
         "except": [
             {"error_equals": ["TimeoutError", "ConnectionError"],
              "next": "handle_failure", "result_var": "error"},
             {"error_equals": ["*"], "next": "report_bug", "result_var": "error"},
         ]},
        # у последнего узла тела нет `next`: дальше исполнение продолжит `try`
        {"id": "fetch_data", "type": "stage", "stage": "FetchDataStage",
         "arguments": {"vars": ["token"], "const": {"n_max": 10}},
         "outputs": {"data": "data", "count": "count"},
         "consume": ["token"],
         "retry": [{"error_equals": ["TimeoutError"], "max_attempts": 3}]},
        {"id": "fan_out", "type": "parallel",
         "branches": [{"id": "hash", "entry": "hash_step"}, {"id": "thumb", "entry": "thumb_step"}],
         "next": "route"},
        {"id": "hash_step", "type": "stage", "stage": "HashStage",
         "arguments": {"vars": ["data"]},
         "outputs": {"digest": "file_hash"}},
        {"id": "thumb_step", "type": "stage", "stage": "ThumbStage",
         "arguments": {"vars": ["data"]},
         "outputs": {"path": "thumb_path"}},
        {"id": "route", "type": "switch",
         "cases": [{"when": "vars.count > 0", "next": "publish"}],
         "default": "handle_failure"},
        {"id": "publish", "type": "subpipeline", "subpipeline_id": "publish_video",
         "inputs": {"payload": "data"},
         "artifact_outputs": {"digest": "child_digest"},
         "result_output": "publish_result",
         "expose": {"file_hash": "hash_for_report"},
         "next": "done"},
        {"id": "handle_failure", "type": "terminal", "result": {"status": "failed"}},
        {"id": "report_bug", "type": "terminal", "result": {"status": "crashed"}},
        {"id": "done", "type": "terminal", "artifacts": ["file_hash", "thumb_path"],
         "result": {"status": "ok"}},
    ],
}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Визуализатор пайплайна StageFlow")
    parser.add_argument("pipeline", nargs="?", help="путь к JSON пайплайна (без него — демо)")
    parser.add_argument("-o", "--out", default="pipeline_graph.html", help="куда писать HTML")
    parser.add_argument("--mermaid", action="store_true", help="напечатать только mermaid")
    args = parser.parse_args(argv)

    if args.pipeline:
        with open(args.pipeline, encoding="utf-8") as fh:
            pipeline = json.load(fh)
        title = args.pipeline
    else:
        pipeline, title = DEMO_PIPELINE, "StageFlow demo pipeline"

    if args.mermaid:
        print(to_mermaid(pipeline))
        return 0

    with open(args.out, "w", encoding="utf-8") as fh:
        fh.write(to_html(pipeline, title))
    print(f"написал {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
