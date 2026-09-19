from __future__ import annotations

import html
import json
from typing import Any, Callable

from ..core.stage import get_stages, get_stages_by_category
from .schema import generate_pipeline_schema, generate_stages_json

_TYPE_HINTS = {"string", "str", "number", "int", "float", "bool", "any", "object", "list"}

_NODE_DEF_KEYS = (
    "entry_node",
    "stage_node",
    "condition_node",
    "switch_node",
    "parallel_node",
    "subpipeline_node",
    "try_node",
    "terminal_node",
)


def _render_json(data: Any) -> str:
    def fmt(val: Any, indent: int = 0) -> str:
        pad = "  " * indent
        if isinstance(val, dict):
            if not val:
                return "{}"
            inner = [f'{pad}  "{key}": {fmt(item, indent + 1)}' for key, item in val.items()]
            return "{\n" + ",\n".join(inner) + f"\n{pad}" + "}"
        if isinstance(val, list):
            if not val:
                return "[]"
            return "[ " + ", ".join(fmt(item, indent + 1) for item in val) + " ]"
        if isinstance(val, bool):
            return "true" if val else "false"
        if val is None:
            return "null"
        if isinstance(val, str):
            return val if val.lower() in _TYPE_HINTS else f'"{val}"'
        return str(val)

    return html.escape(fmt(data))


def _field_table(rows: list[str]) -> str:
    return (
        "<table class='fields'>"
        "<thead><tr><th>Field</th><th>Info</th><th></th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def _field_row(name: str, info_parts: list[str], badge: str = "") -> str:
    return (
        f"<tr><td><code>{html.escape(str(name))}</code></td>"
        f"<td>{' '.join(info_parts)}</td>"
        f"<td>{badge}</td></tr>"
    )


def _stage_fields_table(fields: list[dict[str, Any]], empty_label: str) -> str:
    if not fields:
        return f"<div class='muted'>{html.escape(empty_label)}</div>"
    rows = []
    for field in fields:
        parts = [f"<code>{html.escape(str(field.get('type', 'any')))}</code>"]
        if field.get("default") not in (None, ""):
            parts.append(f"default: <code>{html.escape(str(field['default']))}</code>")
        description = field.get("description", "")
        if description:
            parts.append(html.escape(str(description)))
        badge = "<span class='badge opt'>optional</span>" if field.get("optional") else ""
        rows.append(_field_row(field.get("name", ""), parts, badge))
    return _field_table(rows)


def _schema_fields_table(props: dict[str, Any], required: list[str]) -> str:
    required_set = set(required or [])
    rows = []
    for name, spec in props.items():
        parts = [f"<code>{html.escape(str(spec.get('type', 'any')))}</code>"]
        if spec.get("default") not in (None, ""):
            parts.append(f"default: <code>{html.escape(str(spec['default']))}</code>")
        if spec.get("enum"):
            parts.append("enum: " + ", ".join(html.escape(str(e)) for e in spec["enum"]))
        if spec.get("description"):
            parts.append(html.escape(spec["description"]))
        badge = "<span class='badge req'>required</span>" if name in required_set else ""
        rows.append(_field_row(name, parts, badge))
    return _field_table(rows)


def _placeholder_from_hint(hint: Any) -> Any:
    if isinstance(hint, dict):
        return {key: _placeholder_from_hint(value) for key, value in hint.items()}
    if isinstance(hint, list):
        return [_placeholder_from_hint(hint[0])] if hint else []
    if isinstance(hint, str):
        low = hint.lower()
        if low in {"string", "str"}:
            return "<string>"
        if low in {"number", "int", "float"}:
            return 0
        if low in {"bool", "boolean"}:
            return True
        if low == "object":
            return {}
        if low == "list":
            return []
    return hint or "<value>"


def _placeholder_map(
    fields: list[dict[str, Any]], builder: Callable[[dict], Any]
) -> dict[str, Any]:
    return {
        field["name"]: builder(field)
        for field in fields or []
        if field.get("name")
    }


def build_nodes_section(schema: dict[str, Any]) -> str:
    defs = schema.get("$defs", {})
    blocks = []
    for key in _NODE_DEF_KEYS:
        node = defs.get(key)
        if not node:
            continue
        props = node.get("properties", {})
        example = {k: v["const"] for k, v in props.items() if "const" in (v or {})}
        blocks.append(
            f"""
            <details class="node-block">
              <summary>{html.escape(key.replace('_', ' ').title())}</summary>
              <div class="card">
                {_schema_fields_table(props, node.get("required", []))}
                <div class="example">
                  <div class="label">Minimal fragment</div>
                  <pre>{_render_json(example)}</pre>
                </div>
              </div>
            </details>
            """
        )
    return "\n".join(blocks)


def _stage_card(stage_cls: type, category: str) -> str:
    specs = stage_cls.get_specs()
    stage_name = specs.get("stage_name", stage_cls.__name__)
    arguments = specs.get("arguments") or []
    outputs = specs.get("outputs") or []

    named_args = [f for f in arguments if f.get("name") != "*"]
    example_node = {
        "id": stage_name.lower(),
        "type": "stage",
        "stage": stage_name,
        **(
            {"arguments": {"const": _placeholder_map(
                named_args, lambda f: f.get("default", _placeholder_from_hint(f.get("type")))
            )}}
            if named_args
            else {}
        ),
        **({"outputs": _placeholder_map(outputs, lambda f: f.get("name"))} if outputs else {}),
        "next": "next_node",
    }
    search_blob = " ".join(
        str(part)
        for part in (
            stage_name,
            specs.get("description", ""),
            category,
            *(f.get("name", "") for f in (*arguments, *outputs)),
        )
    ).lower()

    return f"""
    <div class="stage-card" data-search="{html.escape(search_blob)}">
      <div class="stage-title">{html.escape(stage_name)}</div>
      <div class="muted">{html.escape(specs.get("description", '') or 'No description')}</div>
      <div class="chip-row">
        <span class="chip">category: {html.escape(category)}</span>
        <span class="chip">{'skipable' if specs.get('skipable') else 'not skipable'}</span>
      </div>
      <div class="block">
        <div class="label">arguments</div>
        {_stage_fields_table(arguments, "No arguments")}
      </div>
      <div class="block">
        <div class="label">outputs</div>
        {_stage_fields_table(outputs, "No outputs")}
      </div>
      <div class="block two-cols">
        <div>
          <div class="label">allowed events</div>
          <pre>{_render_json(specs.get("allowed_events") or [])}</pre>
        </div>
        <div>
          <div class="label">allowed inputs</div>
          <pre>{_render_json(specs.get("allowed_inputs") or [])}</pre>
        </div>
      </div>
      <div class="block">
        <div class="label">Example usage</div>
        <pre>{_render_json(example_node)}</pre>
      </div>
    </div>
    """


def build_stages_section() -> str:
    by_category = get_stages_by_category()
    sections = []
    for category in sorted(by_category):
        cards = "".join(
            _stage_card(stage_cls, category)
            for stage_cls in sorted(by_category[category], key=lambda c: c.stage_name)
        )
        sections.append(
            f"""
            <details class="category" open data-category="{html.escape(category.lower())}">
              <summary>Category: {html.escape(category)}</summary>
              <div class="stage-grid">{cards}</div>
            </details>
            """
        )
    return "\n".join(sections)


def build_example_block(schema: dict[str, Any]) -> str:
    example = {
        "api_version": schema.get("properties", {}).get("api_version", {}).get("default"),
        "nodes": [
            {
                "id": "start",
                "type": "entry",
                "variables": {"name": "world"},
                "next": "greet",
            },
            {
                "id": "greet",
                "type": "stage",
                "stage": "TemplateStage",
                "arguments": {"vars": {"name": "name"},
                              "const": {"template": "Hello, {name}!"}},
                "outputs": {"value": "greeting"},
                "next": "finish",
            },
            {
                "id": "finish",
                "type": "terminal",
                "result": {"status": "ok"},
                "artifacts": ["greeting"],
            },
        ],
    }
    return f"""
    <section>
      <h2>Minimal pipeline example</h2>
      <p>Copy and plug in your own stages and scope names.</p>
      <pre>{_render_json(example)}</pre>
    </section>
    """


_CSS = """
    :root {
      --bg: #0f1115;
      --card: #181b21;
      --text: #e8ecf2;
      --muted: #9aa3b5;
      --accent: #4ea1ff;
      --border: #262b33;
      --chip: #1f252d;
    }
    * { box-sizing: border-box; }
    body { margin:0; font-family: "Segoe UI", sans-serif; background: var(--bg); color: var(--text); padding: 40px 56px; min-height: 100vh; }
    .content { max-width: 1280px; margin: 0 auto; }
    h1, h2, h3 { margin: 0 0 12px; }
    p { margin: 0 0 12px; color: var(--muted); }
    section { margin-bottom: 32px; }
    details { margin-bottom: 12px; }
    summary { cursor: pointer; color: var(--accent); font-weight: 600; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 16px; }
    .fields { width: 100%; border-collapse: collapse; margin-top: 8px; table-layout: fixed; }
    .fields th, .fields td { border-bottom: 1px solid var(--border); padding: 8px; text-align: left; vertical-align: top; }
    .fields th { color: var(--muted); font-weight: 600; }
    .fields td:first-child { width: 32%; }
    pre { background: #0a0c10; border: 1px solid var(--border); border-radius: 8px; padding: 12px; overflow-x: auto; white-space: pre-wrap; word-break: break-word; }
    .label { color: var(--muted); font-size: 12px; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.04em; }
    .example { margin-top: 12px; }
    .stage-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(420px, 1fr)); gap: 24px; }
    .stage-card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 18px; display: flex; flex-direction: column; gap: 12px; }
    .stage-title { font-weight: 700; font-size: 17px; }
    .muted { color: var(--muted); line-height: 1.5; }
    .chip-row { display: flex; gap: 8px; flex-wrap: wrap; }
    .chip { background: var(--chip); padding: 4px 8px; border-radius: 999px; color: var(--muted); font-size: 12px; border: 1px solid var(--border); }
    .badge.req { background: #2f3; color: #0a0; padding: 2px 6px; border-radius: 6px; font-size: 12px; }
    .badge.opt { background: #f8c146; color: #4b3200; padding: 2px 6px; border-radius: 6px; font-size: 12px; }
    .block { display: flex; flex-direction: column; gap: 4px; }
    .two-cols { display: grid; gap: 8px; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); }
    .topbar { display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; gap: 12px; flex-wrap: wrap; }
    .topbar-actions { display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }
    .search { background: var(--card); border: 1px solid var(--border); color: var(--text); padding: 10px 12px; border-radius: 10px; min-width: 260px; }
    .search:focus { outline: 1px solid var(--accent); }
    .cta { background: var(--accent); color: #0b0f16; padding: 10px 14px; border-radius: 10px; text-decoration: none; font-weight: 700; border: 1px solid transparent; cursor: pointer; }
    .cta.secondary { background: transparent; color: var(--text); border-color: var(--border); }
    .category summary { font-size: 16px; margin-bottom: 8px; }
    .category[hidden], .node-block[hidden] { display: none; }
    .node-grid details { margin-bottom: 8px; }
"""

_JS = """
    const searchInput = document.getElementById('search');
    const categories = Array.from(document.querySelectorAll('details.category'));
    const nodeBlocks = Array.from(document.querySelectorAll('details.node-block'));
    const toggleAllBtn = document.getElementById('toggle-all');

    function applyFilter() {
      const term = (searchInput.value || '').toLowerCase().trim();
      categories.forEach((cat) => {
        let hasVisible = false;
        cat.querySelectorAll('.stage-card').forEach((card) => {
          const match = !term || (card.dataset.search || '').includes(term);
          card.style.display = match ? '' : 'none';
          if (match) hasVisible = true;
        });
        cat.hidden = !hasVisible;
        if (hasVisible && term) cat.open = true;
      });
      nodeBlocks.forEach((block) => {
        const haystack = (block.querySelector('summary')?.textContent || '').toLowerCase();
        block.hidden = term && !haystack.includes(term);
      });
    }

    searchInput.addEventListener('input', applyFilter);
    applyFilter();

    let allOpen = true;
    toggleAllBtn.addEventListener('click', () => {
      allOpen = !allOpen;
      [...categories, ...nodeBlocks].forEach((elem) => {
        if (!elem.hidden) elem.open = allOpen;
      });
      toggleAllBtn.textContent = allOpen ? 'Collapse all' : 'Expand all';
    });

    function downloadJson(data, filename) {
      const blob = new Blob([JSON.stringify(data, null, 2)], { type: 'application/json' });
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = filename;
      link.click();
      URL.revokeObjectURL(url);
    }

    document.getElementById('download-stages')
      .addEventListener('click', () => downloadJson(stagesSpec, 'stages.json'));
    document.getElementById('download-schema')
      .addEventListener('click', () => downloadJson(pipelineSchema, 'pipeline.json'));
"""


def build_html(schema: dict[str, Any], stages_json: str) -> str:
    schema_literal = json.dumps(schema, indent=2, ensure_ascii=False)
    stages_literal = json.dumps(json.loads(stages_json), indent=2, ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <title>StageFlow Docs</title>
  <style>{_CSS}</style>
</head>
<body>
  <div class="content">
    <div class="topbar">
      <div>
        <h1>StageFlow</h1>
        <p>Cheat sheet for pipelines, node types, and available stages.</p>
      </div>
      <div class="topbar-actions">
        <input id="search" class="search" type="search" placeholder="Search stages and categories..." />
        <button id="toggle-all" class="cta secondary" type="button">Collapse all</button>
        <button id="download-stages" class="cta secondary" type="button">Download stages JSON</button>
        <button id="download-schema" class="cta secondary" type="button">Download pipeline schema</button>
      </div>
    </div>

    <section>
      <h2>Node types</h2>
      <p>Quick overview of fields and required properties for each node type.</p>
      <div class="node-grid">{build_nodes_section(schema)}</div>
    </section>

    <section>
      <h2>Stages (registry)</h2>
      <p>All registered stages with arguments, outputs, and allowed events.</p>
      {build_stages_section()}
    </section>

    {build_example_block(schema)}
    <section id="stages-json">
      <h2>Stages JSON</h2>
      <p>Raw stage specs used to build this page.</p>
      <pre>{html.escape(stages_json)}</pre>
    </section>
    <section id="pipeline-schema">
      <h2>Pipeline JSON Schema</h2>
      <p>Schema used for validation (stage enum injected).</p>
      <pre>{_render_json(schema)}</pre>
    </section>
  </div>
  <script>
    const pipelineSchema = {schema_literal};
    const stagesSpec = {stages_literal};
{_JS}
  </script>
</body>
</html>
"""


def generate_docs_assets() -> tuple[str, dict, str]:
    stages = get_stages()
    schema = generate_pipeline_schema(stages)
    stages_json = generate_stages_json(stages)
    return build_html(schema, stages_json), schema, stages_json
