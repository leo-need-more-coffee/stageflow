"""
Generate StageFlow HTML docs and related JSON assets in code (no file writes).

Usage:
    from stageflow.docs.html import generate_docs_assets
    html_page, schema, stages_json = generate_docs_assets()
"""

from __future__ import annotations

import html
import json
from typing import Any, Iterable

import stageflow.builtins  # noqa: F401 - ensure builtins are registered
from stageflow.core.stage import get_stages, get_stages_by_category
from stageflow.docs.schema import generate_pipeline_schema, generate_stages_json

TYPE_HINTS = {"string", "str", "number", "int", "float", "bool", "any", "object", "list"}


def _render_json(data: Any) -> str:
    def fmt(val: Any, indent: int = 0) -> str:
        pad = "  " * indent
        if isinstance(val, dict):
            if not val:
                return "{}"
            inner = []
            for k, v in val.items():
                inner.append(f'{pad}  "{k}": {fmt(v, indent + 1)}')
            return "{\n" + "\n".join(inner) + f"\n{pad}" + "}"
        if isinstance(val, list):
            if not val:
                return "[]"
            inner = [fmt(v, indent + 1) for v in val]
            return "[ " + ", ".join(inner) + " ]"
        if isinstance(val, str):
            if val.lower() in TYPE_HINTS:
                return val
            return f"\"{val}\""
        if isinstance(val, bool):
            return "true" if val else "false"
        return str(val)

    return html.escape(fmt(data))


def _fields_to_placeholder_map(fields: list[dict[str, Any]] | dict[str, Any] | None, builder) -> dict[str, Any]:
    mapping: dict[str, Any] = {}
    if not fields:
        return mapping
    iterable = fields.items() if isinstance(fields, dict) else fields
    for item in iterable:
        if isinstance(item, tuple):
            name, spec = item
            field = {"name": name}
            if isinstance(spec, dict):
                field.update(spec)
            else:
                field["type"] = spec
        else:
            field = item or {}
        name = field.get("name")
        if not name:
            continue
        mapping[name] = builder(field)
    return mapping


def _render_stage_field_table(fields: list[dict[str, Any]], empty_label: str) -> str:
    if not fields:
        return f"<div class='muted'>{html.escape(empty_label)}</div>"
    rows = []
    for field in fields:
        name = field.get("name", "")
        typ = field.get("type", "any")
        description = field.get("description", "")
        default = field.get("default", "")
        optional = field.get("optional", False)
        parts = [f"<code>{html.escape(str(typ))}</code>"]
        if default not in (None, ""):
            parts.append(f"default: <code>{html.escape(str(default))}</code>")
        if optional:
            parts.append("<span class='badge opt'>optional</span>")
        desc_part = html.escape(str(description)) if description else ""
        rows.append(
            f"<tr><td><code>{html.escape(str(name))}</code></td>"
            f"<td>{' '.join(parts)}</td>"
            f"<td>{desc_part}</td></tr>"
        )
    return (
        "<table class='fields'>"
        "<thead><tr><th>Field</th><th>Info</th><th></th></tr></thead>"
        f"<tbody>{''.join(rows)}</tbody>"
        "</table>"
    )


def _render_field_list(props: dict[str, Any], required: Iterable[str]) -> str:
    req_set = set(required or [])
    rows = []
    for name, spec in props.items():
        typ = spec.get("type", "any")
        description = spec.get("description", "")
        default = spec.get("default", "")
        enum = spec.get("enum", [])
        parts = [f"<code>{html.escape(str(typ))}</code>"]
        if default not in (None, ""):
            parts.append(f"default: <code>{html.escape(str(default))}</code>")
        if enum:
            parts.append("enum: " + ", ".join(html.escape(str(e)) for e in enum))
        if description:
            parts.append(html.escape(description))
        req_badge = "<span class='badge req'>required</span>" if name in req_set else ""
        rows.append(
            f"<tr><td><code>{html.escape(name)}</code></td>"
            f"<td>{' '.join(parts)}</td>"
            f"<td>{req_badge}</td></tr>"
        )
    return "\n".join(rows)


def build_nodes_section(schema: dict[str, Any]) -> str:
    defs = schema.get("$defs", {})
    blocks = []
    for key in ("stage_node", "condition_node", "parallel_node", "map_node", "subpipeline_node", "terminal_node"):
        node = defs.get(key)
        if not node:
            continue
        title = key.replace("_", " ").title()
        props = node.get("properties", {})
        required = node.get("required", [])
        rows = _render_field_list(props, required)
        example = {**{k: v.get("const") for k, v in props.items() if "const" in (v or {})}}
        blocks.append(
            f"""
            <details class="node-block">
              <summary>{html.escape(title)}</summary>
              <div class="card">
                <table class="fields">
                  <thead><tr><th>Field</th><th>Info</th><th></th></tr></thead>
                  <tbody>{rows}</tbody>
                </table>
                <div class="example">
                  <div class="label">Minimal fragment</div>
                  <pre>{_render_json(example)}</pre>
                </div>
              </div>
            </details>
            """
        )
    return "\n".join(blocks)


def build_stages_section() -> str:
    by_cat = get_stages_by_category()
    sections = []
    for category in sorted(by_cat.keys()):
        cards = []
        for stage_cls in sorted(by_cat[category], key=lambda c: c.stage_name):
            specs = stage_cls.get_specs()
            arguments = specs.get("arguments") or []
            config = specs.get("config") or []
            outputs = specs.get("outputs") or []
            allowed_events = specs.get("allowed_events") or []
            allowed_inputs = specs.get("allowed_inputs") or []
            stage_name = specs.get("stage_name", stage_cls.__name__)
            args_placeholder = _fields_to_placeholder_map(arguments, lambda f: _placeholder_from_hint(f.get("type")))
            config_placeholder = _fields_to_placeholder_map(
                config, lambda f: f.get("default") if "default" in f else _placeholder_from_hint(f.get("type"))
            )
            outputs_placeholder = _fields_to_placeholder_map(outputs, lambda f: f.get("name"))
            search_blob = " ".join(
                str(x)
                for x in [
                    stage_name,
                    specs.get("description", ""),
                    category,
                    " ".join(f.get("name", "") for f in arguments),
                    " ".join(f.get("name", "") for f in config),
                    " ".join(f.get("name", "") for f in outputs),
                ]
            ).lower()
            example_node = {
                "id": stage_name.lower(),
                "type": "stage",
                "stage": stage_name,
                **({"config": config_placeholder} if config_placeholder else {}),
                **({"arguments": args_placeholder} if args_placeholder else {}),
                **({"outputs": outputs_placeholder} if outputs_placeholder else {}),
                "next": "next_node",
            }
            cards.append(
                f"""
                <div class="stage-card" data-search="{html.escape(search_blob)}">
                  <div class="stage-title">{html.escape(stage_name)}</div>
                  <div class="muted">{html.escape(specs.get("description", '') or 'No description')}</div>
                  <div class="chip-row">
                    <span class="chip">category: {html.escape(category)}</span>
                    <span class="chip">{'skipable' if specs.get('skipable') else 'not skipable'}</span>
                  </div>
                  <div class="block">
                    <div class="label">arguments</div>
                    {_render_stage_field_table(arguments, "No arguments")}
                  </div>
                  <div class="block">
                    <div class="label">config</div>
                    {_render_stage_field_table(config, "No config")}
                  </div>
                  <div class="block">
                    <div class="label">outputs</div>
                    {_render_stage_field_table(outputs, "No outputs")}
                  </div>
                  <div class="block two-cols">
                    <div>
                      <div class="label">allowed events</div>
                      <pre>{_render_json(allowed_events)}</pre>
                    </div>
                    <div>
                      <div class="label">allowed inputs</div>
                      <pre>{_render_json(allowed_inputs)}</pre>
                    </div>
                  </div>
                  <div class="block">
                    <div class="label">Example usage</div>
                    <pre>{_render_json(example_node)}</pre>
                  </div>
                </div>
                """
            )
        sections.append(
            f"""
            <details class="category" open data-category="{html.escape(category.lower())}">
              <summary>Category: {html.escape(category)}</summary>
              <div class="stage-grid">
                {''.join(cards)}
              </div>
            </details>
            """
        )
    return "\n".join(sections)


def build_example_block(schema: dict[str, Any]) -> str:
    example = {
        "api_version": schema.get("properties", {}).get("api_version", {}).get("default"),
        "entry": "start",
        "nodes": [
            {
                "id": "start",
                "type": "stage",
                "stage": "InitStage",
                "outputs": {"value": "value"},
                "next": "finish",
            },
            {
                "id": "finish",
                "type": "terminal",
                "result": {"status": "ok"},
                "artifacts": ["value"],
            },
        ],
    }
    return f"""
    <section>
      <h2>Minimal pipeline example</h2>
      <p>Copy and plug in your own stages and context paths.</p>
      <pre>{_render_json(example)}</pre>
    </section>
    """


def _placeholder_from_hint(val: Any) -> Any:
    if isinstance(val, dict):
        return {k: _placeholder_from_hint(v) for k, v in val.items()}
    if isinstance(val, list):
        if val:
            return [_placeholder_from_hint(val[0])]
        return []
    if isinstance(val, str):
        low = val.lower()
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
    return val or "<value>"


def build_html(schema: dict[str, Any], stages_json: str) -> str:
    schema_json = json.dumps(schema, indent=2, ensure_ascii=False)
    stages_obj_literal = stages_json
    try:
        stages_obj_literal = json.dumps(json.loads(stages_json), ensure_ascii=False, indent=2)
    except Exception:
        # If parsing fails, fall back to raw string (still escaped in HTML/pre).
        stages_obj_literal = json.dumps(stages_json, ensure_ascii=False)
    return f"""<!DOCTYPE html>
<html lang="ru">
<head>
  <meta charset="UTF-8">
  <title>StageFlow Docs</title>
  <style>
    :root {{
      --bg: #0f1115;
      --card: #181b21;
      --text: #e8ecf2;
      --muted: #9aa3b5;
      --accent: #4ea1ff;
      --border: #262b33;
      --chip: #1f252d;
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin:0; font-family: "Segoe UI", sans-serif; background: var(--bg); color: var(--text); padding: 40px 56px; min-height: 100vh; }}
    .content {{ max-width: 1280px; margin: 0 auto; }}
    h1, h2, h3 {{ margin: 0 0 12px; }}
    p {{ margin: 0 0 12px; color: var(--muted); }}
    section {{ margin-bottom: 32px; }}
    details {{ margin-bottom: 12px; }}
    summary {{ cursor: pointer; color: var(--accent); font-weight: 600; }}
    .card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 16px; }}
    .fields {{ width: 100%; border-collapse: collapse; margin-top: 8px; table-layout: fixed; }}
    .fields th, .fields td {{ border-bottom: 1px solid var(--border); padding: 8px; text-align: left; vertical-align: top; }}
    .fields th {{ color: var(--muted); font-weight: 600; }}
    .fields td:first-child {{ width: 32%; }}
    pre {{ background: #0a0c10; border: 1px solid var(--border); border-radius: 8px; padding: 12px; overflow-x: auto; white-space: pre-wrap; word-break: break-word; }}
    .label {{ color: var(--muted); font-size: 12px; margin-bottom: 4px; text-transform: uppercase; letter-spacing: 0.04em; }}
    .example {{ margin-top: 12px; }}
    .stage-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(420px, 1fr)); gap: 24px; }}
    .stage-card {{ background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 18px; display: flex; flex-direction: column; gap: 12px; }}
    .stage-title {{ font-weight: 700; font-size: 17px; }}
    .muted {{ color: var(--muted); line-height: 1.5; }}
    .chip-row {{ display: flex; gap: 8px; flex-wrap: wrap; }}
    .chip {{ background: var(--chip); padding: 4px 8px; border-radius: 999px; color: var(--muted); font-size: 12px; border: 1px solid var(--border); }}
    .badge.req {{ background: #2f3; color: #0a0; padding: 2px 6px; border-radius: 6px; font-size: 12px; }}
    .badge.opt {{ background: #f8c146; color: #4b3200; padding: 2px 6px; border-radius: 6px; font-size: 12px; }}
    .block {{ display: flex; flex-direction: column; gap: 4px; }}
    .two-cols {{ display: grid; gap: 8px; grid-template-columns: repeat(auto-fit, minmax(160px, 1fr)); }}
    .topbar {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 24px; gap: 12px; flex-wrap: wrap; }}
    .topbar-actions {{ display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }}
    .search {{ background: var(--card); border: 1px solid var(--border); color: var(--text); padding: 10px 12px; border-radius: 10px; min-width: 260px; }}
    .search:focus {{ outline: 1px solid var(--accent); }}
    .cta {{ background: var(--accent); color: #0b0f16; padding: 10px 14px; border-radius: 10px; text-decoration: none; font-weight: 700; border: 1px solid transparent; cursor: pointer; }}
    .cta.secondary {{ background: transparent; color: var(--text); border-color: var(--border); }}
    .category summary {{ font-size: 16px; margin-bottom: 8px; }}
    .category[hidden], .node-block[hidden] {{ display: none; }}
    .node-grid details {{ margin-bottom: 8px; }}
  </style>
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
      <p>All registered stages with arguments, config, outputs, and allowed events.</p>
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
    const pipelineSchema = {schema_json};
    const stagesSpec = {stages_obj_literal};
    const searchInput = document.getElementById('search');
    const categories = Array.from(document.querySelectorAll('details.category'));
    const nodeBlocks = Array.from(document.querySelectorAll('details.node-block'));
    const toggleAllBtn = document.getElementById('toggle-all');
    const downloadStagesBtn = document.getElementById('download-stages');
    const downloadSchemaBtn = document.getElementById('download-schema');

    function applyFilter() {{
      const term = (searchInput.value || '').toLowerCase().trim();
      categories.forEach((cat) => {{
        let hasVisible = false;
        const cards = Array.from(cat.querySelectorAll('.stage-card'));
        cards.forEach((card) => {{
          const haystack = (card.dataset.search || '').toLowerCase();
          const match = !term || haystack.includes(term);
          card.style.display = match ? '' : 'none';
          if (match) hasVisible = true;
        }});
        cat.hidden = !hasVisible;
        if (hasVisible && term) {{
          cat.open = true;
        }}
      }});
      nodeBlocks.forEach((block) => {{
        const haystack = (block.querySelector('summary')?.textContent || '').toLowerCase();
        const match = !term || haystack.includes(term);
        block.hidden = !match;
      }});
    }}

    if (searchInput) {{
      searchInput.addEventListener('input', applyFilter);
      applyFilter();
    }}

    function setAll(open) {{
      [...categories, ...nodeBlocks].forEach((elem) => {{
        if (!elem.hidden) {{
          elem.open = open;
        }}
      }});
      toggleAllBtn.textContent = open ? 'Collapse all' : 'Expand all';
    }}

    if (toggleAllBtn) {{
      let allOpen = true;
      toggleAllBtn.addEventListener('click', () => {{
        allOpen = !allOpen;
        setAll(allOpen);
      }});
    }}

    function downloadJson(data, filename) {{
      const blob = new Blob([JSON.stringify(data, null, 2)], {{ type: 'application/json' }});
      const url = URL.createObjectURL(blob);
      const link = document.createElement('a');
      link.href = url;
      link.download = filename;
      link.click();
      URL.revokeObjectURL(url);
    }}

    if (downloadStagesBtn) {{
      downloadStagesBtn.addEventListener('click', () => downloadJson(stagesSpec, 'stages.json'));
    }}
    if (downloadSchemaBtn) {{
      downloadSchemaBtn.addEventListener('click', () => downloadJson(pipelineSchema, 'pipeline.json'));
    }}
  </script>
</body>
</html>
"""


def generate_docs_assets() -> tuple[str, dict, str]:
    """
    Return (html_page, pipeline_schema, stages_json) using registered stages, without writing any files.
    """
    stages = get_stages()
    schema = generate_pipeline_schema(stages)
    stages_json = generate_stages_json(stages)
    html_page = build_html(schema, stages_json)
    return html_page, schema, stages_json
