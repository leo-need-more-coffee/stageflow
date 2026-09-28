"""What this build of StageFlow can do, in a form a client can read.

A visual editor is written against one version of the core and then pointed at
whatever backend the user has running. A version number alone does not answer
the question it actually has — "can I offer a `map` node here?" — because the
answer depends on the build, not on a range of releases someone has to keep a
table of. So the core answers it directly: here are the node types I execute.

A backend is expected to hand this to its clients (the example backend serves
it at `GET /api/meta`); the shape is small and additive on purpose.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version as _package_version
from typing import Any

from .core.nodes import get_node_types
from .core.stage import get_stages

DISTRIBUTION = "stageflow-framework"


def _installed_version() -> str:
    """The version of the installed distribution.

    Running from a source checkout that was never installed there is nothing
    to read it from, and guessing would be worse than saying so: a client that
    sees "0.0.0+unknown" knows not to compare it with anything, while a made-up
    number would be compared and believed.
    """
    try:
        return _package_version(DISTRIBUTION)
    except PackageNotFoundError:  # pragma: no cover - only in a bare checkout
        return "0.0.0+unknown"


__version__ = _installed_version()


def capabilities() -> dict[str, Any]:
    """The language this build understands.

    - `stageflow` — the version of the core, for a human reading a log;
    - `node_types` — the types a pipeline may use here, sorted. This is the
      field a client should branch on: a name that is not in the list will be
      rejected by `Pipeline.from_dict` with `Unknown node type`.
    - `stages` — how many stages are registered. The specs themselves are a
      separate, much larger answer (`get_stages`), so this is only a hint that
      a registry is there at all.
    """
    return {
        "stageflow": __version__,
        "node_types": sorted(get_node_types()),
        "stages": len(get_stages()),
    }
