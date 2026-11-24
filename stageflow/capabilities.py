from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version as _package_version
from typing import Any

from .core.nodes import get_node_types
from .core.policy import Policy
from .core.stage import get_stages
from .i18n import available_locales

DISTRIBUTION = "stageflow-framework"


def _installed_version() -> str:
    """The installed package version, or "0.0.0+unknown" when running from a
    source checkout that was never installed."""
    try:
        return _package_version(DISTRIBUTION)
    except PackageNotFoundError:  # pragma: no cover - only in a bare checkout
        return "0.0.0+unknown"


__version__ = _installed_version()


def capabilities(policy: Policy | None = None) -> dict[str, Any]:
    """What this build supports, or what a caller may use under `policy`.

    - `stageflow`: the core version (informational);
    - `node_types`: node types a pipeline may use, sorted. Clients should
      check this rather than the version: other types are rejected;
    - `stages`: how many stages may be used (the specs are served separately);
    - `locales`: languages the core's own messages are available in, the
      source language first;
    - `cycles`: whether cycles are allowed (`Policy.allow_cycles`).

    With a `policy`, the answer is narrowed to what it allows, which is what a
    backend should report to its client.
    """
    node_types = sorted(get_node_types())
    stages = sorted(get_stages())
    if policy is not None:
        node_types = [t for t in node_types if policy.allows_node_type(t)]
        stages = [name for name in stages if policy.allows_stage(name)]
    return {
        "stageflow": __version__,
        "node_types": node_types,
        "stages": len(stages),
        "locales": available_locales(),
        # Clients need this before drawing a back edge: otherwise it is only
        # refused at validation.
        "cycles": bool(policy is not None and policy.allow_cycles),
    }
