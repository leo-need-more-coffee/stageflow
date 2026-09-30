from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version as _package_version
from typing import Any

from .core.nodes import get_node_types
from .core.policy import Policy
from .core.stage import get_stages
from .i18n import available_locales

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


def capabilities(policy: Policy | None = None) -> dict[str, Any]:
    """The language this build understands — or this caller may use.

    - `stageflow` — the version of the core, for a human reading a log;
    - `node_types` — the types a pipeline may use here, sorted. This is the
      field a client should branch on: a name that is not in the list will be
      rejected by `Pipeline.from_dict` with `Unknown node type`, or by the
      policy.
    - `stages` — how many stages may be used. The specs themselves are a
      separate, much larger answer (`get_stages`), so this is only a hint that
      a registry is there at all.
    - `locales` — the languages the core can put its own text into, the source
      language first. A client cannot guess this: a catalog is a file in the
      build, so the build has to say.

    With a `policy` the answer narrows to what that policy allows, which is
    what a backend should serve to a client: an editor needs to know what
    *this* caller may draw, and the reason a node is unavailable — an older
    core or a narrower allowance — is not a distinction it has to make.

    This exists because a version number cannot answer the question a client
    actually has. An editor built against one version and pointed at whatever
    backend is running needs to know whether it may offer a `map` node here,
    and a build with a node type from a plugin belongs to no range of
    releases. So the registry answers for itself.
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
    }
