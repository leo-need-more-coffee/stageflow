"""The pipeline format version, and upgrades from older versions.

A pipeline document says which format it is written in:

    {"version": 1, "nodes": [...]}

A document without `version` is version 1, the format every pipeline had
before the field existed. `Pipeline.from_dict` upgrades an older document
step by step to `FORMAT_VERSION` before checking it, so pipelines saved by an
older StageFlow keep loading. A document from a newer StageFlow is refused
with a message that says so, instead of failing on some field it does not
know.

To change the format incompatibly: bump `FORMAT_VERSION`, add a function that
turns a document of the previous version into the new one to `MIGRATIONS`,
and update the schema. A migration takes a document it may modify (a copy)
and returns it; it does not need to set `version`.
"""
from __future__ import annotations

import copy
from typing import Callable

from ..exceptions import PipelineDefinitionError
from ..i18n import _

#: The format this StageFlow writes and checks against.
FORMAT_VERSION = 1

Migration = Callable[[dict], dict]

#: `MIGRATIONS[n]` turns a version-`n` document into version `n + 1`.
MIGRATIONS: dict[int, Migration] = {}


def format_version(data: dict) -> int:
    """The version a document declares; 1 when it declares none."""
    version = data.get("version", 1)
    # bool is an int in Python; `true` is not a version
    if isinstance(version, bool) or not isinstance(version, int) or version < 1:
        raise PipelineDefinitionError(
            _("Pipeline 'version' must be a positive integer, got {value}",
              value=repr(version))
        )
    return version


def migrate(data: dict, target: int | None = None) -> dict:
    """Return `data` upgraded to `target` (default `FORMAT_VERSION`).

    The input is not modified. A document already at the target is returned
    as is, without a copy.
    """
    target = FORMAT_VERSION if target is None else target
    version = format_version(data)
    if version > target:
        raise PipelineDefinitionError(
            _("The pipeline is format version {version}, but this StageFlow reads "
              "up to version {supported}: it was saved by a newer StageFlow",
              version=version, supported=target)
        )
    if version == target:
        return data

    upgraded = copy.deepcopy(data)
    while version < target:
        step = MIGRATIONS.get(version)
        if step is None:  # pragma: no cover - a gap in MIGRATIONS is our bug
            raise PipelineDefinitionError(
                _("No upgrade from pipeline format version {version}", version=version)
            )
        upgraded = step(upgraded)
        version += 1
        upgraded["version"] = version
    return upgraded
