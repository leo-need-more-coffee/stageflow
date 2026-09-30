"""The base of the stages that ship with the framework.

It exists for one attribute. The prose of a built-in stage is the framework's
own text, translated in the framework's own catalog, while a host's stage is
translated by whoever wrote it — and the difference has to be written down
somewhere. Written here, it is written once.
"""

from __future__ import annotations

from ..core.stage import BaseStage
from ..i18n import DOMAIN


class BuiltinStage(BaseStage):
    i18n_domain = DOMAIN
