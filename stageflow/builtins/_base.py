"""Base class for the built-in stages.

Its only purpose is `i18n_domain`: built-in stage descriptions are translated
with the framework's own catalog, unlike a host's stages.
"""

from __future__ import annotations

from ..core.stage import BaseStage
from ..i18n import DOMAIN


class BuiltinStage(BaseStage):
    i18n_domain = DOMAIN
