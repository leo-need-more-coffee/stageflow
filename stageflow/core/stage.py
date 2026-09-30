from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from ..exceptions import StageContractError
from ..i18n import _
from .event import Event, EventSpec, InputSpec
from .payload_schema import validate_schema
from .registry import Registry
from .spec import build_stage_spec

if TYPE_CHECKING:  # pragma: no cover
    from .session import Session

_stages: Registry[type["BaseStage"]] = Registry("stage")


def _not_allowed(kind: str, type_: str, stage: str) -> str:
    """Two whole sentences rather than one with the noun filled in — see the
    same problem, with the reason, in `registry.py`."""
    if kind == "Event":
        return _("Event type '{type}' is not allowed for stage '{stage}'",
                 type=type_, stage=stage)
    if kind == "Input":
        return _("Input type '{type}' is not allowed for stage '{stage}'",
                 type=type_, stage=stage)
    return _("{kind} type '{type}' is not allowed for stage '{stage}'",
             kind=kind, type=type_, stage=stage)


def register_stage(name: str):
    def decorator(cls: type["BaseStage"]) -> type["BaseStage"]:
        cls.stage_name = name
        _stages.add(name, cls)
        return cls

    return decorator


def get_stage(name: str) -> type["BaseStage"]:
    return _stages.get(name)


def get_stages() -> dict[str, type["BaseStage"]]:
    return _stages.as_dict()


def get_stages_by_category() -> dict[str, list[type["BaseStage"]]]:
    categories: dict[str, list[type["BaseStage"]]] = {}
    for stage_cls in _stages.as_dict().values():
        categories.setdefault(stage_cls.category or "default", []).append(stage_cls)
    return categories


class BaseStage:
    stage_name: str = "BaseStage"
    category: str | None = None
    skipable: bool = False
    allowed_events: list[EventSpec] = []
    allowed_inputs: list[InputSpec] = []
    timeout: float | None = 30
    #: the gettext domain whose catalog translates this stage's prose. `None`
    #: — there is none, so the docstring's text is used as written, or is a
    #: `{locale: text}` mapping resolved without any catalog at all. A host
    #: that keeps .po files registers its domain once
    #: (`stageflow.i18n.register_domain`) and names it here
    i18n_domain: str | None = None

    def __init__(self, stage_id: str, arguments: dict, session: "Session"):
        self.stage_id = stage_id
        self.arguments = arguments or {}
        self.session = session
        self.collected_outputs: dict[str, Any] = {}
        #: what this run actually spent, by meter — see `charge`
        self.charged: dict[str, float] = {}

    def charge(self, **meters: float) -> None:
        """Report what this run of the stage actually consumed.

        Units, not money: `tokens`, `llm_calls`, `http_calls`, `rows`. What a
        unit is worth is a price list, price lists change without any code
        changing, and they belong to the host — a stage knows how much it
        used, not what that costs.

        The figure replaces whatever the spec's `reserve` held for the same
        meter, so an amount both reserved and charged is not counted twice.
        Call it as many times as you like; the amounts add up within one run.
        """
        for meter, amount in meters.items():
            self.charged[meter] = self.charged.get(meter, 0.0) + float(amount)

    def get_arguments(self) -> dict[str, Any]:
        return dict(self.arguments)

    def set_outputs(self, outputs: dict[str, Any]) -> None:
        self.collected_outputs.update(outputs)

    async def run(self) -> None:
        raise NotImplementedError

    def emit(self, event_type: str, payload: dict | None = None) -> None:
        spec = self._check_allowed(event_type, self.allowed_events, "Event")
        if spec is not None and spec.payload_schema is not None:
            validate_schema(payload or {}, spec.payload_schema, "Event payload")
        self.session.emit(
            Event(
                type=event_type,
                session_id=self.session.id,
                stage_id=self.stage_id,
                payload=payload or {},
            )
        )

    def start_wait_input(self, type_: str) -> asyncio.Future:
        self._check_allowed(type_, self.allowed_inputs, "Input")
        return self.session.start_wait_input(type_)

    async def finish_wait_input(
        self, type_: str, fut: asyncio.Future, timeout: float | None = None
    ) -> dict | None:
        result = await self.session.finish_wait_input(type_, fut, timeout=timeout)
        return self._validated_input(type_, result)

    async def wait_input(self, type_: str, timeout: float | None = None) -> dict | None:
        self._check_allowed(type_, self.allowed_inputs, "Input")
        result = await self.session.wait_input(type_, timeout=timeout)
        return self._validated_input(type_, result)

    def _validated_input(self, type_: str, result: dict | None) -> dict | None:
        if result is None:
            return None
        spec = self._check_allowed(type_, self.allowed_inputs, "Input")
        if spec is not None and spec.payload_schema is not None:
            validate_schema(result.get("payload", {}), spec.payload_schema, "Input payload")
        return result

    def _check_allowed(
        self, type_: str, specs: list[EventSpec] | list[InputSpec], kind: str
    ) -> EventSpec | InputSpec | None:
        if not specs:
            return None
        declared = {spec.type for spec in specs if spec.type}
        if declared and type_ not in declared:
            raise StageContractError(
                _not_allowed(kind, type_, self.stage_name)
            )
        return next((spec for spec in specs if spec.type == type_), None)

    @classmethod
    def get_specs(cls, locale: str | None = None) -> dict[str, Any]:
        """This stage's card.

        `None`, the default, puts every language the build can answer in into
        the prose as a `{locale: text}` mapping, and whatever draws the card
        chooses. A backend serving an editor wants exactly that: the reader
        picks a language in the editor, long after the specs were fetched, and
        nothing about that choice is the backend's to make.

        Naming a locale collapses the prose to one language instead.
        """
        return build_stage_spec(cls, locale)
