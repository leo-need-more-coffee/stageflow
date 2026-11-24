from __future__ import annotations

from typing import TYPE_CHECKING

from ...i18n import _
from ..context import Context
from ..durable import parse_duration
from .base import Node, register_node
from .bindings import apply_outputs

if TYPE_CHECKING:  # pragma: no cover
    from ..pipeline import Pipeline
    from ..session import Session


@register_node("wait")
class WaitNode(Node):
    """Waits for an input of one type, for as long as it takes.

    With a checkpoint store the session is saved and `run()` returns with
    `{"status": "waiting"}`; `Session.resume` continues it when the input
    arrives, in any process. Without a store it waits in memory. Either way
    the payload's fields go to variables through `outputs`, as a stage's do,
    and with a `timeout` the run goes to `on_timeout` if nothing came.

    A wait cannot be inside a `try`, `map` or `parallel` block or in a
    subpipeline: a saved run resumes at a node of the main graph.
    """

    def __init__(
        self,
        id: str,
        input: str,
        outputs: dict | None = None,
        timeout: float | str | None = None,
        on_timeout: str | None = None,
        next: str | None = None,
        **common,
    ):
        super().__init__(id, **common)
        self.input = input
        self.outputs = outputs or {}
        self.timeout = timeout
        self.on_timeout = on_timeout
        self.next = next

    @classmethod
    def _parse(cls, data: dict) -> "WaitNode":
        return cls(
            input=data["input"],
            outputs=data.get("outputs", {}),
            timeout=data.get("timeout"),
            on_timeout=data.get("on_timeout"),
            next=data.get("next"),
            **Node._common(data),
        )

    @property
    def timeout_seconds(self) -> float | None:
        return None if self.timeout is None else parse_duration(self.timeout)

    def order_targets(self) -> list[str]:
        return [t for t in (self.next, self.on_timeout) if t]

    def validate(self, pipeline: "Pipeline") -> list[str]:
        errors = self._validate_common(pipeline)
        if self.next and not pipeline.has_node(self.next):
            errors.append(
                _("{node}: next '{target}' not found in the graph",
                  node=self.id, target=self.next)
            )
        if self.on_timeout and not pipeline.has_node(self.on_timeout):
            errors.append(
                _("{node}: on_timeout '{target}' not found in the graph",
                  node=self.id, target=self.on_timeout)
            )
        if self.timeout is not None:
            try:
                parse_duration(self.timeout)
            except ValueError:
                errors.append(
                    _("{node}: timeout {value} is not a duration "
                      "(seconds, or e.g. '90s', '15m', '2h', '7d')",
                      node=self.id, value=repr(self.timeout))
                )
        if (self.timeout is None) != (self.on_timeout is None):
            errors.append(
                _("{node}: timeout and on_timeout go together: say how long to "
                  "wait and where to go when nothing came", node=self.id)
            )
        for block in pipeline.nodes:
            if self.id in block.enclosed(pipeline):
                errors.append(
                    _("{node}: a wait node cannot be inside the {type} node "
                      "'{block}': a saved run resumes at a node of the main graph",
                      node=self.id, type=block.type, block=block.id)
                )
        return errors

    async def execute(self, session: "Session", ctx: Context) -> tuple[Node | None, Context]:
        entry = await session.wait_at(self)
        if entry is None:
            session.emit_node_event("wait_timed_out", self, {"input": self.input})
            return self._goto(session, self.on_timeout), ctx
        ctx = apply_outputs(
            self.outputs, ctx, entry.get("payload") or {}, session.cel,
            types=session.pipeline.typesystem, owner=self.id,
        )
        ctx = self.apply_expose(session, ctx)
        session.emit_node_event("wait_completed", self, {"input": self.input})
        return self._goto(session, self.next), ctx
