from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any

from ...exceptions import PipelineDefinitionError, StageOutputError, TypeCheckError
from ...i18n import _
from ..context import Context
from .base import Node, register_node
from .bindings import normalize_bucket
from .recovery import error_name, run_with_retry

if TYPE_CHECKING:  # pragma: no cover
    from ..pipeline import Pipeline
    from ..session import Session

SEQUENTIAL = "sequential"
PARALLEL = "parallel"
MODES = (SEQUENTIAL, PARALLEL)


@register_node("map")
class MapNode(Node):
    """One region of the graph, run once per element of a list.

    The body is a region exactly like the body of a `try` block: everything
    reachable from `body` but not from `next`. Every iteration runs it on its
    own copy of the frame, with the element bound to `item_var`, so iterations
    never see each other's writes. The only way out is `collect`: a name
    written by the body becomes a list outside, one entry per element, in the
    order of the items — not the order in which the iterations finished.
    """

    def __init__(
        self,
        id: str,
        items: str,
        body: str,
        item_var: str = "item",
        index_var: str | None = None,
        collect: dict | list | None = None,
        mode: str = SEQUENTIAL,
        cancel_on_error: bool = True,
        next: str | None = None,
        **common,
    ):
        super().__init__(id, **common)
        self.items = items
        self.body = body
        self.item_var = item_var
        self.index_var = index_var
        self.collect = normalize_bucket(collect)
        self.mode = mode
        self.cancel_on_error = cancel_on_error
        self.next = next
        self._scope: frozenset[str] | None = None

    @classmethod
    def _parse(cls, data: dict) -> "MapNode":
        for field in ("items", "body"):
            if not data.get(field):
                raise PipelineDefinitionError(
                    _("Node '{node}': field '{field}' is required",
                      node=data.get("id"), field=field)
                )
        return cls(
            items=data["items"],
            body=data["body"],
            item_var=data.get("item_var", "item"),
            index_var=data.get("index_var"),
            collect=data.get("collect"),
            mode=data.get("mode", SEQUENTIAL),
            cancel_on_error=data.get("cancel_on_error", True),
            next=data.get("next"),
            **Node._common(data),
        )

    def scope(self, pipeline: "Pipeline") -> frozenset[str]:
        if self._scope is None:
            after = pipeline.reachable([self.next]) if self.next else frozenset()
            self._scope = pipeline.reachable([self.body], stop_at=after) - {self.id}
        return self._scope

    def order_targets(self) -> list[str]:
        return [t for t in (self.body, self.next) if t]

    def validate(self, pipeline: "Pipeline") -> list[str]:
        errors = self._validate_common(pipeline)
        if not pipeline.has_node(self.body):
            errors.append(
                _("{node}: body '{target}' not found in the graph",
                  node=self.id, target=self.body)
            )
        if self.next and not pipeline.has_node(self.next):
            errors.append(
                _("{node}: next '{target}' not found in the graph",
                  node=self.id, target=self.next)
            )
        if self.mode not in MODES:
            errors.append(
                _("{node}: mode '{mode}' is not one of {allowed}",
                  node=self.id, mode=self.mode, allowed=", ".join(MODES))
            )
        errors.extend(self._validate_names())
        if pipeline.has_node(self.body):
            errors.extend(self._validate_closed(pipeline))
        return errors

    def _validate_names(self) -> list[str]:
        errors: list[str] = []
        names = [(self.item_var, "item_var")]
        if self.index_var is not None:
            names.append((self.index_var, "index_var"))
        for name, field in names:
            if not (isinstance(name, str) and name.isidentifier()):
                errors.append(
                    _("{node}: {field} '{name}' is not a valid variable name",
                      node=self.id, field=field, name=name)
                )
        if self.index_var is not None and self.index_var == self.item_var:
            errors.append(
                _("{node}: item_var and index_var are the same name", node=self.id)
            )
        for src, dst in self.collect.items():
            # one sentence per side, not a noun dropped into one — `registry.py`
            for name, message in (
                (src, _("{node}: collect source '{name}' must be a variable name")),
                (dst, _("{node}: collect destination '{name}' must be a variable name")),
            ):
                if not (isinstance(name, str) and name.isidentifier()):
                    errors.append(message.format(node=self.id, name=name))
        return errors

    def _validate_closed(self, pipeline: "Pipeline") -> list[str]:
        """A road inside the loop must end inside it.

        A `try` block lets a road leave — the block simply stops owning it.
        A loop cannot: leaving mid-iteration would abandon the elements that
        are still to come, and in parallel mode there would be no single frame
        to continue with. So a way out is a mistake in the graph, and it is
        better said at validation than discovered on the tenth element.
        """
        scope = self.scope(pipeline)
        errors: list[str] = []
        for node_id in sorted(scope):
            for target in pipeline.get_node(node_id).order_targets():
                if target in scope or not pipeline.has_node(target):
                    continue
                errors.append(
                    _("{node}: '{from_node}' leads to '{target}', outside the loop "
                      "body; a road inside the body must end inside it",
                      node=self.id, from_node=node_id, target=target)
                )
        return errors

    async def execute(self, session: "Session", ctx: Context) -> tuple[Node | None, Context]:
        async def body() -> tuple[Node | None, Context]:
            items = self._items_of(session, ctx)
            # how many times a graph goes round is decided by the data, so
            # this is the meter that makes a loop's cost knowable at all
            session.budget.charge("iterations", len(items))
            scope = self.scope(session.pipeline)
            session.emit_node_event(
                "map_started",
                self,
                {"items": len(items), "mode": self.mode, "body": self.body},
            )

            if self.mode == PARALLEL:
                frames = await self._run_parallel(session, ctx, scope, items)
            else:
                frames = await self._run_sequential(session, ctx, scope, items)

            if session.finished:
                return None, ctx

            out = self._collected(ctx, session, frames)
            out = self.apply_expose(session, out)
            session.emit_node_event(
                "map_completed",
                self,
                {"items": len(items), "collected": sorted(self.collect.values())},
            )
            return self._goto(session, self.next), out

        return await run_with_retry(self, session, ctx, body)

    def _items_of(self, session: "Session", ctx: Context) -> list[Any]:
        value = session.cel.eval(self.items, ctx)
        if isinstance(value, (list, tuple)):
            return list(value)
        raise TypeCheckError(
            _("{node}: items '{items}' must be a list, got {got}",
              node=self.id, items=self.items, got=type(value).__name__)
        )

    async def _run_sequential(
        self, session: "Session", ctx: Context, scope: frozenset[str], items: list
    ) -> list[Context | None]:
        frames: list[Context | None] = [None] * len(items)
        first_error: BaseException | None = None
        for index, item in enumerate(items):
            if session.finished:
                break
            try:
                frames[index] = await self._run_item(session, ctx, scope, index, item)
            except Exception as exc:  # noqa: BLE001
                self._emit_failure(session, index, exc)
                if self.cancel_on_error:
                    raise
                first_error = first_error or exc
        if first_error is not None:
            raise first_error
        return frames

    async def _run_parallel(
        self, session: "Session", ctx: Context, scope: frozenset[str], items: list
    ) -> list[Context | None]:
        tasks = [
            asyncio.create_task(self._run_item(session, ctx, scope, index, item))
            for index, item in enumerate(items)
        ]
        if not tasks:
            return []

        mode = asyncio.FIRST_EXCEPTION if self.cancel_on_error else asyncio.ALL_COMPLETED
        try:
            _, pending = await asyncio.wait(tasks, return_when=mode)
        except asyncio.CancelledError:
            for task in tasks:
                task.cancel()
            raise
        if pending:
            cancelled = [index for index, task in enumerate(tasks) if task in pending]
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            session.emit_node_event("map_cancelled", self, {"items": sorted(cancelled)})

        for index, task in enumerate(tasks):
            if task.cancelled() or task.exception() is None:
                continue
            self._emit_failure(session, index, task.exception())
            raise task.exception()
        return [None if task.cancelled() else task.result() for task in tasks]

    async def _run_item(
        self,
        session: "Session",
        base: Context,
        scope: frozenset[str],
        index: int,
        item: Any,
    ) -> Context:
        types = session.pipeline.typesystem
        types.check_write(self.item_var, item, self.id)
        ctx = base.with_var(self.item_var, item)
        if self.index_var:
            types.check_write(self.index_var, index, self.id)
            ctx = ctx.with_var(self.index_var, index)

        session.emit_node_event("map_item_started", self, {"index": index})
        # one unit of concurrency per iteration: in parallel mode this is
        # what stands between a list of ten thousand and ten thousand calls
        # in the air. A list that is longer than the allowance waits its turn
        # rather than failing — it is wide, not wrong
        async with session.budget.slot("concurrency"):
            _, ctx = await session.run_scope(
                session.pipeline.get_node(self.body), ctx, scope
            )
        session.emit_node_event("map_item_completed", self, {"index": index})
        return ctx

    def _emit_failure(self, session: "Session", index: int, exc: BaseException) -> None:
        session.emit_node_event(
            "map_item_failed",
            self,
            {"index": index, "error": str(exc), "type": error_name(exc)},
        )

    def _collected(
        self, ctx: Context, session: "Session", frames: list[Context | None]
    ) -> Context:
        types = session.pipeline.typesystem
        for src, dst in self.collect.items():
            values = []
            for index, frame in enumerate(frames):
                if frame is None or not frame.has_var(src):
                    raise StageOutputError(
                        _("{node}: item {index} did not write '{source}', "
                          "collected as '{target}'",
                          node=self.id, index=index, source=src, target=dst)
                    )
                values.append(frame.get_var(src))
            types.check_write(dst, values, self.id)
            ctx = ctx.with_var(dst, values)
        return ctx
