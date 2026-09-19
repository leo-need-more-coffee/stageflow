"""Узел ``subpipeline`` — вызов вложенного пайплайна.

Единственная настоящая граница видимости данных:
ребёнок стартует со СВЕЖИМ фреймом, пересечение с родителем — только через
явные ``inputs`` / ``artifact_outputs`` / ``result_output``.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from ...exceptions import ArtifactNotFoundError, PipelineDefinitionError
from ..context import Context
from .base import Node, register_node
from .recovery import run_with_retry

if TYPE_CHECKING:  # pragma: no cover
    from ..pipeline import Pipeline
    from ..session import Session


@register_node("subpipeline")
class SubPipelineNode(Node):
    def __init__(
        self,
        id: str,
        subpipeline_id: str,
        inputs: dict | None = None,
        artifact_outputs: dict | None = None,
        result_output: str | None = None,
        next: str | None = None,
        **common,
    ):
        super().__init__(id, **common)
        self.subpipeline_id = subpipeline_id
        self.inputs = inputs or {}
        self.artifact_outputs = artifact_outputs or {}
        self.result_output = result_output
        self.next = next

    @classmethod
    def _parse(cls, data: dict) -> "SubPipelineNode":
        subpipeline_id = data.get("subpipeline_id")
        if not subpipeline_id:
            raise PipelineDefinitionError(
                f"Node '{data.get('id')}': поле 'subpipeline_id' обязательно"
            )
        return cls(
            subpipeline_id=subpipeline_id,
            inputs=data.get("inputs", {}),
            artifact_outputs=data.get("artifact_outputs", {}),
            result_output=data.get("result_output"),
            next=data.get("next"),
            **Node._common(data),
        )

    def validate(self, pipeline: "Pipeline") -> list[str]:
        errors = self._validate_common(pipeline)
        if self.subpipeline_id not in pipeline.subpipelines:
            errors.append(f"{self.id}: субпайплайн '{self.subpipeline_id}' не найден")
        elif self.subpipeline_id == pipeline.entry:
            errors.append(f"{self.id}: субпайплайн не может ссылаться на корневой entry")
        if self.next and not pipeline.has_node(self.next):
            errors.append(f"{self.id}: next '{self.next}' не найден в графе")
        return errors

    async def execute(self, session: "Session", ctx: Context) -> tuple[Node | None, Context]:
        async def body() -> tuple[Node | None, Context]:
            child_ctx = Context()
            for child_name, parent_name in self.inputs.items():
                child_ctx = child_ctx.with_var(child_name, ctx.get_var(parent_name))

            child_result = await session.run_subpipeline(self, child_ctx)

            types = session.pipeline.typesystem
            new_ctx = ctx
            for parent_name, child_name in self.artifact_outputs.items():
                if child_name not in child_result.artifacts:
                    raise ArtifactNotFoundError(
                        f"{self.id}: субпайплайн '{self.subpipeline_id}' не отдал "
                        f"артефакт '{child_name}' (есть: {sorted(child_result.artifacts)})"
                    )
                value = child_result.artifacts[child_name]
                types.check_write(parent_name, value, self.id)
                new_ctx = new_ctx.with_var(parent_name, value)
            if self.result_output:
                types.check_write(self.result_output, child_result.result, self.id)
                new_ctx = new_ctx.with_var(self.result_output, child_result.result)
            new_ctx = self.apply_expose(session, new_ctx)

            session.emit_node_event(
                "subpipeline_completed", self, {"subpipeline_id": self.subpipeline_id}
            )
            return self._goto(session, self.next), new_ctx

        return await run_with_retry(self, session, ctx, body)
