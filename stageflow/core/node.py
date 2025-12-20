from dataclasses import dataclass
from typing import Literal

from .jsonlogic import JsonLogic
from .stage import get_stage


class Node:
    id: str
    type: str
    metadata: dict

    def __init__(self, id: str, type: str, metadata: dict = None):
        self.id = id
        self.type = type
        self.metadata = metadata or {}

    @staticmethod
    def from_dict(data: dict) -> "Node":
        node_type = data.get("type")
        match node_type:
            case "condition":
                return ConditionNode.from_dict(data)
            case "parallel":
                return ParallelNode.from_dict(data)
            case "terminal":
                return TerminalNode.from_dict(data)
            case "stage":
                return StageNode.from_dict(data)
            case "subpipeline":
                return SubPipelineNode.from_dict(data)
            case _:
                raise ValueError(f"Unknown node type: {node_type}")


@dataclass
class Condition:
    if_condition: JsonLogic
    then_goto: str


class ConditionNode(Node):
    conditions: list[Condition]
    else_goto: str | None

    def __init__(
        self,
        id: str,
        type: str,
        conditions: list[Condition],
        else_goto: str | None = None,
        metadata: dict = None,
    ):
        super().__init__(id, type, metadata)
        self.conditions = conditions
        self.else_goto = else_goto

    @staticmethod
    def from_dict(data: dict) -> "ConditionNode":
        id = data.get("id")
        type = data.get("type")
        conditions_data = data.get("conditions", [])
        conditions = [
            Condition(if_condition=JsonLogic(cond["if"]), then_goto=cond["then"])
            for cond in conditions_data
        ]
        else_goto = data.get("else")
        metadata = data.get("metadata", {})
        return ConditionNode(
            id=id,
            type=type,
            conditions=conditions,
            else_goto=else_goto,
            metadata=metadata,
        )


class ParallelNode(Node):
    children: list[str]
    policy: Literal["all", "any"]
    next: str | None
    cancel_on_error: bool

    def __init__(
        self,
        id: str,
        type: str,
        children: list[str],
        policy: Literal["all", "any"] = "all",
        next: str | None = None,
        cancel_on_error: bool = True,
        metadata: dict = None,
    ):
        super().__init__(id, type, metadata)
        self.children = children
        self.policy = policy
        self.next = next
        self.cancel_on_error = cancel_on_error

    @staticmethod
    def from_dict(data: dict) -> "ParallelNode":
        id = data.get("id")
        type = data.get("type")
        children = data.get("children", [])
        policy = data.get("policy", "all")
        next = data.get("next")
        cancel_on_error = data.get("cancel_on_error", True)
        metadata = data.get("metadata", {})
        return ParallelNode(
            id=id,
            type=type,
            children=children,
            policy=policy,
            next=next,
            cancel_on_error=cancel_on_error,
            metadata=metadata,
        )


class TerminalNode(Node):
    artifact_paths: list[str]
    result: dict | None

    def __init__(
        self,
        id: str,
        type: str,
        artifact_paths: list[str] = None,
        result: dict | None = None,
        metadata: dict = None,
    ):
        super().__init__(id, type, metadata)
        self.artifact_paths = artifact_paths or []
        self.result = result

    @staticmethod
    def from_dict(data: dict) -> "TerminalNode":
        id = data.get("id")
        type = data.get("type")
        artifact_paths = data.get("artifacts", [])
        result = data.get("result")
        metadata = data.get("metadata", {})
        return TerminalNode(
            id=id,
            type=type,
            artifact_paths=artifact_paths,
            result=result,
            metadata=metadata,
        )


class SubPipelineNode(Node):
    subpipeline_id: str
    inputs: dict
    artifact_outputs: dict
    result_output: str | None
    next: str | None

    def __init__(
        self,
        id: str,
        type: str,
        subpipeline_id: str,
        inputs: dict = None,
        artifact_outputs: dict = None,
        result_output: str | None = None,
        next: str | None = None,
        metadata: dict = None,
    ):
        super().__init__(id, type, metadata)
        self.subpipeline_id = subpipeline_id
        self.inputs = inputs or {}
        self.artifact_outputs = artifact_outputs or {}
        self.result_output = result_output
        self.next = next

    @staticmethod
    def from_dict(data: dict) -> "SubPipelineNode":
        id = data.get("id")
        type = data.get("type")
        subpipeline_id = data.get("subpipeline_id")
        if not subpipeline_id:
            raise ValueError("subpipeline_id is required for subpipeline node")
        inputs = data.get("inputs", {})
        artifact_outputs = data.get("artifact_outputs", {})
        result_output = data.get("result_output")
        next = data.get("next")
        metadata = data.get("metadata", {})
        return SubPipelineNode(
            id=id,
            type=type,
            subpipeline_id=subpipeline_id,
            inputs=inputs,
            artifact_outputs=artifact_outputs,
            result_output=result_output,
            next=next,
            metadata=metadata,
        )

class StageNode(Node):
    stage: str
    config: dict = {}
    inputs: dict = {}
    outputs: dict = {}
    next: str | None = None
    fallback: str | None = None

    def __init__(
        self,
        id: str,
        type: str,
        stage: str,
        config: dict = None,
        arguments: dict = None,
        outputs: dict = None,
        next: str | None = None,
        fallback: str | None = None,
        metadata: dict = None,
    ):
        super().__init__(id, type, metadata)
        if not get_stage(stage):
            raise ValueError(f"Stage '{stage}' not found in registry")
        self.stage = stage
        self.config = config or {}
        self.arguments = arguments or {}
        self.outputs = outputs or {}
        self.next = next
        self.fallback = fallback

    @staticmethod
    def from_dict(data: dict) -> "StageNode":
        id = data.get("id")
        type = data.get("type")
        stage = data.get("stage")
        config = data.get("config", {})
        arguments = data.get("arguments", {})
        outputs = data.get("outputs", {})
        next = data.get("next")
        fallback = data.get("fallback")
        metadata = data.get("metadata", {})
        return StageNode(
            id=id,
            type=type,
            stage=stage,
            config=config,
            arguments=arguments,
            outputs=outputs,
            next=next,
            fallback=fallback,
            metadata=metadata,
        )

    def get_stage_class(self):
        return get_stage(self.stage)
