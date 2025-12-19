from dataclasses import dataclass
from typing import Literal

from .jsonlogic import JsonLogic
from .stage import get_stage, BaseStage


class Node:
    id: str
    type: str
    guards: list
    metadata: dict

    def __init__(self, id: str, type: str, guards: list = None, metadata: dict = None):
        self.id = id
        self.type = type
        self.guards = guards or []
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
        guards: list = None,
        metadata: dict = None,
    ):
        super().__init__(id, type, guards, metadata)
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
        guards = data.get("guards", [])
        metadata = data.get("metadata", {})
        return ConditionNode(
            id=id,
            type=type,
            conditions=conditions,
            else_goto=else_goto,
            guards=guards,
            metadata=metadata,
        )


class ParallelNode(Node):
    children: list[str]
    policy: Literal["all", "any"]
    next: str | None

    def __init__(
        self,
        id: str,
        type: str,
        children: list[str],
        policy: Literal["all", "any"] = "all",
        next: str | None = None,
        guards: list = None,
        metadata: dict = None,
    ):
        super().__init__(id, type, guards, metadata)
        self.children = children
        self.policy = policy
        self.next = next

    def from_dict(data: dict) -> "ParallelNode":
        id = data.get("id")
        type = data.get("type")
        children = data.get("children", [])
        policy = data.get("policy", "all")
        next = data.get("next")
        guards = data.get("guards", [])
        metadata = data.get("metadata", {})
        return ParallelNode(
            id=id,
            type=type,
            children=children,
            policy=policy,
            next=next,
            guards=guards,
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
        guards: list = None,
        metadata: dict = None,
    ):
        super().__init__(id, type, guards, metadata)
        self.artifact_paths = artifact_paths or []
        self.result = result

    @staticmethod
    def from_dict(data: dict) -> "TerminalNode":
        id = data.get("id")
        type = data.get("type")
        artifact_paths = data.get("artifacts", [])
        result = data.get("result")
        guards = data.get("guards", [])
        metadata = data.get("metadata", {})
        return TerminalNode(
            id=id,
            type=type,
            artifact_paths=artifact_paths,
            result=result,
            guards=guards,
            metadata=metadata,
        )


class StageNode(Node):
    id: str
    type: str
    stage: str
    config: dict = {}
    inputs: dict = {}
    outputs: dict = {}
    next: str | None = None

    def __init__(
        self,
        id: str,
        type: str,
        stage: str,
        config: dict = None,
        arguments: dict = None,
        outputs: dict = None,
        next: str | None = None,
        guards: list = None,
        metadata: dict = None,
    ):
        super().__init__(id, type, guards, metadata)
        if not get_stage(stage):
            raise ValueError(f"Stage '{stage}' not found in registry")
        self.stage = stage
        self.config = config or {}
        self.arguments = arguments or {}
        self.outputs = outputs or {}
        self.next = next

    @staticmethod
    def from_dict(data: dict) -> "StageNode":
        id = data.get("id")
        type = data.get("type")
        stage = data.get("stage")
        config = data.get("config", {})
        arguments = data.get("arguments", {})
        outputs = data.get("outputs", {})
        next = data.get("next")
        guards = data.get("guards", [])
        metadata = data.get("metadata", {})
        return StageNode(
            id=id,
            type=type,
            stage=stage,
            config=config,
            arguments=arguments,
            outputs=outputs,
            next=next,
            guards=guards,
            metadata=metadata,
        )

    def get_stage_class(self):
        return get_stage(self.stage)
