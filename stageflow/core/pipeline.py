from .node import Node, StageNode, ConditionNode, ParallelNode, TerminalNode, SubPipelineNode
from stageflow.docs.schema import load_pipeline_schema

try:
    from jsonschema import validate, ValidationError
except ImportError:  # pragma: no cover - optional dependency
    validate = None

    class ValidationError(Exception):
        pass


class Pipeline:
    entry: str
    nodes: list[Node]
    _nodes_map: dict[str, Node]
    metadata: dict
    raw_json: dict
    subpipelines: dict

    def __init__(
        self,
        entry: str,
        nodes: list[Node],
        metadata: dict = None,
        raw_json: dict = None,
        subpipelines: dict | None = None,
    ):
        self.entry = entry
        self.nodes = nodes
        self._nodes_map = {node.id: node for node in nodes}
        self.metadata = metadata or {}
        self.raw_json = raw_json or {}
        self.subpipelines = subpipelines or {}

    @staticmethod
    def from_dict(data: dict) -> "Pipeline":
        # Schema validation before constructing nodes (if jsonschema available).
        schema = None
        if validate:
            try:
                schema = load_pipeline_schema()
                validate(instance=data, schema=schema)
            except ValidationError as e:
                raise ValueError(f"Pipeline schema validation failed: {e.message}") from e
        entry = data.get("entry")
        nodes_data = data.get("nodes", [])
        nodes = [Node.from_dict(node) for node in nodes_data]
        metadata = data.get("metadata", {})
        subpipelines = data.get("subpipelines", {})
        # Validate subpipelines recursively.
        if validate and schema:
            for sub_id, sub_data in subpipelines.items():
                try:
                    validate(instance=sub_data, schema=schema)
                except ValidationError as e:
                    raise ValueError(f"Subpipeline '{sub_id}' schema validation failed: {e.message}") from e
        return Pipeline(entry=entry, nodes=nodes, metadata=metadata, raw_json=data, subpipelines=subpipelines)

    def get_node(self, node_id: str) -> Node:
        if node_id not in self._nodes_map:
            raise ValueError(f"Node with id {node_id} not found in pipeline")
        return self._nodes_map[node_id]

    def get_entry_node(self) -> Node:
        return self.get_node(self.entry)

    def validate(self) -> bool:
        if self.entry not in self._nodes_map:
            raise ValueError(f"Entry node '{self.entry}' not found in pipeline")

        for node in self.nodes:
            match node:
                case StageNode():
                    stage_class = node.get_stage_class()
                    if not stage_class:
                        raise ValueError(f"Stage class for node {node.id} not found")
                    if node.next and node.next not in self._nodes_map:
                        raise ValueError(f"Next node '{node.next}' for stage {node.id} not found in pipeline")
                    if node.fallback and node.fallback not in self._nodes_map:
                        raise ValueError(f"Fallback node '{node.fallback}' for stage {node.id} not found in pipeline")
                case ConditionNode():
                    for cond in node.conditions:
                        if cond.then_goto not in self._nodes_map:
                            raise ValueError(f"Condition target '{cond.then_goto}' not found in pipeline")
                    if node.else_goto and node.else_goto not in self._nodes_map:
                        raise ValueError(f"Condition else '{node.else_goto}' not found in pipeline")
                case ParallelNode():
                    for child in node.children:
                        if child not in self._nodes_map:
                            raise ValueError(f"Parallel branch '{child}' not found in pipeline")
                    if node.next and node.next not in self._nodes_map:
                        raise ValueError(f"Parallel next '{node.next}' not found in pipeline")
                case TerminalNode():
                    pass
                case SubPipelineNode():
                    if node.subpipeline_id not in self.subpipelines:
                        raise ValueError(f"Subpipeline '{node.subpipeline_id}' not found for node {node.id}")
                    # Basic cycle check: prevent direct self-reference
                    if node.subpipeline_id == self.entry:
                        raise ValueError(f"Subpipeline '{node.subpipeline_id}' cannot reference root entry")
                case _:
                    raise ValueError(f"Unknown node type: {type(node)}")

        return True
