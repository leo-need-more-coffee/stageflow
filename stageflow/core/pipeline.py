from .node import Node, StageNode, ConditionNode, ParallelNode, TerminalNode


class Pipeline:
    entry: str
    nodes: list[Node]
    _nodes_map: dict[str, Node]
    metadata: dict
    raw_json: dict

    def __init__(
        self,
        entry: str,
        nodes: list[Node],
        metadata: dict = None,
        raw_json: dict = None,
    ):
        self.entry = entry
        self.nodes = nodes
        self._nodes_map = {node.id: node for node in nodes}
        self.metadata = metadata or {}
        self.raw_json = raw_json or {}

    @staticmethod
    def from_dict(data: dict) -> "Pipeline":
        entry = data.get("entry")
        nodes_data = data.get("nodes", {})
        nodes = [Node.from_dict(node) for node in nodes_data]
        metadata = data.get("metadata", {})
        return Pipeline(entry=entry, nodes=nodes, metadata=metadata, raw_json=data)

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
                case TerminalNode():
                    pass
                case _:
                    raise ValueError(f"Unknown node type: {type(node)}")

        return True
