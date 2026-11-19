import tomli
from .node import Node, Address

def load_node(path: str, node_id: int) -> Node:
    with open(path, "rb") as f:
        data = tomli.load(f)
    nodes = {n["id"]: Address(n["host"], n["port"]) for n in data["nodes"]}
    if node_id not in nodes:
        raise SystemExit(f"node id {node_id} not in {path}")
    me = nodes.pop(node_id)
    return Node(node_id, me.host, me.port, peers=nodes)