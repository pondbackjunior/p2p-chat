from dataclasses import dataclass, asdict
import json

_REGISTRY: dict[str, type] = {}

def message(cls):
    _REGISTRY[cls.__name__] = cls
    return cls

@message
@dataclass
class Submit:
    sender_id: int
    text: str

def encode(msg) -> bytes:
    return (json.dumps({"type": type(msg).__name__, **asdict(msg)}) + "\n").encode()

def decode(raw: bytes | str):
    data = json.loads(raw)
    cls = _REGISTRY.get(data.pop("type", None))
    if cls is None:
        raise ValueError(f"unknown message type: {raw!r}")
    return cls(**data)

@message
@dataclass
class Heartbeat:
    sender_id: int

@message
@dataclass
class Election:
    sender_id: int

@message
@dataclass
class Answer:
    sender_id: int

@message
@dataclass
class Coordinator:
    sender_id: int