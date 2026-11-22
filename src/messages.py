"""Protocol messages and their JSON encoding.

Every message is a dataclass. On the wire it is one line of JSON with an
extra "type" field naming the class, so the receiver knows which class to
rebuild.
"""
from dataclasses import dataclass, asdict
import json

_REGISTRY: dict[str, type] = {}


def message(cls):
    """Class decorator: make a dataclass decodable by its name."""
    _REGISTRY[cls.__name__] = cls
    return cls


def encode(msg) -> bytes:
    return (json.dumps({"type": type(msg).__name__, **asdict(msg)}) + "\n").encode()


def decode(raw: bytes | str):
    data = json.loads(raw)
    cls = _REGISTRY.get(data.pop("type", None))
    if cls is None:
        raise ValueError(f"unknown message type: {raw!r}")
    return cls(**data)


# --- chat messages (replication.py) --- #

@message
@dataclass
class Submit:
    sender_id: int
    msg_id: str      # unique per message, used to drop duplicates
    text: str


@message
@dataclass
class OrderedMessage:
    seq: int         # global sequence number, starting from 0
    msg_id: str
    sender_id: int
    text: str


# --- election messages (election.py) --- #

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
