"""Protocol messages and their JSON encoding.

Every message is a dataclass. On the wire it is one line of JSON with an
extra "type" field naming the class, so the receiver knows which class to
rebuild.

Every message also carries the sender's current `term`. Node.dispatch() uses
it to ignore messages from outdated leaders (see node.py).
"""
import json
from dataclasses import asdict, dataclass
from typing import Any

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
    term: int
    msg_id: str      # unique per message, used to drop duplicates
    text: str


@message
@dataclass
class OrderedMessage:
    seq: int         # global sequence number, starting from 0
    msg_id: str
    sender_id: int   # the node that wrote the message, not the leader
    text: str
    term: int        # term of the leader that sent this copy


# --- catch-up messages (replication.py) --- #

@message
@dataclass
class StateRequest:
    sender_id: int
    term: int


@message
@dataclass
class State:
    sender_id: int
    term: int
    last_seq: int    # last seq with no gaps before it, -1 if the log is empty


@message
@dataclass
class SyncRequest:
    sender_id: int
    term: int
    from_seq: int


@message
@dataclass
class SyncResponse:
    sender_id: int
    term: int
    messages: list[OrderedMessage]

    def __post_init__(self) -> None:
        # JSON decoding gives plain dicts; turn them back into OrderedMessage.
        raw: list[Any] = list(self.messages)
        self.messages = [OrderedMessage(**m) if isinstance(m, dict) else m for m in raw]


# --- election messages (election.py) --- #

@message
@dataclass
class Heartbeat:
    sender_id: int
    term: int


@message
@dataclass
class HeartbeatReply:
    """Follower -> leader: "I am alive"."""
    sender_id: int
    term: int


@message
@dataclass
class StaleTerm:
    """Tells a node that used an old term what the current term is."""
    sender_id: int
    term: int


@message
@dataclass
class Election:
    sender_id: int
    term: int


@message
@dataclass
class Answer:
    sender_id: int
    term: int


@message
@dataclass
class Coordinator:
    sender_id: int
    term: int
