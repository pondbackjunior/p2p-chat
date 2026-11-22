import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable
from typing import Any, NamedTuple, Optional, TypeVar

from .messages import OrderedMessage

log = logging.getLogger(__name__)

M = TypeVar("M")
Handler = Callable[[Any], Awaitable[None]]
LeaderListener = Callable[[int], None]
Sender = Callable[[int, object], Awaitable[bool]]


class Address(NamedTuple):
    host: str
    port: int


class Node:
    def __init__(self, node_id: int, host: str, port: int,
                 peers: Optional[dict[int, Address]] = None):
        # Identity
        self.node_id = node_id
        self.address = Address(host, port)
        self.peers: dict[int, Address] = dict(peers or {})

        # Election state (used by election.py). Change it with set_leader().
        self.leader_id: Optional[int] = None

        # Replication state (used by replication.py)
        self.next_seq: int = 0                 # leader only: next sequence number to assign
        self.pending: dict[int, OrderedMessage] = {}  # seq -> message that arrived early (hold-back)
        self.log: list[OrderedMessage] = []           # delivered messages; log[i] has seq i

        # Wiring
        self._handlers: dict[type, Handler] = {}
        self._leader_listeners: list[LeaderListener] = []
        self._sender: Optional[Sender] = None  # set by transport.py via attach_sender()

    # --- convenience --- #
    @property
    def is_leader(self) -> bool:
        return self.leader_id == self.node_id

    def other_ids(self) -> list[int]:
        return sorted(self.peers)

    def higher_ids(self) -> list[int]:
        """Peers with a higher ID than ours (the ones Bully contacts)."""
        return [pid for pid in self.other_ids() if pid > self.node_id]

    # --- sending --- #
    def attach_sender(self, sender: Sender) -> None:
        """Called by transport.py so other modules can send through the node."""
        self._sender = sender

    async def send(self, peer_id: int, msg: object) -> bool:
        """Send msg to one peer. Returns False if the peer is unreachable."""
        if self._sender is None:
            raise RuntimeError("transport not started: no sender attached")
        return await self._sender(peer_id, msg)

    # --- leader changes --- #
    def add_leader_listener(self, listener: LeaderListener) -> None:
        """Call listener(leader_id) whenever a new leader becomes known."""
        self._leader_listeners.append(listener)

    def set_leader(self, leader_id: Optional[int]) -> None:
        """Record the current leader (None = unknown) and notify listeners."""
        changed = leader_id != self.leader_id
        self.leader_id = leader_id
        if changed and leader_id is not None:
            for listener in self._leader_listeners:
                listener(leader_id)

    # --- handler registry --- #
    def register_handler(self, msg_type: type[M],
                         handler: Callable[[M], Awaitable[None]]) -> None:
        if not inspect.iscoroutinefunction(handler):
            raise TypeError(f"handler for {msg_type.__name__} must be async")
        if msg_type in self._handlers:
            raise ValueError(f"handler for {msg_type.__name__} already registered")
        self._handlers[msg_type] = handler
        log.debug("registered handler for %s", msg_type.__name__)

    async def dispatch(self, msg: object) -> None:
        handler = self._handlers.get(type(msg))
        if handler is None:
            log.warning("no handler for %s", type(msg).__name__)
            return
        try:
            await handler(msg)
        except asyncio.CancelledError:
            raise
        except Exception:
            # One bad message must not kill the connection loop.
            log.exception("handler for %s failed", type(msg).__name__)
