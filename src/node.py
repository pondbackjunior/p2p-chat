import asyncio
import inspect
import logging
from typing import Awaitable, Callable, NamedTuple, Optional

log = logging.getLogger(__name__)

Handler = Callable[[object], Awaitable[None]]


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

        # Election state (used by election.py)
        self.leader_id: Optional[int] = None

        # Replication state (used by replication.py)
        self.next_seq: int = 0            # leader only: next sequence number to assign
        self.pending: dict[int, object] = {}   # seq -> message awaiting commit
        self.log: list[object] = []       # committed messages, in order

        # Wiring
        self._handlers: dict[type, Handler] = {}
        # Set by transport.py. Signature: async send(peer_id, msg)
        self.send: Optional[Callable[[int, object], Awaitable[None]]] = None

    # --- convenience -----------------------------------------------------
    @property
    def is_leader(self) -> bool:
        return self.leader_id == self.node_id

    def other_ids(self) -> list[int]:
        return sorted(self.peers)

    def higher_ids(self) -> list[int]:
        """Peers with a higher ID than ours (the ones Bully contacts)."""
        return [pid for pid in self.other_ids() if pid > self.node_id]

    # --- handler registry ------------------------------------------------
    def register_handler(self, msg_type: type, handler: Handler) -> None:
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