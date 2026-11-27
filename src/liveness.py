"""Tracks which nodes are alive. Only the leader uses this.

A node is considered failed when it has not replied to a heartbeat for
`timeout` seconds, and back again as soon as it replies.
"""
import logging
import time
from collections.abc import Iterable

log = logging.getLogger(__name__)


class Liveness:
    def __init__(self, peer_ids: Iterable[int], timeout: float = 3.0):
        self.timeout = timeout
        self._peer_ids = list(peer_ids)
        self._last_reply: dict[int, float] = {}   # node id -> time of its last reply
        self.failed: set[int] = set()             # nodes currently considered dead
        self._last_check = 0.0                    # time of the previous check()
        self.reset()

    def reset(self) -> None:
        """Start fresh, giving every node a full timeout to reply.

        Called when this node becomes leader: replies it saw during an
        earlier term say nothing about the present.
        """
        now = time.monotonic()
        self._last_reply = {pid: now for pid in self._peer_ids}
        self._last_check = now
        self.failed.clear()

    def record_reply(self, peer_id: int) -> None:
        """A heartbeat reply arrived from peer_id."""
        self._last_reply[peer_id] = time.monotonic()
        if peer_id in self.failed:
            self.failed.remove(peer_id)
            log.info("node %s is back", peer_id)

    def check(self) -> None:
        """Mark (and log once) every node that has been silent for too long."""
        now = time.monotonic()
        stalled = now - self._last_check > self.timeout
        self._last_check = now
        if stalled:
            # We were paused ourselves, so the silence says nothing about the
            # others. Give them a full timeout again.
            self._last_reply = {pid: now for pid in self._peer_ids}
            return
        for pid, last in self._last_reply.items():
            if pid not in self.failed and now - last > self.timeout:
                self.failed.add(pid)
                log.warning("node %s failed (no heartbeat reply for %.1fs)",
                            pid, now - last)
