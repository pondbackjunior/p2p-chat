"""Leader election: Bully algorithm with heartbeat-based failure detection.

Rules
-----
* The node with the highest ID that is alive is the leader.
* The leader sends Heartbeat to everyone every `heartbeat_interval` seconds.
* A follower that hears nothing from the leader for `leader_timeout` seconds
  suspects it and starts an election.
* Election: send Election to all higher IDs.
    - no Answer within `answer_timeout`  -> I win, broadcast Coordinator
    - got an Answer -> a higher node is taking over; wait up to
      `coordinator_timeout` for its Coordinator, otherwise start over.
* A node that receives Election from a lower ID replies Answer and starts its
  own election (this is how a restarted high-ID node takes leadership back).
"""
import asyncio
import logging
import time

from .messages import Answer, Coordinator, Heartbeat
from .messages import Election as ElectionMsg
from .node import Node

log = logging.getLogger(__name__)


class Election:
    def __init__(self, node: Node,
                 heartbeat_interval: float = 1.0,
                 leader_timeout: float = 3.0,
                 answer_timeout: float = 1.5,
                 coordinator_timeout: float = 3.0):
        self.node = node
        self.heartbeat_interval = heartbeat_interval
        self.leader_timeout = leader_timeout
        self.answer_timeout = answer_timeout
        self.coordinator_timeout = coordinator_timeout

        self._last_heartbeat = time.monotonic()
        self._answer_seen = asyncio.Event()
        self._coordinator_seen = asyncio.Event()
        self._election_task: asyncio.Task | None = None
        self._tasks: list[asyncio.Task] = []

    # --- wiring ----------------------------------------------------------
    def register(self) -> None:
        """Register message handlers on the node. Call before transport.start()."""
        self.node.register_handler(Heartbeat, self._on_heartbeat)
        self.node.register_handler(ElectionMsg, self._on_election)
        self.node.register_handler(Answer, self._on_answer)
        self.node.register_handler(Coordinator, self._on_coordinator)

    async def start(self) -> None:
        self._last_heartbeat = time.monotonic()
        self._tasks = [
            asyncio.create_task(self._heartbeat_loop()),
            asyncio.create_task(self._monitor_loop()),
        ]

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        if self._election_task:
            self._election_task.cancel()

    # --- failure detection --- #
    async def _heartbeat_loop(self) -> None:
        """Leader only: tell everyone we are alive."""
        while True:
            if self.node.is_leader:
                await self._send_all(Heartbeat(sender_id=self.node.node_id))
            await asyncio.sleep(self.heartbeat_interval)

    async def _monitor_loop(self) -> None:
        """Follower only: start an election if the leader goes silent."""
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            if self.node.is_leader or self._electing:
                continue
            silent_for = time.monotonic() - self._last_heartbeat
            if silent_for > self.leader_timeout:
                log.warning("suspect leader %s (silent for %.1fs)",
                            self.node.leader_id, silent_for)
                self.node.set_leader(None)
                self._begin_election()

    # --- handlers --- #
    async def _on_heartbeat(self, msg: Heartbeat) -> None:
        me = self.node.node_id
        if msg.sender_id < me:
            # A lower node thinks it leads, but I outrank it.
            log.info("heartbeat from lower node %s, starting election", msg.sender_id)
            self._begin_election()
            return
        if self.node.leader_id != msg.sender_id:
            log.info("accepting node %s as leader (via heartbeat)", msg.sender_id)
            self.node.set_leader(msg.sender_id)
        self._last_heartbeat = time.monotonic()

    async def _on_election(self, msg: ElectionMsg) -> None:
        if msg.sender_id < self.node.node_id:
            log.info("election from node %s, answering", msg.sender_id)
            await self.node.send(msg.sender_id, Answer(sender_id=self.node.node_id))
            self._begin_election()

    async def _on_answer(self, msg: Answer) -> None:
        log.info("answer from node %s", msg.sender_id)
        self._answer_seen.set()

    async def _on_coordinator(self, msg: Coordinator) -> None:
        if msg.sender_id < self.node.node_id:
            # Bully: a lower node must not win while I am alive.
            log.info("coordinator claim from lower node %s, starting election",
                     msg.sender_id)
            self._begin_election()
            return
        log.info("node %s is the new leader", msg.sender_id)
        self.node.set_leader(msg.sender_id)
        self._last_heartbeat = time.monotonic()
        self._coordinator_seen.set()

    # --- election procedure --- #
    @property
    def _electing(self) -> bool:
        return self._election_task is not None and not self._election_task.done()

    def _begin_election(self) -> None:
        """Start an election unless one is already running."""
        if not self._electing:
            self._election_task = asyncio.create_task(self._run_election())

    async def _run_election(self) -> None:
        me = self.node.node_id
        while True:
            higher = self.node.higher_ids()
            log.info("starting election (higher nodes: %s)", higher)
            self._answer_seen.clear()
            self._coordinator_seen.clear()

            await asyncio.gather(*(
                self.node.send(pid, ElectionMsg(sender_id=me)) for pid in higher
            ))

            if not await self._wait(self._answer_seen, self.answer_timeout):
                await self._become_leader()
                return

            # A higher node is alive and will announce itself.
            if await self._wait(self._coordinator_seen, self.coordinator_timeout):
                return
            log.warning("no coordinator announced after answer, retrying election")

    async def _become_leader(self) -> None:
        self.node.set_leader(self.node.node_id)
        log.info("became leader")
        await self._send_all(Coordinator(sender_id=self.node.node_id))

    # --- helpers --- #
    @staticmethod
    async def _wait(event: asyncio.Event, timeout: float) -> bool:
        try:
            await asyncio.wait_for(event.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def _send_all(self, msg) -> None:
        await asyncio.gather(*(self.node.send(pid, msg) for pid in self.node.peers))
