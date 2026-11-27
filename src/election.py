"""Leader election: Bully algorithm with heartbeat-based failure detection.

Rules
-----
* The node with the highest ID that is alive is the leader.
* The leader sends Heartbeat to everyone every `heartbeat_interval` seconds.
  Nodes answer with HeartbeatReply, and the leader uses the replies to track
  which nodes are alive (see liveness.py).
* A follower that hears nothing from the leader for `leader_timeout` seconds
  suspects it and starts an election.
* Election: send Election to all higher IDs.
    - no Answer within `answer_timeout`  -> I win, broadcast Coordinator
    - got an Answer -> a higher node is taking over; wait up to
      `coordinator_timeout` for its Coordinator, otherwise start over.
* A node that receives Election from a lower ID replies Answer and starts its
  own election. If it already is the leader, it replies Answer and Coordinator
  instead, so the term does not change.
* A restarted high-ID node takes leadership back when it hears a heartbeat
  from a lower leader.

Terms
-----
* A node that wins an election raises the term by one before announcing itself.
* Every message carries a term. Node.dispatch() adopts higher terms and
  ignores lower ones, answering StaleTerm to an outdated leader or candidate.
* A restarted node begins at term 0 and learns the current term this way.
* Two nodes can win at the same time with the same term. Followers then keep
  the higher ID, and that node starts a new election, which raises the term.
"""
import asyncio
import logging
import time

from .liveness import Liveness
from .messages import Answer, Coordinator, Heartbeat, HeartbeatReply, StaleTerm
from .messages import Election as ElectionMsg
from .node import Node

log = logging.getLogger(__name__)


class Election:
    def __init__(self, node: Node,
                 heartbeat_interval: float = 1.0,
                 leader_timeout: float = 3.0,
                 answer_timeout: float = 1.5,
                 coordinator_timeout: float = 3.0,
                 node_timeout: float = 3.0):
        self.node = node
        self.heartbeat_interval = heartbeat_interval
        self.leader_timeout = leader_timeout
        self.answer_timeout = answer_timeout
        self.coordinator_timeout = coordinator_timeout
        self.liveness = Liveness(node.peers, timeout=node_timeout)

        self._last_heartbeat = time.monotonic()
        self._answer_seen = asyncio.Event()
        self._coordinator_seen = asyncio.Event()
        self._election_task: asyncio.Task | None = None
        self._tasks: list[asyncio.Task] = []

    # --- wiring ----------------------------------------------------------
    def register(self) -> None:
        """Register message handlers on the node. Call before transport.start()."""
        self.node.register_handler(Heartbeat, self._on_heartbeat)
        self.node.register_handler(HeartbeatReply, self._on_heartbeat_reply)
        self.node.register_handler(StaleTerm, self._on_stale_term)
        self.node.register_handler(ElectionMsg, self._on_election)
        self.node.register_handler(Answer, self._on_answer)
        self.node.register_handler(Coordinator, self._on_coordinator)
        self.node.add_term_listener(self._on_new_term)

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
        """Leader only: tell everyone we are alive and check who has replied."""
        while True:
            if self.node.is_leader:
                self.liveness.check()
                await self._send_all(Heartbeat(sender_id=self.node.node_id,
                                               term=self.node.term))
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
        # Reply even if we will not accept this leader: we are still alive.
        await self.node.send(msg.sender_id,
                             HeartbeatReply(sender_id=me, term=self.node.term))
        if msg.sender_id < me:
            # A lower node thinks it leads, but I outrank it.
            log.info("heartbeat from lower node %s, starting election", msg.sender_id)
            self._begin_election()
            return
        if self._outranked_by_current_leader(msg.sender_id):
            return
        if self.node.leader_id != msg.sender_id:
            log.info("accepting node %s as leader (via heartbeat)", msg.sender_id)
            self.node.set_leader(msg.sender_id)
        self._last_heartbeat = time.monotonic()

    async def _on_heartbeat_reply(self, msg: HeartbeatReply) -> None:
        if self.node.is_leader:
            self.liveness.record_reply(msg.sender_id)

    async def _on_stale_term(self, msg: StaleTerm) -> None:
        # Node.dispatch() has already adopted the newer term. A running
        # election notices that by itself; otherwise start one.
        self._begin_election()

    async def _on_election(self, msg: ElectionMsg) -> None:
        me = self.node.node_id
        if msg.sender_id < me:
            log.info("election from node %s, answering", msg.sender_id)
            await self.node.send(msg.sender_id,
                                 Answer(sender_id=me, term=self.node.term))
            if self.node.is_leader:
                # No need for a new term, just repeat who leads this one.
                await self.node.send(msg.sender_id,
                                     Coordinator(sender_id=me, term=self.node.term))
            else:
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
        if self._outranked_by_current_leader(msg.sender_id):
            return
        log.info("node %s is the new leader (term %s)", msg.sender_id, msg.term)
        self.node.set_leader(msg.sender_id)
        self._last_heartbeat = time.monotonic()
        self._coordinator_seen.set()

    def _outranked_by_current_leader(self, claimant_id: int) -> bool:
        """True if we already follow a higher node than claimant_id in this term."""
        leader = self.node.leader_id
        return leader is not None and claimant_id < leader

    def _on_new_term(self) -> None:
        """Give the new term's leader a full timeout before suspecting it."""
        self._last_heartbeat = time.monotonic()

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
            term = self.node.term
            higher = self.node.higher_ids()
            log.info("starting election (higher nodes: %s)", higher)
            self._answer_seen.clear()
            self._coordinator_seen.clear()

            await asyncio.gather(*(
                self.node.send(pid, ElectionMsg(sender_id=me, term=term))
                for pid in higher
            ))

            if not await self._wait(self._answer_seen, self.answer_timeout):
                if self._coordinator_seen.is_set():
                    return
                if self.node.term != term:
                    # We were behind. Ask again using the current term.
                    continue
                await self._become_leader()
                return

            # A higher node is alive and will announce itself.
            if await self._wait(self._coordinator_seen, self.coordinator_timeout):
                return
            log.warning("no coordinator announced after answer, retrying election")

    async def _become_leader(self) -> None:
        self.node.advance_term(self.node.term + 1)
        self.liveness.reset()
        self.node.set_leader(self.node.node_id)
        log.info("became leader (term %s)", self.node.term)
        await self._send_all(Coordinator(sender_id=self.node.node_id,
                                         term=self.node.term))

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
