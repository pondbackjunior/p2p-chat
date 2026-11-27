"""Leader-based message ordering and catch-up.

Typed messages go to the leader, which numbers them and sends them to all
nodes. Each node shows messages in number order, holding back any that
arrive early. Unconfirmed messages are resent to a new leader, and
duplicates are dropped by msg_id.

Catch-up: a new leader asks every node how far its log goes (StateRequest)
and fetches what it is missing (SyncRequest) before numbering anything new.
Other nodes ask the leader for missing messages when they learn of a new
leader or notice a gap.

Terms: Node.dispatch() drops messages from older terms before they get here.
When the term goes up, held-back messages from the old leader are discarded
too. Copies sent again later are stamped with the current term.
"""
import asyncio
import logging
import time
import uuid
from collections.abc import Coroutine
from dataclasses import replace
from typing import Any

from .messages import (
    OrderedMessage,
    State,
    StateRequest,
    Submit,
    SyncRequest,
    SyncResponse,
)
from .node import Node

log = logging.getLogger(__name__)


class Replication:
    def __init__(self, node: Node,
                 state_timeout: float = 1.0,
                 sync_timeout: float = 2.0,
                 gap_retry: float = 2.0):
        self.node = node
        self.state_timeout = state_timeout   # how long a new leader waits for State replies
        self.sync_timeout = sync_timeout     # how long a new leader waits for missing messages
        self.gap_retry = gap_retry           # min seconds between identical gap requests

        self._unconfirmed: dict[str, Submit] = {}         # own messages not yet seen back
        self._numbered: dict[str, OrderedMessage] = {}    # msg_id -> first numbering seen
        self._shown: set[str] = set()                     # msg_ids already printed
        self._tasks: set[asyncio.Task[None]] = set()      # keeps background tasks alive

        # Catch-up state
        self._ready = asyncio.Event()        # cleared while a new leader catches up
        self._ready.set()
        self._waiting: list[Submit] = []     # submits that arrived while catching up
        self._catch_up_task: asyncio.Task[None] | None = None
        self._states: dict[int, int] = {}    # node id -> last seq, from State replies
        self._expected_states = 0
        self._states_in = asyncio.Event()    # set when every State reply has arrived
        self._sync_in = asyncio.Event()      # set when a SyncResponse arrives
        self._last_gap_request = (-1, 0.0)   # (from_seq, time) of the last request

    # --- wiring --- #
    def register(self) -> None:
        """Register handlers on the node. Call before transport.start()."""
        self.node.register_handler(Submit, self._on_submit)
        self.node.register_handler(OrderedMessage, self._on_ordered)
        self.node.register_handler(StateRequest, self._on_state_request)
        self.node.register_handler(State, self._on_state)
        self.node.register_handler(SyncRequest, self._on_sync_request)
        self.node.register_handler(SyncResponse, self._on_sync_response)
        self.node.add_leader_listener(self._on_new_leader)
        self.node.add_term_listener(self._on_new_term)

    def _spawn(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
        """Run coro in the background, keeping a reference so it isn't lost."""
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    # --- sending side --- #
    async def submit(self, text: str) -> None:
        """Called when the user types a message."""
        msg = Submit(sender_id=self.node.node_id, term=self.node.term,
                     msg_id=uuid.uuid4().hex, text=text)
        self._unconfirmed[msg.msg_id] = msg
        await self._send_to_leader(msg)

    async def _send_to_leader(self, msg: Submit) -> None:
        leader = self.node.leader_id
        if leader is None:
            log.info("no leader yet, holding message %s", msg.msg_id)
        elif leader == self.node.node_id:
            await self._sequence(msg)   # we are the leader: no network needed
        # The term may have moved on since the message was typed.
        elif not await self.node.send(leader, replace(msg, term=self.node.term)):
            log.warning("could not reach leader %s, will resend %s later", leader, msg.msg_id)

    def _on_new_term(self) -> None:
        """Discard held-back messages: the old leader's numbering may be reused.

        If the new leader has them, we get them again when we sync with it.
        """
        for m in self.node.pending.values():
            self._numbered.pop(m.msg_id, None)
        self.node.pending.clear()

    def _on_new_leader(self, leader_id: int) -> None:
        """Catch up with the new leader and resend anything unconfirmed."""
        if leader_id == self.node.node_id:
            self._ready.clear()   # hold submits until we have caught up
            if self._catch_up_task:
                self._catch_up_task.cancel()
            self._catch_up_task = self._spawn(self._catch_up_as_leader())
        else:
            self._ask_leader_for_missing(leader_id)

        if self._unconfirmed:
            log.info("new leader %s, resending %d message(s)", leader_id, len(self._unconfirmed))
            self._spawn(self._resend_unconfirmed())

    async def _resend_unconfirmed(self) -> None:
        for msg in list(self._unconfirmed.values()):
            await self._send_to_leader(msg)

    # --- leader side --- #
    async def _on_submit(self, msg: Submit) -> None:
        if not self.node.is_leader:
            # The sender will resend when it learns who the leader is.
            log.warning("got Submit %s but I am not the leader, ignoring", msg.msg_id)
            return
        await self._sequence(msg)

    async def _sequence(self, msg: Submit) -> None:
        """Leader only: number a message and send it to everyone."""
        if not self._ready.is_set():
            log.info("still catching up, holding Submit %s", msg.msg_id)
            self._waiting.append(msg)
            return

        existing = self._numbered.get(msg.msg_id)
        if existing is not None:
            log.info("duplicate Submit %s, resending seq %s", msg.msg_id, existing.seq)
            if msg.sender_id != self.node.node_id:
                await self.node.send(msg.sender_id,
                                     replace(existing, term=self.node.term))
            return

        seq = self._next_free_seq()
        self.node.next_seq = seq + 1
        ordered = OrderedMessage(seq=seq, msg_id=msg.msg_id, sender_id=msg.sender_id,
                                 text=msg.text, term=self.node.term)
        log.info("assigned seq %s to %s from node %s", seq, msg.msg_id, msg.sender_id)

        self._accept(ordered)   # deliver locally first
        await asyncio.gather(*(self.node.send(pid, ordered) for pid in self.node.peers))

    def _next_free_seq(self) -> int:
        """First number not used by us or by anything we have received.

        A node that just became leader still has an old next_seq, so it
        must skip past every message it has already seen.
        """
        highest_held = max(self.node.pending, default=-1)
        return max(self.node.next_seq, len(self.node.log), highest_held + 1)

    # --- leader side: catching up before numbering --- #
    async def _catch_up_as_leader(self) -> None:
        """Fetch any messages other nodes have that we lack, then start numbering."""
        try:
            best_id, best_last = await self._collect_states()
            own_last = len(self.node.log) - 1
            if best_last > own_last:
                log.info("missing seq %s..%s, fetching from node %s",
                         own_last + 1, best_last, best_id)
                self._sync_in.clear()
                request = SyncRequest(sender_id=self.node.node_id, term=self.node.term,
                                      from_seq=own_last + 1)
                if await self.node.send(best_id, request):
                    await _wait(self._sync_in, self.sync_timeout)
            log.info("caught up at seq %s, ready to number messages", len(self.node.log) - 1)
        finally:
            # A cancelled catch-up must not release submits held by its replacement.
            if asyncio.current_task() is self._catch_up_task:
                self._ready.set()
                waiting, self._waiting = self._waiting, []
                for msg in waiting:
                    if self.node.is_leader:   # if we lost leadership, senders resend anyway
                        await self._sequence(msg)

    async def _collect_states(self) -> tuple[int, int]:
        """Ask every peer how far its log goes. Returns (node id, last seq) of the furthest."""
        self._states.clear()
        self._states_in.clear()
        request = StateRequest(sender_id=self.node.node_id, term=self.node.term)
        sent = await asyncio.gather(*(self.node.send(pid, request) for pid in self.node.peers))
        self._expected_states = sum(sent)   # only count peers we could reach
        if len(self._states) < self._expected_states:
            await _wait(self._states_in, self.state_timeout)

        best_id, best_last = self.node.node_id, len(self.node.log) - 1
        for pid, last in self._states.items():
            if last > best_last:
                best_id, best_last = pid, last
        return best_id, best_last

    async def _on_state(self, msg: State) -> None:
        self._states[msg.sender_id] = msg.last_seq
        if 0 < self._expected_states <= len(self._states):
            self._states_in.set()

    # --- every node: answering catch-up requests --- #
    async def _on_state_request(self, msg: StateRequest) -> None:
        reply = State(sender_id=self.node.node_id, term=self.node.term,
                      last_seq=len(self.node.log) - 1)
        await self.node.send(msg.sender_id, reply)

    async def _on_sync_request(self, msg: SyncRequest) -> None:
        # Answer in the background: a new leader only answers once it has caught
        # up, and waiting here would stop this connection from reading.
        self._spawn(self._answer_sync(msg))

    async def _answer_sync(self, msg: SyncRequest) -> None:
        await self._ready.wait()
        missing = self.node.log[msg.from_seq:]
        log.info("sending %d message(s) from seq %s to node %s",
                 len(missing), msg.from_seq, msg.sender_id)
        await self.node.send(msg.sender_id,
                             SyncResponse(sender_id=self.node.node_id, term=self.node.term,
                                          messages=missing))

    async def _on_sync_response(self, msg: SyncResponse) -> None:
        log.info("received %d message(s) from node %s", len(msg.messages), msg.sender_id)
        for m in msg.messages:
            self._accept(m)
        self._sync_in.set()

    def _ask_leader_for_missing(self, leader_id: int) -> None:
        """Ask the leader for every message after the last one we have."""
        from_seq = len(self.node.log)
        self._last_gap_request = (from_seq, time.monotonic())
        request = SyncRequest(sender_id=self.node.node_id, term=self.node.term,
                              from_seq=from_seq)
        self._spawn(self._send_ignoring_result(leader_id, request))

    async def _send_ignoring_result(self, peer_id: int, msg: object) -> None:
        await self.node.send(peer_id, msg)

    # --- every node: in-order delivery --- #
    async def _on_ordered(self, msg: OrderedMessage) -> None:
        self._accept(msg)

    def _accept(self, msg: OrderedMessage) -> None:
        """Store an ordered message and deliver everything now in order."""
        if msg.seq < len(self.node.log) or msg.seq in self.node.pending:
            log.debug("ignoring seq %s, already have it", msg.seq)
            return
        self._numbered.setdefault(msg.msg_id, msg)
        self.node.pending[msg.seq] = msg

        # log[i] always holds seq i, so len(log) is the next seq we expect.
        while len(self.node.log) in self.node.pending:
            m = self.node.pending.pop(len(self.node.log))
            self.node.log.append(m)
            self._deliver(m)

        if self.node.pending:   # something is still waiting: there is a gap
            self._on_gap()

    def _on_gap(self) -> None:
        """Ask the leader for the missing messages, unless we just did."""
        leader = self.node.leader_id
        if leader is None or leader == self.node.node_id:
            return
        last_from, last_time = self._last_gap_request
        if last_from == len(self.node.log) and time.monotonic() - last_time < self.gap_retry:
            return
        log.info("missing seq %s, asking leader %s", len(self.node.log), leader)
        self._ask_leader_for_missing(leader)

    def _deliver(self, m: OrderedMessage) -> None:
        self._unconfirmed.pop(m.msg_id, None)
        if m.msg_id in self._shown:
            log.info("seq %s repeats message %s, not showing it again", m.seq, m.msg_id)
            return
        self._shown.add(m.msg_id)
        log.info("delivered seq %s from node %s", m.seq, m.sender_id)
        print(f"[{m.seq}] node {m.sender_id}: {m.text}", flush=True)


async def _wait(event: asyncio.Event, timeout: float) -> bool:
    """Wait for event, at most timeout seconds. Returns True if it was set."""
    try:
        await asyncio.wait_for(event.wait(), timeout)
        return True
    except TimeoutError:
        return False
