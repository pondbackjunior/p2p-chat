"""Leader-based message ordering.

Typed messages go to the leader, which numbers them and sends them to all
nodes. Each node shows messages in number order, holding back any that
arrive early. Unconfirmed messages are resent to a new leader, and
duplicates are dropped by msg_id.
"""
import asyncio
import logging
import uuid

from .messages import OrderedMessage, Submit
from .node import Node

log = logging.getLogger(__name__)


class Replication:
    def __init__(self, node: Node):
        self.node = node
        self._unconfirmed: dict[str, Submit] = {}         # own messages not yet seen back
        self._numbered: dict[str, OrderedMessage] = {}    # msg_id -> first numbering seen
        self._shown: set[str] = set()                     # msg_ids already printed
        self._tasks: set[asyncio.Task] = set()            # keeps background tasks alive

    # --- wiring --- #
    def register(self) -> None:
        """Register handlers on the node. Call before transport.start()."""
        self.node.register_handler(Submit, self._on_submit)
        self.node.register_handler(OrderedMessage, self._on_ordered)
        self.node.add_leader_listener(self._on_new_leader)

    # --- sending side --- #
    async def submit(self, text: str) -> None:
        """Called when the user types a message."""
        msg = Submit(sender_id=self.node.node_id, msg_id=uuid.uuid4().hex, text=text)
        self._unconfirmed[msg.msg_id] = msg
        await self._send_to_leader(msg)

    async def _send_to_leader(self, msg: Submit) -> None:
        leader = self.node.leader_id
        if leader is None:
            log.info("no leader yet, holding message %s", msg.msg_id)
        elif leader == self.node.node_id:
            await self._sequence(msg)   # we are the leader: no network needed
        elif not await self.node.send(leader, msg):
            log.warning("could not reach leader %s, will resend %s later", leader, msg.msg_id)

    def _on_new_leader(self, leader_id: int) -> None:
        """Resend everything still unconfirmed to the new leader."""
        if self._unconfirmed:
            log.info("new leader %s, resending %d message(s)", leader_id, len(self._unconfirmed))
            task = asyncio.create_task(self._resend_unconfirmed())
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

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
        existing = self._numbered.get(msg.msg_id)
        if existing is not None:
            log.info("duplicate Submit %s, resending seq %s", msg.msg_id, existing.seq)
            if msg.sender_id != self.node.node_id:
                await self.node.send(msg.sender_id, existing)
            return

        seq = self._next_free_seq()
        self.node.next_seq = seq + 1
        ordered = OrderedMessage(seq=seq, msg_id=msg.msg_id,
                                 sender_id=msg.sender_id, text=msg.text)
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

    # --- every node: in-order delivery --- #
    async def _on_ordered(self, msg: OrderedMessage) -> None:
        self._accept(msg)

    def _accept(self, msg: OrderedMessage) -> None:
        """Store an ordered message and deliver everything now in order."""
        if msg.seq < len(self.node.log) or msg.seq in self.node.pending:
            return
        self._numbered.setdefault(msg.msg_id, msg)
        self.node.pending[msg.seq] = msg
        if msg.seq > len(self.node.log):
            log.info("holding seq %s, waiting for seq %s", msg.seq, len(self.node.log))

        # log[i] always holds seq i, so len(log) is the next seq we expect.
        while len(self.node.log) in self.node.pending:
            m = self.node.pending.pop(len(self.node.log))
            self.node.log.append(m)
            self._deliver(m)

    def _deliver(self, m: OrderedMessage) -> None:
        self._unconfirmed.pop(m.msg_id, None)
        if m.msg_id in self._shown:
            log.info("seq %s repeats message %s, not showing it again", m.seq, m.msg_id)
            return
        self._shown.add(m.msg_id)
        log.info("delivered seq %s from node %s", m.seq, m.sender_id)
        print(f"[{m.seq}] node {m.sender_id}: {m.text}", flush=True)
