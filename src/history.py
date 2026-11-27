"""Saving the chat history to disk: one JSON Lines file per node.

Every delivered message is appended to the file as one line, in the same JSON
format the nodes use on the wire. Messages are delivered in sequence order, so line i of the file 
holds the message with seq i, exactlylike node.log[i]. A restarted node loads the 
file into its log and then onlyhas to fetch the messages that came after the last saved one.
"""
import logging
import os
from pathlib import Path

from .messages import OrderedMessage, decode, encode

log = logging.getLogger(__name__)


class HistoryStore:
    def __init__(self, path: str | os.PathLike):
        self.path = Path(path)

    @classmethod
    def for_node(cls, directory: str | os.PathLike, node_id: int,
                 fresh: bool = False) -> "HistoryStore":
        """The store for one node: <directory>/node-<id>.jsonl.

        fresh=True deletes any saved history, so the node starts empty.
        """
        store = cls(Path(directory) / f"node-{node_id}.jsonl")
        if fresh:
            log.info("--fresh: discarding saved history in %s", store.path)
            store.clear()
        return store

    # --- writing --- #
    def append(self, msg: OrderedMessage) -> None:
        """Add one delivered message to the end of the file."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "a", encoding="utf-8", newline="\n") as f:
            f.write(_to_line(msg))
            f.flush()
            os.fsync(f.fileno())   # the line must be on disk before we go on

    def clear(self) -> None:
        """Delete the saved history."""
        self.path.unlink(missing_ok=True)

    # --- reading --- #
    def load(self) -> list[OrderedMessage]:
        """Return the saved messages, seq 0, 1, 2, ... without gaps.

        Reading stops at the first line that is damaged or out of order, and
        the file is rewritten without it.
        """
        try:
            raw = self.path.read_bytes()
        except FileNotFoundError:
            return []

        lines = raw.split(b"\n")
        if lines[-1] == b"":
            lines.pop()                      # normal case: the file ends with a newline
        damaged = bool(raw) and not raw.endswith(b"\n")   # last line was cut off

        messages: list[OrderedMessage] = []
        for number, line in enumerate(lines, start=1):
            msg = _from_line(line)
            if msg is None or msg.seq != len(messages):
                log.warning("%s: line %d is damaged or out of order, "
                            "keeping only the %d message(s) before it",
                            self.path, number, len(messages))
                damaged = True
                break
            messages.append(msg)

        if damaged:
            self._rewrite(messages)
        return messages

    def _rewrite(self, messages: list[OrderedMessage]) -> None:
        """Replace the file with exactly these messages (via a temp file)."""
        temp = self.path.with_name(self.path.name + ".tmp")
        with open(temp, "w", encoding="utf-8", newline="\n") as f:
            for msg in messages:
                f.write(_to_line(msg))
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp, self.path)


def _to_line(msg: OrderedMessage) -> str:
    return encode(msg).decode("utf-8")      # one JSON object plus "\n"


def _from_line(line: bytes) -> OrderedMessage | None:
    """Parse one line. None if it is not a valid OrderedMessage."""
    try:
        msg = decode(line)
    except (ValueError, TypeError, AttributeError):   # bad JSON, bad fields, not an object
        return None
    return msg if isinstance(msg, OrderedMessage) else None
