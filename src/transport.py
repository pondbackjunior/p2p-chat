import asyncio
import json
import logging
from .messages import encode, decode
from .node import Node, Address

log = logging.getLogger(__name__)


class Transport:
    def __init__(self, node: Node):
        self.node = node
        self._writers: dict[int, asyncio.StreamWriter] = {}
        self._tasks: list[asyncio.Task] = []
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        a = self.node.address
        self._server = await asyncio.start_server(self._on_inbound, a.host, a.port)
        log.info("listening on %s:%s", a.host, a.port)
        for pid, addr in self.node.peers.items():
            self._tasks.append(asyncio.create_task(self._dial_loop(pid, addr)))
        self.node.attach_sender(self.send)   # modules use node.send, never import transport

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        if self._server:
            self._server.close()
        for w in self._writers.values():
            w.close()

    # --- inbound: read-only --- #
    async def _on_inbound(self, reader, writer):
        peer = writer.get_extra_info("peername")
        log.info("inbound connection from %s", peer)
        try:
            while line := await reader.readline():
                try:
                    msg = decode(line)
                except (ValueError, TypeError) as e:   # JSONDecodeError is a ValueError
                    log.warning("bad message from %s: %s", peer, e)
                    continue
                await self.node.dispatch(msg)
        except ConnectionError:
            pass
        finally:
            writer.close()
            log.info("inbound connection from %s closed", peer)

    # --- outbound: write-only, with reconnect --- #
    async def _dial_loop(self, pid: int, addr: Address):
        delay = 0.5
        while True:
            try:
                reader, writer = await asyncio.open_connection(addr.host, addr.port)
            except OSError:
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5.0)
                continue
            delay = 0.5
            self._writers[pid] = writer
            log.info("connected to peer %s", pid)
            try:
                await reader.read()   # peer never writes back, so this returns only on close
            except ConnectionError:
                pass
            finally:
                self._writers.pop(pid, None)
                writer.close()
                log.warning("lost connection to peer %s, retrying", pid)

    # --- sending --- #
    async def send(self, peer_id: int, msg) -> bool:
        writer = self._writers.get(peer_id)
        if writer is None:
            log.warning("no connection to peer %s, dropping %s", peer_id, type(msg).__name__)
            return False
        try:
            writer.write(encode(msg))
            await writer.drain()
            return True
        except ConnectionError:
            log.warning("send to peer %s failed", peer_id)
            return False

    async def broadcast(self, msg) -> None:
        await asyncio.gather(*(self.send(pid, msg) for pid in self.node.peers))
