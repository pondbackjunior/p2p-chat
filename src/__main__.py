import argparse
import asyncio
import logging
import sys

from .config import load_node
from .election import Election
from .log_setup import setup_logging
from .history import HistoryStore
from .replication import Replication
from .transport import Transport


async def run(node, history: HistoryStore):
    log = logging.getLogger("main")
    transport = Transport(node)
    election = Election(node)
    replication = Replication(node, history=history)

    election.register()
    replication.register()
    replication.restore()
    await transport.start()
    await election.start()
    log.info("started; type a message and press Enter")

    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if not line:            # EOF (Ctrl-D)
            break
        text = line.strip()
        if text:
            await replication.submit(text)
    await election.stop()
    await transport.stop()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="cluster.toml")
    p.add_argument("--id", type=int, required=True)
    p.add_argument("--quiet", action="store_true", help="Only show warnings and errors in the terminal")
    p.add_argument("--history-dir", default="history",
                   help="directory for the chat history files, one per node (default: history)")
    p.add_argument("--fresh", action="store_true",
                   help="delete this node's saved history and start with an empty one")
    args = p.parse_args()

    node = load_node(args.config, args.id)
    setup_logging(node.node_id, quiet=args.quiet)
    history = HistoryStore.for_node(args.history_dir, node.node_id, fresh=args.fresh)
    try:
        asyncio.run(run(node, history))
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
