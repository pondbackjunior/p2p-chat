import argparse
import asyncio
import sys
import logging

from .config import load_node
from .log_setup import setup_logging
from .messages import Submit
from .transport import Transport

async def run(node):
    log = logging.getLogger("main")
    transport = Transport(node)

    async def on_submit(msg: Submit):
        print(f"[{msg.sender_id}] {msg.text}", flush=True)

    node.register_handler(Submit, on_submit)
    await transport.start()
    log.info("started; type a message and press Enter")

    loop = asyncio.get_running_loop()
    while True:
        line = await loop.run_in_executor(None, sys.stdin.readline)
        if not line:            # EOF (Ctrl-D)
            break
        text = line.strip()
        if text:
            await transport.broadcast(Submit(sender_id=node.node_id, text=text))
    await transport.stop()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="cluster.toml")
    p.add_argument("--id", type=int, required=True)
    args = p.parse_args()

    node = load_node(args.config, args.id)
    setup_logging(node.node_id)
    try:
        asyncio.run(run(node))
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()