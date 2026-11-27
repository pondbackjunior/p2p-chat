# p2p-chat
## Running

Edit `cluster.toml` to change ports, then in three terminals:

    python -m src --id 1
    python -m src --id 2
    python -m src --id 3

If config is not in `cluster.toml`, use `--config <filename>` to manually specify it.

Type a line in any terminal and press Enter. It appears on the other two.

### Additional arguments

- `--quiet` – Reduces terminal output by hiding INFO and DEBUG log messages. Warnings and errors are still displayed. All log messages are still saved to the node's log file.

- `--fresh` – Deletes the current node's saved chat history before starting, allowing it to start with an empty local history. The node can then synchronize with the other nodes.