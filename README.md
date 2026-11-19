# p2p-chat
## Running

Edit `cluster.toml` to change ports, then in three terminals:

    python -m src --id 1
    python -m src --id 2
    python -m src --id 3

If config is not in `cluster.toml`, use `--config <filename>` to manually specify it.

Type a line in any terminal and press Enter. It appears on the other two.