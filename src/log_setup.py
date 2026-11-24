import logging
from pathlib import Path

class NodeIdFilter(logging.Filter):
    """Adds the node ID to every log record."""

    def __init__(self, node_id: int):
        super().__init__()
        self.node_id = node_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.node_id = self.node_id
        return True


def setup_logging(node_id: int, level=logging.INFO, quiet: bool = False) -> None:
    """Configure console and per-node file logging."""

    # Create the logs directory in the project root
    log_dir = Path(__file__).resolve().parent.parent / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    # Configure the root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # Remove existing handlers to prevent duplicate log messages
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
        handler.close()

    # Use the same format for both console and file logging
    formatter = logging.Formatter(
        "[node %(node_id)s] %(asctime)s %(name)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    node_filter = NodeIdFilter(node_id)

    # Console handler
    console_handler = logging.StreamHandler()

    if quiet:
        console_handler.setLevel(logging.WARNING)
    else:
        console_handler.setLevel(level)

    console_handler.setFormatter(formatter)
    console_handler.addFilter(node_filter)

    # File handler: records all messages, including DEBUG
    file_handler = logging.FileHandler(
        log_dir / f"node-{node_id}.log",
        mode="a",
        encoding="utf-8",
    )

    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    file_handler.addFilter(node_filter)

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
