import logging

def setup_logging(node_id: int, level=logging.INFO):
    old_factory = logging.getLogRecordFactory()
    def factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.node_id = node_id
        return record
    logging.setLogRecordFactory(factory)
    logging.basicConfig(
        level=level,
        format="[node %(node_id)s] %(asctime)s %(name)s %(levelname)s: %(message)s",
    )