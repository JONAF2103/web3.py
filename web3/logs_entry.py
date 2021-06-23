import logging

from typing import List

logger = logging.getLogger(__name__)


class EthLogsCacheEntry:

    def __init__(self,
                 address: str,
                 topics: List[str],
                 from_block: int,
                 to_block: int,
                 logs: List[dict]):
        self.address = address
        self.topics = topics
        self.from_block = from_block
        self.to_block = to_block
        self.logs = logs

    def get_logs(self, from_block: int, to_block: int):
        logs = list()
        for log in self.logs:
            # this is needed since rskj sometimes provide events without blockNumber
            # (that's something that sometimes happens but not always, seems to be a bug on the core)
            log_block_number: int = log['blockNumber'] if "blockNumber" in log else None
            if log_block_number and from_block <= log_block_number <= to_block:
                logs.append(log)
        logger.debug(f"Getting logs from cached value from block {from_block} to block {to_block} -> logs {logs}")
        return logs

