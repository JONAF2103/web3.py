import logging

from typing import List, Dict, Any

logger = logging.getLogger(__name__)

GET_LOGS_BATCH_SIZE: int = 300000  # 300000 blocks

GET_LOGS_MAX_RETRIES: int = 5


class EthLogsManager:

    def __init__(self, web3):
        self.web3 = web3

    @staticmethod
    def get_next_batch_end(current_batch_start: int,
                           final_block_number: int,
                           batch_size: int = GET_LOGS_BATCH_SIZE) -> int:
        current_batch_end = current_batch_start + batch_size
        return min(current_batch_end, final_block_number)

    def fetch_logs(self, filter_params: Dict,
                   current_batch_start: int,
                   current_batch_end: int,
                   retries: int) -> List[dict]:
        for currentTry in range(retries):
            try:
                logger.debug(f'Trying to get logs with params {filter_params} '
                             f'from block {current_batch_start} to block {current_batch_end}. '
                             f'(attempt {currentTry + 1})')
                return self.web3.manager.request_blocking(
                    "eth_getLogs", [filter_params],
                )
            except ValueError as e:
                logger.warning(f'Error trying to get logs with params {filter_params} '
                               f'from block {current_batch_start} to block {current_batch_end}', e)
        raise ValueError(f'Error trying to get logs with params {filter_params} '
                         f'from block {current_batch_start} to block {current_batch_end}')

    def get_logs(self, filter_params: Dict, earliest_block_number: int, latest_block_number: int) -> List[dict]:
        from_block = EthLogsManager.block_identifier_to_number(
            filter_params.get("fromBlock", earliest_block_number),
            earliest_block_number,
            latest_block_number)
        to_block = EthLogsManager.block_identifier_to_number(
            filter_params.get("toBlock", latest_block_number),
            earliest_block_number,
            latest_block_number)
        batch_size = filter_params.get("batchSize", GET_LOGS_BATCH_SIZE)
        retries = filter_params.get("retries", GET_LOGS_MAX_RETRIES)

        # remove superflous keys for eventual manager request call
        filter_params.pop("batchSize", None)
        filter_params.pop("retries", None)

        current_batch_start: int = int(from_block)
        current_batch_end: int = EthLogsManager.get_next_batch_end(current_batch_start, to_block, batch_size)
        logs = []
        while current_batch_start <= current_batch_end:
            filter_params["fromBlock"] = current_batch_start
            filter_params["toBlock"] = current_batch_end
            logs_batch = self.fetch_logs(filter_params, current_batch_start, current_batch_end, retries)
            for log in logs_batch:
                logs.append(log)
            current_batch_start = current_batch_end + 1
            current_batch_end = EthLogsManager.get_next_batch_end(current_batch_start, to_block, batch_size)
        return logs

    @staticmethod
    def block_identifier_to_number(block_identifier: Any, earliest_block_number: int, latest_block_number: int) -> int:
        if str(block_identifier) == "latest":
            return latest_block_number
        elif str(block_identifier) == "earliest":
            return earliest_block_number
        else:
            return block_identifier
