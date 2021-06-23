import logging

from typing import List, Dict, Any

from web3.logs_entry import EthLogsCacheEntry

logger = logging.getLogger(__name__)

GET_LOGS_BATCH_SIZE: int = 300000  # 300000 blocks

GET_LOGS_MAX_RETRIES: int = 5

CACHE_TIMEOUT: int = 60000  # in ms

CONFIRMATION_BLOCKS: int = 50  # 50 blocks


class EthLogsManager:

    def __init__(self, web3):
        self.logs_cache: Dict[int, EthLogsCacheEntry] = {}
        self.web3 = web3

    @staticmethod
    def get_next_batch_end(current_batch_start: int,
                           final_block_number: int,
                           batch_size: int = GET_LOGS_BATCH_SIZE) -> int:
        current_batch_end = current_batch_start + batch_size
        return min(current_batch_end, final_block_number)

    def get_logs(self, filter_params, earliest_block_number: int, latest_block_number: int) -> List[dict]:
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
        while current_batch_start < current_batch_end:
            filter_params["fromBlock"] = current_batch_start
            filter_params["toBlock"] = current_batch_end
            log_list = None
            for currentTry in range(retries):
                try:
                    logger.debug(f'Trying to get logs with params {filter_params} '
                                 f'from block {current_batch_start} to block {current_batch_end}. '
                                 f'(attempt {currentTry + 1})')
                    log_list = self.web3.manager.request_blocking(
                        "eth_getLogs", [filter_params],
                    )
                    break
                except ValueError as e:
                    logger.warning(f'Error trying to get logs with params {filter_params} '
                                   f'from block {current_batch_start} to block {current_batch_end}', e)
            if log_list is None:
                raise ValueError(f'Error trying to get logs with params {filter_params} '
                                 f'from block {current_batch_start} to block {current_batch_end}')
            else:
                for log in log_list:
                    logs.append(log)
                current_batch_start = current_batch_end + 1
                current_batch_end = EthLogsManager.get_next_batch_end(current_batch_start, to_block, batch_size)

        return logs

    @staticmethod
    def generate_cache_key(address: str, topics: List[str]) -> int:
        return hash(f"logs_{address}_{topics.sort()}")

    @staticmethod
    def block_identifier_to_number(block_identifier: Any, earliest_block_number: int, latest_block_number: int) -> int:
        if str(block_identifier) == "latest":
            return latest_block_number
        elif  str(block_identifier) == "earliest":
            return earliest_block_number
        else:
            return block_identifier

    def retrieve_logs(self, filter_params: Dict, earliest_block_number: int, latest_block_number: int) -> List[dict]:
        from_block = filter_params.get("fromBlock", earliest_block_number)
        to_block = filter_params.get("toBlock", latest_block_number)
        address = filter_params.get("address", "")
        topics = filter_params.get("topics", [])
        invalidate_cache = filter_params.get("invalidate_cache", False)

        # remove superflous keys for eventual manager request call
        filter_params.pop("invalidate_cache", None)

        # check the cache before populating with data
        cache_key: int = self.generate_cache_key(address, topics)
        logger.debug(f"getting logs with cache key {cache_key}")

        if cache_key in self.logs_cache and not invalidate_cache:
            logger.debug(f"cache exists and is not being invalidated")
            # here we have all the logs from earliest to latest on the last iteration
            # we need to check if the current iteration doesn't need an update from
            # latest of last iteration to current block
            cached_value: EthLogsCacheEntry = self.logs_cache[cache_key]
            # we need to leave at least a number of confirmation blocks for reorgs
            cache_to_block: int = latest_block_number - CONFIRMATION_BLOCKS
            if cached_value.to_block < cache_to_block:
                logger.debug(f"cache needs update from {cached_value.to_block} to {cache_to_block}")
                # we need to update this cache from the current to_block to the new cache_to_block
                filter_params["fromBlock"] = cached_value.to_block
                filter_params["toBlock"] = cache_to_block
                logs = self.get_logs(filter_params)
                cached_value.logs.extend(logs)
                cached_value.to_block = cache_to_block
                logger.debug(f"updated cache")
            return cached_value.get_logs(from_block, to_block)

        else:
            logger.debug(f"cache doesn't exists or is being invalidated")
            # retrieve all logs from earliest to latest to populate cache
            filter_params["fromBlock"] = earliest_block_number
            filter_params["toBlock"] = latest_block_number
            logs = self.get_logs(filter_params)
            logger.debug(f"generating and saving cached value logs {logs}")
            cached_value = EthLogsCacheEntry(address, topics, earliest_block_number, latest_block_number, logs)
            self.logs_cache[cache_key] = cached_value
            return cached_value.get_logs(from_block, to_block)
