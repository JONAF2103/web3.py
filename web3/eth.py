import logging

from eth_account import (
    Account,
)
from eth_utils import (
    apply_to_return_value,
    is_checksum_address,
    is_string,
)
from hexbytes import (
    HexBytes,
)

from web3.contract import (
    Contract,
)
from web3.iban import (
    Iban,
)
from web3.module import (
    Module,
)
from web3.utils.blocks import (
    select_method_for_block_identifier,
)
from web3.utils.decorators import (
    deprecated_for,
)
from web3.utils.empty import (
    empty,
)
from web3.utils.encoding import (
    to_hex,
)
from web3.utils.filters import (
    BlockFilter,
    LogFilter,
    TransactionFilter,
)
from web3.utils.toolz import (
    assoc,
    merge,
)
from web3.utils.transactions import (
    assert_valid_transaction_params,
    extract_valid_transaction_params,
    get_buffered_gas_estimate,
    get_required_transaction,
    replace_transaction,
    wait_for_transaction_receipt,
)

from typing import Dict, List, Any

# 300000 blocks
GET_LOGS_BATCH_SIZE: int = 300000

GET_LOGS_MAX_RETRIES: int = 5

# cache timeout in ms
CACHE_TIMEOUT: int = 60000

# 50 blocks
CONFIRMATION_BLOCKS: int = 50

logger = logging.getLogger(__name__)


class EthGetLogsCachedValue:

    def __init__(self,
                 address: str,
                 topics: List[str],
                 from_block: int,
                 to_block: int,
                 logs: List[Dict]):
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


class EthGetLogsCache:

    logs: Dict[int, EthGetLogsCachedValue]

    def __init__(self, web3):
        self.logs = {}
        self.web3 = web3

    @staticmethod
    def get_next_batch_end(current_batch_start: int,
                           final_block_number: int,
                           batch_size: int = GET_LOGS_BATCH_SIZE) -> int:
        current_batch_end = current_batch_start + batch_size
        return min(current_batch_end, final_block_number)

    def get_logs(self, filter_params):
        from_block: int = filter_params["fromBlock"]
        to_block: int = filter_params["toBlock"]
        batch_size: int = filter_params["batchSize"] if "batchSize" in filter_params else GET_LOGS_BATCH_SIZE
        retries: int = filter_params["retries"] if "retries" in filter_params else GET_LOGS_MAX_RETRIES
        if "batchSize" in filter_params:
            del filter_params["batchSize"]
        if "retries" in filter_params:
            del filter_params["retries"]
        current_batch_start: int = int(from_block)
        current_batch_end: int = EthGetLogsCache.get_next_batch_end(current_batch_start, to_block, batch_size)
        logs = list()
        while current_batch_start < current_batch_end:
            filter_params["fromBlock"] = current_batch_start
            filter_params["toBlock"] = current_batch_end
            log_list = None
            for currentTry in range(1, (retries + 1)):
                try:
                    logger.debug(f'Trying to get logs with params {filter_params} '
                                 f'from block {current_batch_start} to block {current_batch_end}. '
                                 f'(attempt {currentTry})')
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
                current_batch_end = EthGetLogsCache.get_next_batch_end(current_batch_start, to_block, batch_size)

        return logs

    @staticmethod
    def generate_cache_key(address: str, topics: List[str]) -> int:
        return hash(f"logs_{address}_{topics}")

    def get(self, filter_params: Any, earliest_block_number: int, latest_block_number: int) -> List:
        from_block: int = filter_params["fromBlock"] if "fromBlock" in filter_params else earliest_block_number
        to_block: int = filter_params["toBlock"] if "toBlock" in filter_params else latest_block_number
        address: str = filter_params["address"] if "address" in filter_params else ""
        topics: List[str] = filter_params["topics"] if "topics" in filter_params else ""
        invalidate_cache: bool = filter_params["invalidate_cache"] if "invalidate_cache" in filter_params else False
        if "invalidate_cache" in filter_params:
            del filter_params["invalidate_cache"]
        # check the cache before populating with data
        cache_key: int = self.generate_cache_key(address, topics)
        logger.debug(f"getting logs with cache key {cache_key}")
        if cache_key in self.logs and not invalidate_cache:
            logger.debug(f"cache exists and is not being invalidated")
            # here we have all the logs from earliest to latest on the last iteration
            # we need to check if the current iteration doesn't need an update from
            # latest of last iteration to current block
            cached_value: EthGetLogsCachedValue = self.logs[cache_key]
            # we need to leave at least a number of confirmation blocks for reorgs
            cache_to_block: int = latest_block_number - CONFIRMATION_BLOCKS
            if cached_value.to_block < cache_to_block:
                logger.debug(f"cache needs update from {cached_value.to_block} to {cache_to_block}")
                # we need to update this cache from the current to_block to the new cache_to_block
                filter_params["fromBlock"] = cached_value.to_block
                filter_params["toBlock"] = cache_to_block
                logs = self.get_logs(filter_params)
                cached_value.logs.append(logs)
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
            cached_value = EthGetLogsCachedValue(address, topics, earliest_block_number, latest_block_number, logs)
            self.logs[cache_key] = cached_value
            return cached_value.get_logs(from_block, to_block)


class Eth(Module):
    account = Account()
    defaultAccount = empty
    defaultBlock = "latest"
    defaultContractFactory = Contract
    iban = Iban
    gasPriceStrategy = None
    logs_cache: EthGetLogsCache

    def __init__(self, web3):
        super().__init__(web3)
        self.logs_cache = EthGetLogsCache(web3)

    @deprecated_for("doing nothing at all")
    def enable_unaudited_features(self):
        pass

    def namereg(self):
        raise NotImplementedError()

    def icapNamereg(self):
        raise NotImplementedError()

    @property
    def protocolVersion(self):
        return self.web3.manager.request_blocking("eth_protocolVersion", [])

    @property
    def syncing(self):
        return self.web3.manager.request_blocking("eth_syncing", [])

    @property
    def coinbase(self):
        return self.web3.manager.request_blocking("eth_coinbase", [])

    @property
    def mining(self):
        return self.web3.manager.request_blocking("eth_mining", [])

    @property
    def hashrate(self):
        return self.web3.manager.request_blocking("eth_hashrate", [])

    @property
    def gasPrice(self):
        return self.web3.manager.request_blocking("eth_gasPrice", [])

    @property
    def accounts(self):
        return self.web3.manager.request_blocking("eth_accounts", [])

    @property
    def blockNumber(self):
        return self.web3.manager.request_blocking("eth_blockNumber", [])

    def getBalance(self, account, block_identifier=None):
        if block_identifier is None:
            block_identifier = self.defaultBlock
        return self.web3.manager.request_blocking(
            "eth_getBalance",
            [account, block_identifier],
        )

    def getStorageAt(self, account, position, block_identifier=None):
        if block_identifier is None:
            block_identifier = self.defaultBlock
        return self.web3.manager.request_blocking(
            "eth_getStorageAt",
            [account, position, block_identifier]
        )

    def getCode(self, account, block_identifier=None):
        if block_identifier is None:
            block_identifier = self.defaultBlock
        return self.web3.manager.request_blocking(
            "eth_getCode",
            [account, block_identifier],
        )

    def getBlock(self, block_identifier, full_transactions=False):
        """
        `eth_getBlockByHash`
        `eth_getBlockByNumber`
        """
        method = select_method_for_block_identifier(
            block_identifier,
            if_predefined='eth_getBlockByNumber',
            if_hash='eth_getBlockByHash',
            if_number='eth_getBlockByNumber',
        )

        return self.web3.manager.request_blocking(
            method,
            [block_identifier, full_transactions],
        )

    def getBlockTransactionCount(self, block_identifier):
        """
        `eth_getBlockTransactionCountByHash`
        `eth_getBlockTransactionCountByNumber`
        """
        method = select_method_for_block_identifier(
            block_identifier,
            if_predefined='eth_getBlockTransactionCountByNumber',
            if_hash='eth_getBlockTransactionCountByHash',
            if_number='eth_getBlockTransactionCountByNumber',
        )
        return self.web3.manager.request_blocking(
            method,
            [block_identifier],
        )

    def getUncleCount(self, block_identifier):
        """
        `eth_getUncleCountByBlockHash`
        `eth_getUncleCountByBlockNumber`
        """
        method = select_method_for_block_identifier(
            block_identifier,
            if_predefined='eth_getUncleCountByBlockNumber',
            if_hash='eth_getUncleCountByBlockHash',
            if_number='eth_getUncleCountByBlockNumber',
        )
        return self.web3.manager.request_blocking(
            method,
            [block_identifier],
        )

    def getUncleByBlock(self, block_identifier, uncle_index):
        """
        `eth_getUncleByBlockHashAndIndex`
        `eth_getUncleByBlockNumberAndIndex`
        """
        method = select_method_for_block_identifier(
            block_identifier,
            if_predefined='eth_getUncleByBlockNumberAndIndex',
            if_hash='eth_getUncleByBlockHashAndIndex',
            if_number='eth_getUncleByBlockNumberAndIndex',
        )
        return self.web3.manager.request_blocking(
            method,
            [block_identifier, uncle_index],
        )

    def getTransaction(self, transaction_hash):
        return self.web3.manager.request_blocking(
            "eth_getTransactionByHash",
            [transaction_hash],
        )

    @deprecated_for("w3.eth.getTransactionByBlock")
    def getTransactionFromBlock(self, block_identifier, transaction_index):
        """
        Alias for the method getTransactionByBlock
        Depreceated to maintain naming consistency with the json-rpc API
        """
        return self.getTransactionByBlock(block_identifier, transaction_index)

    def getTransactionByBlock(self, block_identifier, transaction_index):
        """
        `eth_getTransactionByBlockHashAndIndex`
        `eth_getTransactionByBlockNumberAndIndex`
        """
        method = select_method_for_block_identifier(
            block_identifier,
            if_predefined='eth_getTransactionByBlockNumberAndIndex',
            if_hash='eth_getTransactionByBlockHashAndIndex',
            if_number='eth_getTransactionByBlockNumberAndIndex',
        )
        return self.web3.manager.request_blocking(
            method,
            [block_identifier, transaction_index],
        )

    def waitForTransactionReceipt(self, transaction_hash, timeout=120):
        return wait_for_transaction_receipt(self.web3, transaction_hash, timeout)

    def getTransactionReceipt(self, transaction_hash):
        return self.web3.manager.request_blocking(
            "eth_getTransactionReceipt",
            [transaction_hash],
        )

    def getTransactionCount(self, account, block_identifier=None):
        if block_identifier is None:
            block_identifier = self.defaultBlock
        return self.web3.manager.request_blocking(
            "eth_getTransactionCount",
            [
                account,
                block_identifier,
            ],
        )

    def replaceTransaction(self, transaction_hash, new_transaction):
        current_transaction = get_required_transaction(self.web3, transaction_hash)
        return replace_transaction(self.web3, current_transaction, new_transaction)

    def modifyTransaction(self, transaction_hash, **transaction_params):
        assert_valid_transaction_params(transaction_params)
        current_transaction = get_required_transaction(self.web3, transaction_hash)
        current_transaction_params = extract_valid_transaction_params(current_transaction)
        new_transaction = merge(current_transaction_params, transaction_params)
        return replace_transaction(self.web3, current_transaction, new_transaction)

    def sendTransaction(self, transaction):
        # TODO: move to middleware
        if 'from' not in transaction and is_checksum_address(self.defaultAccount):
            transaction = assoc(transaction, 'from', self.defaultAccount)

        # TODO: move gas estimation in middleware
        if 'gas' not in transaction:
            transaction = assoc(
                transaction,
                'gas',
                get_buffered_gas_estimate(self.web3, transaction),
            )

        return self.web3.manager.request_blocking(
            "eth_sendTransaction",
            [transaction],
        )

    def sendRawTransaction(self, raw_transaction):
        return self.web3.manager.request_blocking(
            "eth_sendRawTransaction",
            [raw_transaction],
        )

    def sign(self, account, data=None, hexstr=None, text=None):
        message_hex = to_hex(data, hexstr=hexstr, text=text)
        return self.web3.manager.request_blocking(
            "eth_sign", [account, message_hex],
        )

    @apply_to_return_value(HexBytes)
    def call(self, transaction, block_identifier=None):
        # TODO: move to middleware
        if 'from' not in transaction and is_checksum_address(self.defaultAccount):
            transaction = assoc(transaction, 'from', self.defaultAccount)

        # TODO: move to middleware
        if block_identifier is None:
            block_identifier = self.defaultBlock
        return self.web3.manager.request_blocking(
            "eth_call",
            [transaction, block_identifier],
        )

    def estimateGas(self, transaction):
        # TODO: move to middleware
        if 'from' not in transaction and is_checksum_address(self.defaultAccount):
            transaction = assoc(transaction, 'from', self.defaultAccount)

        return self.web3.manager.request_blocking(
            "eth_estimateGas",
            [transaction],
        )

    def filter(self, filter_params=None, filter_id=None):
        if filter_id and filter_params:
            raise TypeError(
                "Ambiguous invocation: provide either a `filter_params` or a `filter_id` argument. "
                "Both were supplied."
            )
        if is_string(filter_params):
            if filter_params == "latest":
                filter_id = self.web3.manager.request_blocking(
                    "eth_newBlockFilter", [],
                )
                return BlockFilter(self.web3, filter_id)
            elif filter_params == "pending":
                filter_id = self.web3.manager.request_blocking(
                    "eth_newPendingTransactionFilter", [],
                )
                return TransactionFilter(self.web3, filter_id)
            else:
                raise ValueError(
                    "The filter API only accepts the values of `pending` or "
                    "`latest` for string based filters"
                )
        elif isinstance(filter_params, dict):
            _filter_id = self.web3.manager.request_blocking(
                "eth_newFilter",
                [filter_params],
            )
            return LogFilter(self.web3, _filter_id)
        elif filter_id and not filter_params:
            return LogFilter(self.web3, filter_id)
        else:
            raise TypeError("Must provide either filter_params as a string or "
                            "a valid filter object, or a filter_id as a string "
                            "or hex.")

    def getFilterChanges(self, filter_id):
        return self.web3.manager.request_blocking(
            "eth_getFilterChanges", [filter_id],
        )

    def getFilterLogs(self, filter_id):
        return self.web3.manager.request_blocking(
            "eth_getFilterLogs", [filter_id],
        )

    def getLogs(self, filter_params):
        return self.logs_cache.get(filter_params,
                                   self.getBlock('earliest')["number"],
                                   self.getBlock('latest')["number"])

    def uninstallFilter(self, filter_id):
        return self.web3.manager.request_blocking(
            "eth_uninstallFilter", [filter_id],
        )

    def contract(self,
                 address=None,
                 **kwargs):
        ContractFactoryClass = kwargs.pop('ContractFactoryClass', self.defaultContractFactory)

        ContractFactory = ContractFactoryClass.factory(self.web3, **kwargs)

        if address:
            return ContractFactory(address)
        else:
            return ContractFactory

    def setContractFactory(self, contractFactory):
        self.defaultContractFactory = contractFactory

    def getCompilers(self):
        return self.web3.manager.request_blocking("eth_getCompilers", [])

    def getWork(self):
        return self.web3.manager.request_blocking("eth_getWork", [])

    def generateGasPrice(self, transaction_params=None):
        if self.gasPriceStrategy:
            return self.gasPriceStrategy(self.web3, transaction_params)

    def setGasPriceStrategy(self, gas_price_strategy):
        self.gasPriceStrategy = gas_price_strategy
