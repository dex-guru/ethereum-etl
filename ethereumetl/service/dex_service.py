# import json
# import logging
# from collections.abc import Iterable
# from pathlib import Path
#
# import clickhouse_connect
# from clickhouse_connect.driver import Client
#
# from ethereumetl.utils import parse_clickhouse_url
#
# logger = logging.getLogger('dex_service')
#
#
# class DexService:
#     """
#     resolve factories, resolve pools, parse events.
#
#     """
#
#     def __init__(self, web3, chain_id, clickhouse_url, function_call_result_transformer=None):
#         self._web3 = web3
#         self._clickhouse_url = clickhouse_url
#         self.clickhouse: Client = self.clickhouse_client_from_url(self._clickhouse_url)
#         self.dex_namespaces = self.get_dex_namespaces()
#         self.dex_factories = self.get_dex_factories(chain_id)
#         self._function_call_result_transformer = function_call_result_transformer
#
#     @staticmethod
#     def clickhouse_client_from_url(url) -> Client:
#         connect_kwargs = parse_clickhouse_url(url)
#         return clickhouse_connect.create_client(
#             **connect_kwargs, compress=False, query_limit=0, send_receive_timeout=600
#         )
#
#     @staticmethod
#     def get_dex_namespaces():
#         abi_dir = Path('ethereumetl/service/amm')
#         namespaces = []
#         for file_path in abi_dir.rglob('*.py'):
#             namespace = (
#                 str(str(Path(file_path).name[:-3]))
#                 if str(Path(file_path).name) != '__init__.py'
#                 else None
#             )
#             if namespace:
#                 namespaces.append(namespace)
#         return namespaces
#
#     def read_deployment_metadata(self, chain_id) -> Iterable[dict]:
#         abi_dir = Path('./ethereumetl/abi')
#         for file_path in abi_dir.rglob(f'deploys/{chain_id}/metadata.json'):
#             with file_path.open() as f:
#                 data = json.load(f)
#             assert isinstance(data, list)
#             yield data
#
#     def get_dex_factories(self, chain_id):
#         """
#         Reads Metadata  from  f"{item['type']}/deploys/{item['chain_id']}/metadata.json".
#         """
#         factories = {}
#         for metadatas in self.read_deployment_metadata(chain_id):
#             for metadata in metadatas:
#                 if metadata['type'] in self.dex_namespaces and metadata.get('contracts'):
#                     for contract_name, address in metadata['contracts'].items():
#                         if 'Factory'.lower() in contract_name.lower():
#                             factories[address] = {
#                                 'namespace': metadata['type'],
#                                 'dex': metadata['name'],
#                             }
#                             break
#         return factories
#
#     def get_logs_from_db(self, start_block, end_block):
#         query = f"""
#             SELECT
#                 logs.block_number as block_number,
#                 info.namespace as namespace,
#                 info.contract_name as contract_name,
#                 info.event_name as event_name,
#                 logs.address as contract_address,
#                 logs.transaction_hash as transaction_hash,
#                 logs.log_index as log_index,
#                 logs.topics as topics,
#                 logs.data as data,
#                 info.event_signature as signature,
#                 logs.block_hash as block_hash
#             FROM `logs` AS logs
#             INNER JOIN `event_inventory` AS info
#                 ON (logs.topics[1], toUInt8(length(logs.topics))) = info.event_signature_hash_and_log_topic_count
#                 AND hasAny(info.namespace, {self.dex_namespaces})
#             WHERE logs.block_number >= {start_block} AND logs.block_number <= {end_block}
#         """
#         logs_decoded = tuple(self.clickhouse.query(query).named_results())
#         return logs_decoded
#
#     def extract_dex_events_from_logs(self, start_block, end_block):
#         """
#         extract pools from logs.
#
#         :param logs:
#         :return:
#         """
#         self.get_logs_from_db(start_block, end_block)
#         print(self.dex_namespaces)
#         pass
#
#
# #     def get_token(self, token_address):
# #         checksum_address = self._web3.toChecksumAddress(token_address)
# #         contract = self._web3.eth.contract(address=checksum_address, abi=ERC20_ABI)
# #         contract_alternative_1 = self._web3.eth.contract(
# #             address=checksum_address, abi=ERC20_ABI_ALTERNATIVE_1
# #         )
# #
# #         symbol = self._get_first_result(
# #             contract.functions.symbol(),
# #             contract.functions.SYMBOL(),
# #             contract_alternative_1.functions.symbol(),
# #             contract_alternative_1.functions.SYMBOL(),
# #         )
# #         if isinstance(symbol, bytes):
# #             symbol = self._bytes_to_string(symbol)
# #
# #         name = self._get_first_result(
# #             contract.functions.name(),
# #             contract.functions.NAME(),
# #             contract_alternative_1.functions.name(),
# #             contract_alternative_1.functions.NAME(),
# #         )
# #         if isinstance(name, bytes):
# #             name = self._bytes_to_string(name)
# #
# #         decimals = self._get_first_result(
# #             contract.functions.decimals(), contract.functions.DECIMALS()
# #         )
# #         total_supply = self._get_first_result(contract.functions.totalSupply())
# #
# #         token = EthToken(
# #             address=token_address,
# #             symbol=symbol or '',
# #             name=name or '',
# #             decimals=decimals or 0,
# #             total_supply=total_supply or 0,
# #         )
# #
# #         return token
# #
# #     def _get_first_result(self, *funcs):
# #         for func in funcs:
# #             result = self._call_contract_function(func)
# #             if result is not None:
# #                 return result
# #         return None
# #
# #     def _call_contract_function(self, func):
# #         # BadFunctionCallOutput exception happens if the token doesn't implement a particular function
# #         # or was self-destructed
# #         # OverflowError exception happens if the return type of the function doesn't match the expected type
# #         result = call_contract_function(
# #             func=func,
# #             ignore_errors=(
# #                 BadFunctionCallOutput,
# #                 ContractLogicError,
# #                 OverflowError,
# #                 ValueError,
# #             ),
# #             default_value=None,
# #         )
# #
# #         if self._function_call_result_transformer is not None:
# #             return self._function_call_result_transformer(result)
# #         else:
# #             return result
# #
# #     def _bytes_to_string(self, b, ignore_errors=True):
# #         if b is None:
# #             return b
# #
# #         try:
# #             b = b.decode('utf-8')
# #         except UnicodeDecodeError as e:
# #             if ignore_errors:
# #                 logger.debug(
# #                     'A UnicodeDecodeError exception occurred while trying to decode bytes to string',
# #                     exc_info=True,
# #                 )
# #                 b = None
# #             else:
# #                 raise e
# #
# #         if self._function_call_result_transformer is not None:
# #             b = self._function_call_result_transformer(b)
# #         return b
# #
# #
# # def call_contract_function(func, ignore_errors, default_value=None):
# #     try:
# #         result = func.call()
# #         return result
# #     except Exception as ex:
# #         if type(ex) in ignore_errors:
# #             logger.debug(
# #                 f'An exception occurred in function {func.fn_name} of contract {func.address}. '
# #                 + 'This exception can be safely ignored.',
# #                 exc_info=True,
# #             )
# #             return default_value
# #         else:
# #             raise ex
