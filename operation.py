from datetime import datetime
import pandas as pd
import logging
import math
import traceback

from asset import Amount
from asset import Feed
# import bitsharesbase.operationids

# operation id : operation name dictionary for printing out readable data
operation_names = {
    0: 'transfer_operation',
    1: 'limit_order_create_operation',
    2: 'limit_order_cancel_operation',
    3: 'call_order_update_operation',
    4: 'fill_order_operation',
    5: 'account_create_operation',
    6: 'account_update_operation',
    7: 'account_whitelist_operation',
    8: 'account_upgrade_operation',
    9: 'account_transfer_operation',
    10: 'asset_create_operation',
    11: 'asset_update_operation',
    12: 'asset_update_bitasset_operation',
    13: 'asset_update_feed_producers_operation',
    14: 'asset_issue_operation',
    15: 'asset_reserve_operation',
    16: 'asset_fund_fee_pool_operation',
    17: 'asset_settle_operation',
    18: 'asset_global_settle_operation',
    19: 'asset_publish_feed_operation',
    20: 'witness_create_operation',
    21: 'witness_update_operation',
    22: 'proposal_create_operation',
    23: 'proposal_update_operation',
    24: 'proposal_delete_operation',
    25: 'withdraw_permission_create_operation',
    26: 'withdraw_permission_update_operation',
    27: 'withdraw_permission_claim_operation',
    28: 'withdraw_permission_delete_operation',
    29: 'committee_member_create_operation',
    30: 'committee_member_update_operation',
    31: 'committee_member_update_global_parameters_operation',
    32: 'vesting_balance_create_operation',
    33: 'vesting_balance_withdraw_operation',
    34: 'worker_create_operation',
    35: 'custom_operation',
    36: 'assert_operation',
    37: 'balance_claim_operation',
    38: 'override_transfer_operation',
    39: 'transfer_to_blind_operation',
    40: 'blind_transfer_operation',
    41: 'transfer_from_blind_operation',
    42: 'asset_settle_cancel_operation',
    43: 'asset_claim_fees_operation',
    44: 'fba_distribute_operation',
    45: 'bid_collateral_operation',
    46: 'execute_bid_operation',
    47: 'asset_claim_pool_operation',
    48: 'asset_update_issuer_operation',
    48: 'custom_authority_create_operation',
    50: 'custom_authority_update_operation',
    52: 'custom_authority_delete_operation'
}

# operation base class that captures the common attributes


class Operation():
    def __init__(self, operation_json):
        self.account = operation_json['_source']['account_history']['account']
        self.operation_id = operation_json['_source']['operation_id_num']
        self.operation_type = operation_json['_source']['operation_type']
        self.block_number = operation_json['_source']['block_data']['block_num']
        self.block_time = datetime.strptime(
            operation_json['_source']['block_data']['block_time'], '%Y-%m-%dT%H:%M:%S')

    index = 'operation_id'
    cols = ['operation_id', 'account',
            'operation_type', 'block_number', 'block_time']

    def get_values_list(self):
        return [self.operation_id, self.account, self.operation_type, self.block_number, self.block_time]

    @classmethod
    def empty_df(cls):
        return pd.DataFrame(columns=cls.cols)

    # return a list with only the column names as row 0
    @classmethod
    def empty_list(cls):
        return [cls.cols]

    @classmethod
    def get_agg_columns(cls, asset_ids, market_asset_id_pairs):
        return []

    @classmethod
    def get_agg_value_list(cls, asset_ids, market_asset_id_pairs, operations):
        return []

# operation that transfers an asset amount from once account to another


class Transfer(Operation):
    def __init__(self, operation_json):
        Operation.__init__(self, operation_json)
        self.from_account = operation_json['_source']['operation_history']['op'][1]['from']
        self.to_account = operation_json['_source']['operation_history']['op'][1]['to']
        self.amount = Amount(
            operation_json['_source']['operation_history']['op'][1]['amount'])

    index = Operation.index
    cols = Operation.cols + ['from_account',
                             'to_account',
                             'amount.asset_id',
                             'amount.amount']
    agg_cols = []

    def get_values_list(self):
        return super().get_values_list() + [self.from_account,
                                            self.to_account,
                                            self.amount.asset_id,
                                            self.amount.amount]

    @classmethod
    def get_agg_columns(cls, asset_ids, market_asset_id_pairs):
        agg_cols = Operation.get_agg_columns(asset_ids, market_asset_id_pairs)
        for asset in asset_ids:
            agg_cols += [cls.__name__ + '_' + asset + '_count',
                             cls.__name__ + '_' + asset + '_amount']
        return agg_cols

    @classmethod
    def get_agg_value_list(cls, asset_ids, market_asset_id_pairs, operations):
        value_list = Operation.get_agg_value_list(asset_ids, market_asset_id_pairs, operations)
        for asset_id in asset_ids:
            filtered_oerations = operations[operations['amount.asset_id'] == asset_id]
            aggregates = filtered_oerations.agg(
                {'amount.amount': ['count', 'sum']})
            value_list += [aggregates.iloc[0]['amount.amount'],
                           aggregates.iloc[1]['amount.amount']]
        return value_list


# operation that creates the ability for a counterparty to execute a transaction against an account


class LimitOrderCreate(Operation):
    def __init__(self, operation_json):
        Operation.__init__(self, operation_json)
        self.seller = operation_json['_source']['operation_history']['op'][1]['seller']
        self.amount_to_sell = Amount(
            operation_json['_source']['operation_history']['op'][1]['amount_to_sell'])
        self.min_to_receive = Amount(
            operation_json['_source']['operation_history']['op'][1]['min_to_receive'])
        self.expiration = datetime.strptime(
            operation_json['_source']['operation_history']['op'][1]['expiration'], '%Y-%m-%dT%H:%M:%S')
        self.expiration_in_seconds = (datetime.strptime(
            operation_json['_source']['operation_history']['op'][1]['expiration'],
            '%Y-%m-%dT%H:%M:%S') - self.block_time).total_seconds()
        if self.expiration_in_seconds < 0:
            self.expiration_in_seconds = -1
        self.fill_or_kill = operation_json['_source']['operation_history']['op'][1]['fill_or_kill']
        self.market = self.min_to_receive.asset_id + '/' + self.amount_to_sell.asset_id
        self.min_rate = self.min_to_receive.amount/self.amount_to_sell.amount
        self.limit_id = operation_json['_source']['operation_history']['operation_result'][1]

    index = Operation.index
    cols = Operation.cols + ['seller',
                             'amount_to_sell.asset_id',
                             'amount_to_sell.amount',
                             'min_to_receive.asset_id',
                             'min_to_receive.amount',
                             'expiration',
                             'expiration_in_seconds',
                             'fill_or_kill',
                             'market',
                             'min_rate',
                             'limit_id']
    agg_cols = []

    def get_values_list(self):
        return super().get_values_list() + [self.seller,
                                            self.amount_to_sell.asset_id,
                                            self.amount_to_sell.amount,
                                            self.min_to_receive.asset_id,
                                            self.min_to_receive.amount,
                                            self.expiration,
                                            self.expiration_in_seconds,
                                            self.fill_or_kill,
                                            self.market,
                                            self.min_rate,
                                            self.limit_id]

    @classmethod
    def get_agg_columns(cls, asset_ids, market_asset_id_pairs):
        agg_cols = Operation.get_agg_columns(asset_ids, market_asset_id_pairs)
        for asset_pair in market_asset_id_pairs:
            asset_id_pair = asset_pair[0] + '/' + asset_pair[1]
            agg_cols += [cls.__name__ + '_' + asset_id_pair + '_count',
                             cls.__name__ + '_sell_' + asset_id_pair + '_amount',
                             cls.__name__ + '_minreceive_' + asset_id_pair + '_amount',
                             cls.__name__ + '_' + asset_id_pair + '_medianrate',
                             cls.__name__ + '_' + asset_id_pair + '_averagerate',
                             cls.__name__ + '_' + asset_id_pair + '_maxrate',
                             cls.__name__ + '_' + asset_id_pair + '_minrate',
                             cls.__name__ + '_' + asset_id_pair + '_stddevrate',
                             cls.__name__ + '_sell_' + asset_id_pair + '_medianexpiration',
                             cls.__name__ + '_sell_' + asset_id_pair + '_averageexpiration',
                             cls.__name__ + '_sell_' + asset_id_pair + '_pctnegativeexpiration'
                             ]
        return agg_cols

    @classmethod
    def get_agg_value_list(cls, asset_ids, market_asset_id_pairs, operations):
        value_list = Operation.get_agg_value_list(asset_ids, market_asset_id_pairs, operations)
        for asset_id_pair in market_asset_id_pairs:
            filtered_operations = operations[(operations['min_to_receive.asset_id'] == asset_id_pair[0]) &
                                             (operations['amount_to_sell.asset_id'] == asset_id_pair[1])]

            filtered_operation_cnt = len(filtered_operations.index)
            if filtered_operation_cnt != 0:
                filtered_negative_expriations = filtered_operations[
                    filtered_operations['expiration_in_seconds'] < 0]
                aggregates = filtered_operations.agg(
                    {
                        'min_to_receive.amount': ['sum'],
                        'amount_to_sell.amount': ['sum'],
                        'min_rate': ['median', 'mean', 'max', 'min', 'std'],
                        'expiration_in_seconds': ['median', 'mean']
                    })
                value_list += [filtered_operation_cnt,
                               aggregates.loc[['sum']
                                              ].iloc[0]['amount_to_sell.amount'],
                               aggregates.loc[['sum']
                                              ].iloc[0]['min_to_receive.amount'],
                               aggregates.loc[['median']
                                              ].iloc[0]['min_rate'],
                               aggregates.loc[['mean']
                                              ].iloc[0]['min_rate'],
                               aggregates.loc[['max']
                                              ].iloc[0]['min_rate'],
                               aggregates.loc[['min']
                                              ].iloc[0]['min_rate'],
                               aggregates.loc[['std']
                                              ].iloc[0]['min_rate'],
                               aggregates.loc[['median']
                                              ].iloc[0]['expiration_in_seconds'],
                               aggregates.loc[['mean']
                                              ].iloc[0]['expiration_in_seconds'],
                               len(filtered_negative_expriations.index)/filtered_operation_cnt
                               ]
            else:
                value_list += [0, 0, 0, '', '', '', '', '', 0, 0, 0]
        return value_list


# operation that cancels a limit order


class LimitOrderCancel(Operation):
    def __init__(self, operation_json):
        Operation.__init__(self, operation_json)
        self.canceler = operation_json['_source']['operation_history']['op'][1]['fee_paying_account']
        self.limit_id = operation_json['_source']['operation_history']['op'][1]['order']
        # not in earlier transactions
        # self.canceled_amount = Asset(operation_json['_source']['operation_history']['operation_result'][1])

    index = Operation.index
    cols = Operation.cols + ['canceler', 'limit_id']
    agg_cols = []

    def get_values_list(self):
        return super().get_values_list() + [self.canceler, self.limit_id]

    @classmethod
    def get_agg_columns(cls, asset_ids, market_asset_id_pairs):
        agg_cols = Operation.get_agg_columns(asset_ids, market_asset_id_pairs)
        agg_cols += [cls.__name__ + '_count']
        return agg_cols

    @classmethod
    def get_agg_value_list(cls, asset_ids, market_asset_id_pairs, operations):
        value_list = Operation.get_agg_value_list(asset_ids, market_asset_id_pairs, operations)
        value_list += [len(operations.index)]
        return value_list

# # operation that updates an existing limit order


# class LimitOrderupdate(Operation):
#     def __init__(self, operation_json):
#         Operation.__init__(self, operation_json)
#         self.funding_account = \
#             operation_json['_source']['operation_history']['op'][1]['funding_account']
#         self.delta_collateral = Amount(
#             operation_json['_source']['operation_history']['op'][1]['delta_collateral'])
#         self.delta_debt = Amount(
#             operation_json['_source']['operation_history']['op'][1]['delta_debt'])

#     index = Operation.index
#     cols = Operation.cols + ['funding_account',
#                              'delta_collateral.asset_id',
#                              'delta_collateral.amount',
#                              'delta_debt.asset_id',
#                              'delta_debt.amount'
#                              ]
#     agg_cols = []

#     def get_values_list(self):
#         return super().get_values_list() + \
#             [self.funding_account,
#              self.delta_collateral.asset_id,
#              self.delta_collateral.amount,
#              self.delta_debt.asset_id,
#              self.delta_debt.amount
#              ]

#     @classmethod
#     def get_agg_columns(cls, asset_ids, market_asset_id_pairs):
#         for asset_pair in market_asset_id_pairs:
#             agg_cols += [cls.__name__ + '_' + asset_pair[0] + '/' + asset_pair[1] + '_count',
#                              cls.__name__ + '_base_' +
#                              asset_pair[0] + '_amount',
#                              cls.__name__ + '_quote_' +
#                              asset_pair[1] + '_amount'
#                              ]
#         return agg_cols

#     @classmethod
#     def get_agg_value_list(cls, asset_ids, market_asset_id_pairs, operations):
#         value_list = []
#         for asset_id_pair in market_asset_id_pairs:
#             filtered_operations = operations[(operations['delta_collateral.asset_id'] == asset_id_pair[0]) &
#                                             (operations['delta_debt.asset_id'] == asset_id_pair[1])]

#             filtered_operation_cnt = len(filtered_operations.index)
#             if filtered_operation_cnt != 0:
#                 aggregates = filtered_operations.agg(
#                     {'delta_collateral.amount': ['sum'],
#                      'delta_debt.amount': ['sum']
#                      })
#                 value_list += [filtered_operation_cnt,
#                                aggregates.loc[['sum']].iloc[0]['delta_collateral.amount'],
#                                aggregates.loc[['sum']].iloc[0]['delta_debt.amount']
#                                ]
#             else:
#                 value_list += [0, 0, 0]
#         return value_list


# represents filling all or part of a limit order


class FillOrder(Operation):
    def __init__(self, operation_json):
        Operation.__init__(self, operation_json)
        self.account = operation_json['_source']['operation_history']['op'][1]['account_id']
        self.receives = Amount(
            operation_json['_source']['operation_history']['op'][1]['receives'])
        self.pays = Amount(
            operation_json['_source']['operation_history']['op'][1]['pays'])
        self.fill_base = Amount(
            operation_json['_source']['operation_history']['op'][1]['fill_price']['base'])
        self.fill_quote = Amount(
            operation_json['_source']['operation_history']['op'][1]['fill_price']['quote'])
        self.limit_id = operation_json['_source']['operation_history']['op'][1]['order_id']

    index = Operation.index
    cols = Operation.cols + ['account',
                             'receives.asset_id',
                             'receives.amount',
                             'pays.asset_id',
                             'pays.amount',
                             'fill_base.asset_id',
                             'fill_base.amount',
                             'fill_quote.asset_id',
                             'fill_quote.amount',
                             'limit_id',
                             'market',
                             'rate'
                             ]
    agg_cols = []

    def get_values_list(self):
        values = super().get_values_list()
        values += [
            self.account,
            self.receives.asset_id,
            self.receives.amount,
            self.pays.asset_id,
            self.pays.amount,
            self.fill_base.asset_id,
            self.fill_base.amount,
            self.fill_quote.asset_id,
            self.fill_quote.amount,
            self.limit_id,
            self.fill_base.asset_id + '/' + self.fill_quote.asset_id
            ]
        # return rate of quote amount <> 0, null otherwise
        if self.fill_quote.amount != 0:
            values += [self.fill_base.amount/self.fill_quote.amount]
        else:
            values += ['']
            logging.info('operation {} has 0 for quote amount'.format(self.operation_id))

        return values

    @classmethod
    def get_agg_columns(cls, asset_ids, market_asset_id_pairs):
        agg_cols = Operation.get_agg_columns(asset_ids, market_asset_id_pairs)
        for asset_pair in market_asset_id_pairs:
            asset_id_pair = asset_pair[0] + '/' + asset_pair[1]
            agg_cols += [cls.__name__ + '_' + asset_id_pair + '_count',
                             cls.__name__ + '_receives_' + asset_id_pair + '_' + asset_pair[0] + '_amount',
                             cls.__name__ + '_pays_' + asset_id_pair + '_' + asset_pair[1] + '_amount',
                             cls.__name__ + '_' + asset_id_pair + '_medianrate',
                             cls.__name__ + '_' + asset_id_pair + '_meanrate',
                             cls.__name__ + '_' + asset_id_pair + '_minrate',
                             cls.__name__ + '_' + asset_id_pair + '_maxrate',
                             cls.__name__ + '_' + asset_id_pair + '_stddevrate',
                             cls.__name__ + '_' + asset_id_pair + '_last10pctrate',
                             ]
        return agg_cols

    @classmethod
    def get_agg_value_list(cls, asset_ids, market_asset_id_pairs, operations):
        value_list = Operation.get_agg_value_list(asset_ids, market_asset_id_pairs, operations)
        for asset_id_pair in market_asset_id_pairs:
            filtered_operations = operations[(operations['fill_base.asset_id'] == asset_id_pair[0]) &
                                             (operations['fill_quote.asset_id'] == asset_id_pair[1]) &
                                             (operations['rate'] != 0)]
            filtered_operation_cnt = len(filtered_operations.index)
            if filtered_operation_cnt != 0:
                last_10pct_row_cnt = math.ceil(filtered_operation_cnt*0.10)
                aggregates = filtered_operations.agg(
                    {'receives.amount': ['sum'],
                     'pays.amount': ['sum'],
                     'rate': ['median', 'mean', 'min', 'max', 'std']
                     })
                value_list += [filtered_operation_cnt,
                               aggregates.loc[['sum']
                                              ].iloc[0]['receives.amount'],
                               aggregates.loc[['sum']].iloc[0]['pays.amount'],
                               aggregates.loc[['median']].iloc[0]['rate'],
                               aggregates.loc[['mean']].iloc[0]['rate'],
                               aggregates.loc[['min']].iloc[0]['rate'],
                               aggregates.loc[['max']].iloc[0]['rate'],
                               aggregates.loc[['std']].iloc[0]['rate'],
                               ]

                # now get the median last 10% of rates
                last10pct_filtered_operations = filtered_operations.tail(last_10pct_row_cnt)
                aggregates = last10pct_filtered_operations.agg({'rate': ['median']})
                value_list += [aggregates.loc[['median']].iloc[0]['rate']]
            else:
                value_list += [0, 0, 0, '', '', '', '', '', '']
        return value_list


# Schedules a market-issued asset for automatic settlement
#
# Holders of market-issued assets may request a forced settlement
# for some amount of their asset. This means that the specified sum
# will be locked by the chain and held for the settlement period,
# after which time the chain will choose a margin position holder and
# buy the settled asset using the margin’s collateral. The price of
# this sale will be based on the feed price for the market-issued
# asset being settled. The exact settlement price will be the feed
# price at the time of settlement with an offset in favor of the
# margin position, where the offset is a blockchain parameter set
# in the global_property_object.
#
# The fee is paid by account, and account must authorize this operation
# represents market maker publishing an asset price


class AssetSettle(Operation):
    def __init__(self, operation_json):
        Operation.__init__(self, operation_json)
        self.account = \
            operation_json['_source']['operation_history']['op'][1]['account']
        self.amount = Amount(
            operation_json['_source']['operation_history']['op'][1]['amount'])

    index = Operation.index
    cols = Operation.cols + ['account',
                             'amount.asset_id',
                             'amount.amount'
                             ]
    agg_cols = []

    def get_values_list(self):
        return super().get_values_list() + [self.account,
                                            self.amount.asset_id,
                                            self.amount.amount
                                            ]

    @classmethod
    def get_agg_columns(cls, asset_ids, market_asset_id_pairs):
        agg_cols = Operation.get_agg_columns(asset_ids, market_asset_id_pairs)
        for asset in asset_ids:
            agg_cols += [cls.__name__ + '_' + asset + '_count',
                             cls.__name__ + '_' + asset + '_amount'
                             ]
        return agg_cols

    @classmethod
    def get_agg_value_list(cls, asset_ids, market_asset_id_pairs, operations):
        value_list = Operation.get_agg_value_list(asset_ids, market_asset_id_pairs, operations)
        for asset_id in asset_ids:
            filtered_operations = operations[operations['amount.asset_id'] == asset_id]
            filtered_operation_cnt = len(filtered_operations.index)
            if filtered_operation_cnt != 0:
                aggregates = filtered_operations.agg({'amount.amount': ['sum']})
                value_list += [filtered_operation_cnt,
                               aggregates.loc[['sum']].iloc[0]['amount.amount']
                               ]
            else:
                value_list += [0, 0]
        return value_list

# represents market maker publishing an asset price


class AssetPublishFeed(Operation):
    def __init__(self, operation_json):
        Operation.__init__(self, operation_json)
        self.publisher = operation_json['_source']['operation_history']['op'][1]['publisher']
        self.asset_id = operation_json['_source']['operation_history']['op'][1]['asset_id']
        self.feed = Feed(operation_json['_source']
                         ['operation_history']['op'][1]['feed'])

    index = Operation.index
    cols = Operation.cols + ['publisher',
                             'asset_id',
                             'feed.settlement_price.base.asset_id',
                             'feed.settlement_price.base.amount',
                             'feed.settlement_price.quote.asset_id',
                             'feed.settlement_price.quote.amount',
                             'feed.maintenance_collateral_ratio',
                             'feed.maximum_short_squeeze_ratio',
                             'feed.core_exchange_rate.base.asset_id',
                             'feed.core_exchange_rate.base.amount',
                             'feed.core_exchange_rate.quote.asset_id',
                             'feed.core_exchange_rate.quote.amount',
                             'rate']
    agg_cols = []

    def get_values_list(self):
        values = super().get_values_list()
        values += [
            self.publisher,
            self.asset_id,
            self.feed.settlement_price.base.asset_id,
            self.feed.settlement_price.base.amount,
            self.feed.settlement_price.quote.asset_id,
            self.feed.settlement_price.quote.amount,
            self.feed.maintenance_collateral_ratio,
            self.feed.maximum_short_squeeze_ratio,
            self.feed.core_exchange_rate.base.asset_id,
            self.feed.core_exchange_rate.base.amount,
            self.feed.core_exchange_rate.quote.asset_id,
            self.feed.core_exchange_rate.quote.amount
        ]
        if self.feed.settlement_price.quote.amount != 0:
            values += [self.feed.settlement_price.base.amount /
                       self.feed.settlement_price.quote.amount]
        else:
            values += ['']
            logging.info(
                'operation {} pays 0 for quote amount'.format(self.operation_id))
        return values

    @classmethod
    def get_agg_columns(cls, asset_ids, market_asset_id_pairs):
        agg_cols = Operation.get_agg_columns(asset_ids, market_asset_id_pairs)
        for asset_pair in market_asset_id_pairs:
            asset_id_pairs = asset_pair[0] + '/' + asset_pair[1]
            agg_cols += [cls.__name__ + '_' + asset_id_pairs + '_count',
                             cls.__name__ + '_' + asset_id_pairs + '_medianrate',
                             cls.__name__ + '_' + asset_id_pairs + '_meanrate',
                             cls.__name__ + '_' + asset_id_pairs + '_minrate',
                             cls.__name__ + '_' + asset_id_pairs + '_maxrate',
                             cls.__name__ + '_' + asset_id_pairs + '_stddevrate',
                             cls.__name__ + '_' + asset_id_pairs + '_last10pctrate',
                             ]
        return agg_cols

    @classmethod
    def get_agg_value_list(cls, asset_ids, market_asset_id_pairs, operations):
        value_list = Operation.get_agg_value_list(asset_ids, market_asset_id_pairs, operations)
        for asset_id_pair in market_asset_id_pairs:
            filtered_operations = operations[(operations['feed.settlement_price.base.asset_id'] == asset_id_pair[0]) &
                                             (operations['feed.settlement_price.quote.asset_id'] == asset_id_pair[1])]
            filtered_operation_cnt = len(filtered_operations.index)
            if filtered_operation_cnt != 0:
                last_10pct_row_cnt = math.ceil(filtered_operation_cnt*0.10)
                aggregates = filtered_operations.agg({'rate': ['median', 'mean', 'min', 'max', 'std']})
                value_list += [filtered_operation_cnt,
                               aggregates.loc[['median']].iloc[0]['rate'],
                               aggregates.loc[['mean']].iloc[0]['rate'],
                               aggregates.loc[['min']].iloc[0]['rate'],
                               aggregates.loc[['max']].iloc[0]['rate'],
                               aggregates.loc[['std']].iloc[0]['rate']
                               ]

                # now get the median last 10% of rates
                last10pct_filtered_operations = filtered_operations.tail(last_10pct_row_cnt)
                aggregates = last10pct_filtered_operations.agg({'rate': ['median']})
                value_list += [aggregates.loc[['median']].iloc[0]['rate']]
            else:
                value_list += [0,'','','','','','']
        return value_list

# Create a vesting balance.
#
# The chain allows a user to create a vesting balance. Normally, vesting balances are
# created automatically as part of cashback and worker operations. This operation
# allows vesting balances to be created manually as well.
#
# Manual creation of vesting balances can be used by a stakeholder to publicly
# demonstrate that they are committed to the chain. It can also be used as a building
# block to create transactions that function like public debt. Finally, it is useful
# for testing vesting balance functionality.


class VestingBalanceCreate(Operation):
    def __init__(self, operation_json):
        Operation.__init__(self, operation_json)
        self.creator = operation_json['_source']['operation_history']['op'][1]['creator']
        self.owner = operation_json['_source']['operation_history']['op'][1]['owner']
        self.amount = Amount(
            operation_json['_source']['operation_history']['op'][1]['amount'])

    index = Operation.index
    cols = Operation.cols + ['creator',
                             'owner',
                             'amount.asset_id',
                             'amount.amount'
                             ]
    aggcols = []

    def get_values_list(self):
        return super().get_values_list() + [
            self.creator,
            self.owner,
            self.amount.asset_id,
            self.amount.amount
        ]

    @classmethod
    def get_agg_columns(cls, asset_ids, market_asset_id_pairs):
        agg_cols = Operation.get_agg_columns(asset_ids, market_asset_id_pairs)
        for asset_id in asset_ids:
            agg_cols += [cls.__name__ + '_' + asset_id + '_count',
                         cls.__name__ + '_' + asset_id + '_amount'
                         ]
        return agg_cols

    @classmethod
    def get_agg_value_list(cls, asset_ids, market_asset_id_pairs, operations):
        value_list = Operation.get_agg_value_list(asset_ids, market_asset_id_pairs, operations)
        for asset_id in asset_ids:
            filtered_operations = operations[operations['amount.asset_id'] == asset_id]
            filtered_operation_cnt = len(filtered_operations.index)
            if filtered_operation_cnt != 0:
                aggregates = filtered_operations.agg({'amount.amount': ['sum']})
                value_list += [filtered_operation_cnt,
                               aggregates.loc[['sum']].iloc[0]['amount.amount']
                               ]
            else:
                value_list += [0, 0]
        return value_list


# Withdraw from a vesting balance.
#
# Withdrawal from a not-completely-mature vesting balance will result in paying fees.


class VestingBalanceWithdraw(Operation):
    def __init__(self, operation_json):
        Operation.__init__(self, operation_json)
        self.vesting_balance = \
            operation_json['_source']['operation_history']['op'][1]['vesting_balance']
        self.owner = \
            operation_json['_source']['operation_history']['op'][1]['owner']
        self.amount = Amount(
            operation_json['_source']['operation_history']['op'][1]['amount'])

    index = Operation.index
    cols = Operation.cols + ['vesting_balance',
                             'asownerset_id',
                             'amount.asset_id',
                             'amount.amount'
                             ]
    aggcols = []

    def get_values_list(self):
        return super().get_values_list() + [self.vesting_balance,
                                            self.owner,
                                            self.amount.asset_id,
                                            self.amount.amount
                                            ]

    @classmethod
    def get_agg_columns(cls, asset_ids, market_asset_id_pairs):
        agg_cols = Operation.get_agg_columns(asset_ids, market_asset_id_pairs)
        for asset_id in asset_ids:
            agg_cols += [cls.__name__ + '_' + asset_id + '_count',
                             cls.__name__ + '_' + asset_id + '_amount'
                             ]
        return agg_cols

    @classmethod
    def get_agg_value_list(cls, asset_ids, market_asset_id_pairs, operations):
        value_list = Operation.get_agg_value_list(asset_ids, market_asset_id_pairs, operations)
        for asset_id in asset_ids:
            filtered_operations = operations[operations['amount.asset_id'] == asset_id]
            filtered_operation_cnt = len(filtered_operations.index)
            if filtered_operation_cnt != 0:
                aggregates = filtered_operations.agg({'amount.amount': ['sum']})
                value_list += [filtered_operation_cnt,
                               aggregates.loc[['sum']].iloc[0]['amount.amount']
                               ]
            else:
                value_list += [0, 0]
        return value_list


# blind transfer, tells amount, not to whom


class TransferToBlind(Operation):
    def __init__(self, operation_json):
        Operation.__init__(self, operation_json)
        self.amount = Amount(
            operation_json['_source']['operation_history']['op'][1]['amount'])
        self.from_ = operation_json['_source']['operation_history']['op'][1]['from']

    index = Operation.index
    cols = Operation.cols + ['amount.asset_id', 'amount.amount', 'from_']
    agg_cols = []

    def get_values_list(self):
        return super().get_values_list() + [self.amount.asset_id,
                                            self.amount.amount,
                                            self.from_]

    @classmethod
    def get_agg_columns(cls, asset_ids, market_asset_id_pairs):
        agg_cols = Operation.get_agg_columns(asset_ids, market_asset_id_pairs)
        for asset in asset_ids:
            agg_cols += [cls.__name__ + '_' + asset + '_count',
                             cls.__name__ + '_' + asset + '_amount']
        return agg_cols

    @classmethod
    def get_agg_value_list(cls, asset_ids, market_asset_id_pairs, operations):
        value_list = Operation.get_agg_value_list(asset_ids, market_asset_id_pairs, operations)
        for asset_id in asset_ids:
            filtered_operations = operations[operations['amount.asset_id'] == asset_id]
            filtered_operation_cnt = len(filtered_operations.index)
            if filtered_operation_cnt != 0:
                aggregates = filtered_operations.agg({'amount.amount': ['sum']})
                value_list += [filtered_operation_cnt,
                               aggregates.loc[['sum']].iloc[0]['amount.amount']
                               ]
            else:
                value_list += [0, 0]
        return value_list


# blind transfer, tells who receives, not amount


class TransferFromBlind(Operation):
    def __init__(self, operation_json):
        Operation.__init__(self, operation_json)
        self.asset_id = 'unknown'

    index = Operation.index
    cols = Operation.cols + ['asset_id']
    agg_cols = []

    def get_values_list(self):
        return super().get_values_list() + [self.asset_id]

    @classmethod
    def get_agg_columns(cls, asset_ids, market_asset_id_pairs):
        agg_cols = Operation.get_agg_columns(asset_ids, market_asset_id_pairs)
        agg_cols += [cls.__name__ + '_' +'_count']
        return agg_cols

    @classmethod
    def get_agg_value_list(cls, asset_ids, market_asset_id_pairs, operations):
        value_list = Operation.get_agg_value_list(asset_ids, market_asset_id_pairs, operations)
        value_list += [len(operations.index)]
        return value_list

# override transfer: issues moves any assets where ever the issuer wants


class OverrideTransfer(Operation):
    def __init__(self, operation_json):
        Operation.__init__(self, operation_json)
        self.issuer = operation_json['_source']['operation_history']['op'][1]['issuer']
        self.from_ = operation_json['_source']['operation_history']['op'][1]['from']
        self.to_ = operation_json['_source']['operation_history']['op'][1]['to']
        self.amount = Amount(
            operation_json['_source']['operation_history']['op'][1]['amount'])

    index = Operation.index
    cols = Operation.cols + ['issuer',
                             'from',
                             'to',
                             'amount.asset_id',
                             'amount.amount'
                             ]
    agg_cols = []

    def get_values_list(self):
        return super().get_values_list() + \
            [self.issuer, self.from_, self.to_,
                self.amount.asset_id, self.amount.amount]

    @classmethod
    def get_agg_columns(cls, asset_ids, market_asset_id_pairs):
        agg_cols = Operation.get_agg_columns(asset_ids, market_asset_id_pairs)
        for asset in asset_ids:
            agg_cols += [cls.__name__ + '_' + asset + '_count',
                             cls.__name__ + '_' + asset + '_amount']
        return agg_cols

    @classmethod
    def get_agg_value_list(cls, asset_ids, market_asset_id_pairs, operations):
        value_list = Operation.get_agg_value_list(asset_ids, market_asset_id_pairs, operations)
        for asset_id in asset_ids:
            filtered_operations = operations[operations['amount.asset_id'] == asset_id]
            filtered_operation_cnt = len(filtered_operations.index)
            if filtered_operation_cnt != 0:
                aggregates = filtered_operations.agg({'amount.amount': ['sum']})
                value_list += [filtered_operation_cnt,
                               aggregates.loc[['sum']].iloc[0]['amount.amount']
                               ]
            else:
                value_list += [0, 0]
        return value_list


# transfers accumluated fees back to issuers balance


class AssetClaimFees(Operation):
    def __init__(self, operation_json):
        Operation.__init__(self, operation_json)
        self.issuer = \
            operation_json['_source']['operation_history']['op'][1]['issuer']
        self.amount_to_claim = \
            Amount(operation_json['_source']
                   ['operation_history']['op'][1]['amount_to_claim'])

    index = Operation.index
    cols = Operation.cols + ['issuer',
                             'amount_to_claim.asset_id',
                             'amount_to_claim.amount'
                             ]
    agg_cols = []

    def get_values_list(self):
        return super().get_values_list() + \
            [self.issuer, self.amount_to_claim.asset_id, self.amount_to_claim.amount]

    @classmethod
    def get_agg_columns(cls, asset_ids, market_asset_id_pairs):
        agg_cols = Operation.get_agg_columns(asset_ids, market_asset_id_pairs)
        for asset in asset_ids:
            agg_cols += [cls.__name__ + '_' + asset + '_count',
                             cls.__name__ + '_' + asset + '_amount']
        return agg_cols

    @classmethod
    def get_agg_value_list(cls, asset_ids, market_asset_id_pairs, operations):
        value_list = Operation.get_agg_value_list(asset_ids, market_asset_id_pairs, operations)
        for asset_id in asset_ids:
            filtered_operations = operations[operations['amount_to_claim.asset_id'] == asset_id]
            filtered_operation_cnt = len(filtered_operations.index)
            if filtered_operation_cnt != 0:
                aggregates = filtered_operations.agg(
                    {'amount_to_claim.amount': ['sum']})
                value_list += [filtered_operation_cnt,
                               aggregates.loc[['sum']].iloc[0]['amount_to_claim.amount']]
            else:
                value_list += [0, 0]
        return value_list


# dictionary of supported operations and their assocated Operation subclass
supported_operations = {
    0: Transfer,
    1: LimitOrderCreate,
    2: LimitOrderCancel,
    4: FillOrder,
    17: AssetSettle,
    19: AssetPublishFeed,
    32: VestingBalanceCreate,
    33: VestingBalanceWithdraw,
    38: OverrideTransfer,
    39: TransferToBlind,
    40: TransferFromBlind,
    43: AssetClaimFees
}

# factory for instantiating the operation type specified in the JSON record


def parse_operation(operation_json):
    # get the appropriate class, default to the Operation superclass for all non-supported operation types
    op_class = supported_operations.get(
        operation_json['_source']['operation_type'], Operation)

    # instantiate and returne the specified operation object
    return op_class(operation_json)


def make_aggregate_dfs():
    aggregate_dfs = {}
    for op_key, op_class in supported_operations.items():
        aggregate_dfs[op_key] = op_class.empty_df()
    return aggregate_dfs

# create a dictionary of lists with only one row, the header row. Kind of a poor man's DataFrame


def make_empty_lists():
    empty_lists = {}
    for op_key, op_class in supported_operations.items():
        empty_lists[op_key] = op_class.empty_list()
    return empty_lists


def append_record(operation, op_lists):
    if operation.operation_type in supported_operations.keys():
        value_list = operation.get_values_list()
        op_lists[operation.operation_type].append(value_list)
