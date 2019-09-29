class Amount():
    def __init__(self, asset_json):
        self.asset_id = asset_json['asset_id']
        self.amount = float(asset_json['amount'])


class Market():
    def __init__(self, market_json):
        self.base = Amount(market_json['base'])
        self.quote = Amount(market_json['quote'])


class Feed():
    def __init__(self, feed_json):
        self.settlement_price = Market(feed_json['settlement_price'])
        self.maintenance_collateral_ratio = feed_json['maintenance_collateral_ratio']
        self.maximum_short_squeeze_ratio = feed_json['maximum_short_squeeze_ratio']
        self.core_exchange_rate = Market(feed_json['core_exchange_rate'])


supported_assets = {
    'BTS': '1.3.0',
    'USD': '1.3.121',
    'CNY': '1.3.113',
    'BTC': '1.3.103',
    'GDEX.BTC': '1.3.2241',
    'OPEN.BTC': '1.3.861',
    'BRIDGE.BTC': '1.3.1570',
    'SPARKDEX.BTC': '1.3.4198',
    'RUDEX.BTC': '1.3.3926',
    'ESCODEX.BTC': '1.3.3458'
}

supported_markets = {
    'BTS/OPEN.BTC':     [supported_assets['BTS'],          supported_assets['OPEN.BTC']],
    'BTS/GDEX.BTC':     [supported_assets['BTS'],          supported_assets['GDEX.BTC']],
    'BTS/BRIDGE.BTC':   [supported_assets['BTS'],          supported_assets['BRIDGE.BTC']],
    'BTS/SPARKDEX.BTC': [supported_assets['BTS'],          supported_assets['SPARKDEX.BTC']],
    'OPEN.BTC/BTS':     [supported_assets['OPEN.BTC'],     supported_assets['BTS']],
    'SPARKDEX.BTC/BTS': [supported_assets['SPARKDEX.BTC'], supported_assets['BTS']],
    'ESCODEX.BTC/BTS':  [supported_assets['ESCODEX.BTC'],  supported_assets['BTS']],
    'BTS/BTC':          [supported_assets['BTS'],          supported_assets['BTC']],
    'BTC/BTS':          [supported_assets['BTC'],          supported_assets['BTS']],

    'BTS/CNY':          [supported_assets['BTS'],          supported_assets['CNY']],
    'CNY/BTS':          [supported_assets['CNY'],          supported_assets['BTS']],

    'USD/CNY':          [supported_assets['USD'],          supported_assets['CNY']],
    'CNY/USD':          [supported_assets['CNY'],          supported_assets['USD']],

    'BTS/USD':          [supported_assets['BTS'],          supported_assets['USD']],
    'USD/BTS':          [supported_assets['USD'],          supported_assets['BTS']]
}

synonym_markets = {
    'BTS/BTC': ['BTS/BTC',
                'BTC/BTS',
                'BTS/OPEN.BTC',
                'OPEN.BTC/BTS',
                'BTS/GDEX.BTC',
                'GDEX.BTC/BTS',
                'BTS/BRIDGE.BTC',
                'BRIDGE.BTC/BTS',
                'SPARKDEX.BTC/SPARKDEX.BTC',
                'BTS/BTS'
                ],
    'BTC/BTS': ['BTC/BTS', 'OPEN.BTC/BTS', 'SPARKDEX.BTC/BTS'],
    'USD/CNY': ['USD/CNY', 'CNY/USD'],
    'CNY/USD': ['USD/CNY', 'CNY/USD'],
    'USD/BTS': ['USD/BTS', 'BTS/USD'],
    'BTS/USD': ['USD/BTS', 'BTS/USD'],
    'BTS/CNY': ['BTS/CNY', 'CNY/BTS'],
    'CNY/BTS': ['BTS/CNY', 'CNY/BTS']
}

# get distinct list of assets IDs to be processed and market asset ID pairs
def distinct_asset_ids(market_list):
    distinct_asset_ids    = {}
    market_asset_id_pairs = {}

    for market in market_list:
        if market in synonym_markets.keys():
            synonyms = synonym_markets[market]

            for a_synonym in synonyms:
                # get the quote and base asset id
                quote_asset_id = supported_markets[a_synonym][0]
                base_asset_id  = supported_markets[a_synonym][1]

                # use a dictionary to guarantee a distinct list
                distinct_asset_ids[quote_asset_id] = quote_asset_id
                distinct_asset_ids[base_asset_id]  = base_asset_id

                # create dictionary entry with quote and base asset IDs for value
                market_asset_id_pairs[a_synonym] = [quote_asset_id, base_asset_id]

    return distinct_asset_ids.values(), market_asset_id_pairs.values()

def is_fill_order_last10pct(column):
    return ('FillOrder' in column) & ('/1.3.0' in column) & ('last10pctrate' in column)

