import ccxt

def _fetch_positions(self, symbols = None, params={}):
    self.load_markets()
    request = {
        # 'instType': 'MARGIN',  # optional string, MARGIN, SWAP, FUTURES, OPTION
        # 'instId': market['id'],  # optional string, e.g. 'BTC-USD-190927-5000-C'
        # 'posId': '307173036051017730',  # optional string, Single or multiple position IDs(no more than 20) separated with commas
    }
    if symbols is not None:
        marketIds = []
        for i in range(0, len(symbols)):
            entry = symbols[i]
            market = self.market(entry)
            marketIds.append(market['id'])
        marketIdsLength = len(marketIds)
        if marketIdsLength > 0:
            request['instId'] = ','.join(marketIds)
    fetchPositionsOptions = self.safe_value(self.options, 'fetchPositions', {})
    method = self.safe_string(fetchPositionsOptions, 'method', 'privateGetAccountPositions')
    response = getattr(self, method)(self.extend(request, params))

    positions = self.safe_value(response, 'data', [])
    result = []
    for i in range(0, len(positions)):
        result.append(self.parse_position(positions[i]))
    return self.filter_by_array(result, 'symbol', symbols, False)


def patch():
    ccxt.okex.fetch_positions = _fetch_positions
