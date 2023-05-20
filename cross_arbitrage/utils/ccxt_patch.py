from typing import Optional
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
    return result

def _fetch_funding_history(self, symbol: Optional[str] = None, since: Optional[int] = None, limit: Optional[int] = None, params={}):
        self.load_markets()
        request = {
            'type': '8',
        }
        if limit is not None:
            request['limit'] = str(limit)  # default 100, max 100
        market = None
        if symbol is not None:
            market = self.market(symbol)
            symbol = market['symbol']
            if market['contract']:
                if market['linear']:
                    request['ctType'] = 'linear'
                    request['ccy'] = market['quoteId']
                else:
                    request['ctType'] = 'inverse'
                    request['ccy'] = market['baseId']
        type, query = self.handle_market_type_and_params('fetchFundingHistory', market, params)
        if type == 'swap':
            request['instType'] = self.convert_to_instrument_type(type)
        if since is not None:
            request['begin'] = str(since)
        if limit is not None:
            request['limit'] = str(limit)

        # AccountBillsArchive has the same cost but supports three months of data
        response = self.privateGetAccountBillsArchive(self.extend(request, query))
        data = self.safe_value(response, 'data', [])
        result = []
        for i in range(0, len(data)):
            entry = data[i]
            timestamp = self.safe_integer(entry, 'ts')
            instId = self.safe_string(entry, 'instId')
            marketInner = self.safe_market(instId)
            currencyId = self.safe_string(entry, 'ccy')
            code = self.safe_currency_code(currencyId)
            result.append({
                'info': entry,
                'symbol': marketInner['symbol'],
                'code': code,
                'timestamp': timestamp,
                'datetime': self.iso8601(timestamp),
                'id': self.safe_string(entry, 'billId'),
                'amount': self.safe_number(entry, 'balChg'),
            })
        sorted = self.sort_by(result, 'timestamp')
        return self.filter_by_symbol_since_limit(sorted, symbol, since, limit)


def patch():
    ccxt.okex.fetch_positions = _fetch_positions
    ccxt.okex.fetch_funding_history = _fetch_funding_history
