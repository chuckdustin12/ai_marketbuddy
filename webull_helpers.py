async def parse_most_active(ticker_entry):

    all_parsed_data = []
    # Parsing 'ticker' attributes
    datas = ticker_entry.get('data', {})
    for data in datas:
        parsed_data = {}
        ticker_info = data.get('ticker', {})
        parsed_data['tickerId'] = ticker_info.get('tickerId')
        parsed_data['exchangeId'] = ticker_info.get('exchangeId')
        parsed_data['type'] = ticker_info.get('type')
        parsed_data['secType'] = ticker_info.get('secType')
        parsed_data['regionId'] = ticker_info.get('regionId')
        parsed_data['currencyId'] = ticker_info.get('currencyId')
        parsed_data['currencyCode'] = ticker_info.get('currencyCode')
        parsed_data['name'] = ticker_info.get('name')
        parsed_data['symbol'] = ticker_info.get('symbol')
        parsed_data['disSymbol'] = ticker_info.get('disSymbol')
        parsed_data['disExchangeCode'] = ticker_info.get('disExchangeCode')
        parsed_data['exchangeCode'] = ticker_info.get('exchangeCode')
        parsed_data['listStatus'] = ticker_info.get('listStatus')
        parsed_data['template'] = ticker_info.get('template')
        parsed_data['isPTP'] = ticker_info.get('isPTP')
        parsed_data['tradeTime'] = ticker_info.get('tradeTime')
        parsed_data['status'] = ticker_info.get('status')
        parsed_data['close'] = ticker_info.get('close')
        parsed_data['change'] = ticker_info.get('change')
        parsed_data['changeRatio'] = ticker_info.get('changeRatio')
        parsed_data['marketValue'] = ticker_info.get('marketValue')
        parsed_data['volume'] = ticker_info.get('volume')
        parsed_data['turnoverRate'] = ticker_info.get('turnoverRate')
        parsed_data['regionName'] = ticker_info.get('regionName')
        parsed_data['regionIsoCode'] = ticker_info.get('regionIsoCode')
        parsed_data['peTtm'] = ticker_info.get('peTtm')
        parsed_data['timeZone'] = ticker_info.get('timeZone')
        parsed_data['preClose'] = ticker_info.get('preClose')
        parsed_data['fiftyTwoWkHigh'] = ticker_info.get('fiftyTwoWkHigh')
        parsed_data['fiftyTwoWkLow'] = ticker_info.get('fiftyTwoWkLow')
        parsed_data['open'] = ticker_info.get('open')
        parsed_data['high'] = ticker_info.get('high')
        parsed_data['low'] = ticker_info.get('low')
        parsed_data['vibrateRatio'] = ticker_info.get('vibrateRatio')
        
        # Parsing 'values' attributes
        values_info = ticker_entry.get('values', {})
        parsed_data['rankValue'] = values_info.get('rankValue')
        parsed_data['isRatio'] = values_info.get('isRatio')
        all_parsed_data.append(parsed_data)
    return all_parsed_data



# Creating a function to parse each attribute of the data_entry and return it as a dictionary
def parse_total_top_options(data_entry):
    all_parsed_data = []
    
    for data in data_entry:
        parsed_data = {}
        ticker_info = data.get('ticker', {})
        for key, value in ticker_info.items():
            parsed_data[f'ticker_{key}'] = value
    
        # Parsing 'values' attributes
        values_info = data.get('values', {})
        for key, value in values_info.items():
            parsed_data[f'values_{key}'] = value
        
        all_parsed_data.append(parsed_data)
    return all_parsed_data



def parse_contract_top_options(data_entry):
    all_parsed_data = []
    for data in data_entry:
        parsed_data = {}
        # Parsing 'belongTicker' attributes
        belong_ticker_info = data.get('belongTicker', {})
        
        for key, value in belong_ticker_info.items():
            parsed_data[f'belongTicker_{key}'] = value
        
        # Parsing 'derivative' attributes
        derivative_info = data.get('derivative', {})
        for key, value in derivative_info.items():
            parsed_data[f'derivative_{key}'] = value
        
        # Parsing 'values' attributes
        values_info = data.get('values', {})
        for key, value in values_info.items():
            parsed_data[f'values_{key}'] = value

        all_parsed_data.append(parsed_data)

    return all_parsed_data

