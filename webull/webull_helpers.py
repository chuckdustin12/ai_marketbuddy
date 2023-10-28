from datetime import datetime, timedelta
import pytz

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
async def parse_total_top_options(data_entry):
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



async def parse_contract_top_options(data_entry):
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



# Creating a function to parse each attribute of the data_entry and return it as a dictionary
async def parse_ticker_values(data_entry):
    all_parsed_data = []
    data_entry = data_entry.get('data', {})
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


def parse_forex(ticker_list):
    parsed_data_list = []
    
    for ticker_entry in ticker_list:
        parsed_data = {}
        
        parsed_data['tickerId'] = ticker_entry.get('tickerId')
        parsed_data['exchangeId'] = ticker_entry.get('exchangeId')
        parsed_data['type'] = ticker_entry.get('type')
        parsed_data['name'] = ticker_entry.get('name')
        parsed_data['symbol'] = ticker_entry.get('symbol')
        parsed_data['disSymbol'] = ticker_entry.get('disSymbol')
        parsed_data['disExchangeCode'] = ticker_entry.get('disExchangeCode')
        parsed_data['exchangeCode'] = ticker_entry.get('exchangeCode')
        parsed_data['listStatus'] = ticker_entry.get('listStatus')
        parsed_data['template'] = ticker_entry.get('template')
        parsed_data['futuresSupport'] = ticker_entry.get('futuresSupport')
        parsed_data['tradeTime'] = ticker_entry.get('tradeTime')
        parsed_data['status'] = ticker_entry.get('status')
        parsed_data['close'] = ticker_entry.get('close')
        parsed_data['change'] = ticker_entry.get('change')
        parsed_data['changeRatio'] = ticker_entry.get('changeRatio')
        parsed_data['marketValue'] = ticker_entry.get('marketValue')
        
        parsed_data_list.append(parsed_data)
    
    return parsed_data_list


async def parse_etfs(response):
    flattened_data = []
    
    for tab in response.get('tabs', []):
        tab_info = {
            'id': tab.get('id'),
            'name': tab.get('name'),
            'comment': tab.get('comment'),
            'queryId': tab.get('queryId'),
            'upNum': tab.get('upNum'),
            'dowoNum': tab.get('dowoNum'),
            'flatNum': tab.get('flatNum'),
        }
        
        for ticker in tab.get('tickerTupleList', []):
            # Merge the 'tab' info and the 'ticker' info into a single dictionary
            merged_info = {**tab_info, **ticker}
            flattened_data.append(merged_info)

    return flattened_data



# Function to convert Unix timestamps in seconds to Eastern Time in milliseconds
def convert_seconds_to_ms_eastern_time(seconds_timestamp):
    et_offset = -5 * 3600  # Eastern Standard Time (EST) offset in seconds
    utc_time = datetime.utcfromtimestamp(int(seconds_timestamp))
    eastern_time = utc_time + timedelta(seconds=et_offset)
    eastern_time_ms = int(eastern_time.timestamp() * 1000)  # Convert to milliseconds
    return eastern_time_ms


def convert_unix_to_eastern(unix_timestamp):
    eastern_time = datetime.fromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')
    return eastern_time


def format_date(input_str):
    # Parse the input string as a datetime object
    input_datetime = datetime.fromisoformat(input_str.replace("Z", "+00:00"))

    # Convert the datetime object to Eastern Time
    utc_timezone = pytz.timezone("UTC")
    eastern_timezone = pytz.timezone("US/Eastern")
    input_datetime = input_datetime.astimezone(utc_timezone)
    eastern_datetime = input_datetime.astimezone(eastern_timezone)

    # Format the output string
    output_str = eastern_datetime.strftime("%Y-%m-%d at %I:%M%p %Z")
    return output_str