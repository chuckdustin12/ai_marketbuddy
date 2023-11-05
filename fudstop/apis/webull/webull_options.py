import os
from dotenv import load_dotenv
# Assuming we have the array of ticker IDs as numpy arrays from the user's environment
import numpy as np
from fudstop.apis.webull.webull_helpers import convert_to_date
from aiohttp.client_exceptions import ContentTypeError
from fudstop.apis.webull.trade_models.options import WebullOptionsData, VolumeAnalysis
import aiohttp
import asyncio
import json
import asyncpg
load_dotenv()
GEX_KEY = os.environ.get('GEXBOT')

print(GEX_KEY)

from list_sets.ticker_lists import most_active_tickers
most_active_tickers = set(most_active_tickers)
from fudstop.apis.webull.webull_trading import WebullTrading
from datetime import datetime, timedelta

trading = WebullTrading()

class WebullOptions:
    def __init__(self, connection_string = None):
        self.pool = None
        self.connection_string = connection_string
        self.api_key = os.environ.get('YOUR_POLYGON_KEY')
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')
        self.headers = trading.headers

        if connection_string is not None:
            connection_string = os.environ.get('WEBULL_OPTIONS')
    async def connect(self):
        self.pool = await asyncpg.create_pool(
            dsn=self.connection_string, min_size=1, max_size=10
        )

    async def fetch(self, query, *args):
        """
        Fetch data from the database using the provided SQL query.

        :param query: The SQL query to execute.
        :param args: The arguments to pass to the SQL query.
        :return: The result of the query as a list of records.
        """
        async with self.pool.acquire() as conn:  # Acquire a connection from the pool
            # Execute the query with the provided arguments
            return await conn.fetch(query, *args)


    async def batch_insert_options(self, pairs):
        try:
            async with self.pool.acquire() as conn:  # Acquire a connection from the pool

                async with conn.transaction():  # Start a transaction
                    # Prepare the statement to insert data
                    insert_query = 'INSERT INTO webull_options (symbol, ticker_id, ticker) VALUES ($1, $2, $3)'
                    # Perform the batch insert
                    await conn.executemany(insert_query, pairs)
                    print("Batch insert completed.")
        except asyncpg.exceptions.UniqueViolationError:
            print(f'Duplicate found - skipping.')


    async def yield_batch_ids(self, ticker_symbol):
   
        async with self.pool.acquire() as conn:
            # We will fetch all derivative IDs associated with the ticker symbol
            derivative_ids = await conn.fetch(
                'SELECT ticker_id FROM webull_options WHERE ticker = $1',
                ticker_symbol
            )
            
            # Convert the records to a list of IDs
            derivative_id_list = [str(record['ticker_id']) for record in derivative_ids]

            # Yield batches of 55 IDs at a time as a comma-separated string
            for i in range(0, len(derivative_id_list), 55):
                yield ','.join(derivative_id_list[i:i+55])

    async def get_option_ids(self, ticker):
        ticker_id = await trading.get_ticker_id(ticker)
        params = {
            "tickerId": f"{ticker_id}",
            "count": -1,
            "direction": "all",
            "expireCycle": [1,
                3,
                2,
                4
            ],
            "type": 0,
            "quoteMultiplier": 100,
            "unSymbol": f"{ticker}"
        }
        data = json.dumps(params)
        url="https://quotes-gw.webullfintech.com/api/quote/option/strategy/list"

        # Headers you may need to include, like authentication tokens, etc.
        headers = trading.headers
        # The body of your POST request as a Python dictionary
        import pandas as pd
        # Make the POST request
        # Make the POST request
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.post(url, data=data) as resp:
                response_json = await resp.json()
                print(response_json)
                # Extract the 'expireDateList' from the response
                expireDateList = response_json.get('expireDateList')

                # Flatten the nested 'data' from each item in 'expireDateList'
            try:
                data_flat = [item for sublist in expireDateList if sublist and sublist.get('data') for item in sublist['data']]



                # Create a DataFrame from the flattened data
                df_cleaned = pd.DataFrame(data_flat)

                # Drop the 'askList' and 'bidList' columns if they exist
                df_cleaned.drop(columns=['askList', 'bidList'], errors='ignore', inplace=True)
                # Existing DataFrame columns
                df_columns = df_cleaned.columns

                # Original list of columns you want to convert to numeric
                numeric_cols = ['open', 'high', 'low', 'strikePrice', 'isStdSettle', 'quoteMultiplier', 'quoteLotSize']

                # Filter the list to include only columns that exist in the DataFrame
                existing_numeric_cols = [col for col in numeric_cols if col in df_columns]

                # Now apply the to_numeric conversion only to the existing columns
                df_cleaned[existing_numeric_cols] = df_cleaned[existing_numeric_cols].apply(pd.to_numeric, errors='coerce')

      
                print(df_cleaned)
                df_cleaned.to_csv('test.csv', index=False)


                # Load the data from the CSV file
                df = pd.read_csv('test.csv')

                # Extract 'tickerId' column values in batches of 55
                ticker_ids = df['tickerId'].unique()  # Assuming 'tickerId' is a column in your DataFrame
                symbol_list = df['symbol'].unique().tolist()
            # Pair up 'tickerId' and 'symbol'
                # Before you call batch_insert_options, make sure pairs contain the correct types
                pairs = [(str(symbol), int(ticker_id), str(ticker)) for ticker_id, symbol in zip(ticker_ids, symbol_list)]

                
                await self.batch_insert_options(pairs)
               
            except (ContentTypeError, TypeError):
                print(f'Error for {ticker}')
    async def get_option_id_for_symbol(self, ticker_symbol):
        async with self.pool.acquire() as conn:
            # Start a transaction
            async with conn.transaction():
                # Execute the query to get the option_id for a given ticker_symbol
                # This assumes 'symbol' column exists in 'options_data' table and 
                # is used to store the ticker symbol
                query = f'''
                    SELECT ticker_id FROM webull_options
                    WHERE ticker = '{ticker_symbol}';
                '''
                # Fetch the result
                result = await conn.fetch(query)
                # Return a list of option_ids or an empty list if none were found
                return [record['ticker_id'] for record in result]


    async def get_option_symbols_by_ticker_id(self, ticker_id):
        async with self.pool.acquire() as conn:
            # Start a transaction
            async with conn.transaction():
                # Execute the query to get all option_symbols for a given ticker_id
                query = '''
                    SELECT option_symbol FROM options_data
                    WHERE ticker_id = $1;
                '''
                # Fetch the result
                records = await conn.fetch(query, ticker_id)
                # Extract option_symbols from the records
                return [record['option_symbol'] for record in records]
    async def get_ticker_symbol_pairs(self):
        # Assume 'pool' is an instance variable pointing to a connection pool
        async with self.pool.acquire() as conn:
            # Start a transaction
            async with conn.transaction():
                # Create a cursor for iteration using 'cursor()' instead of 'execute()'
                async for record in conn.cursor('SELECT ticker_id, symbol FROM webull_options'):
                    yield (record['ticker_id'], record['symbol'])

    async def get_volume_analysis(self, ticker):
        ticker_id = await trading.get_ticker_id(ticker)
        params = {
            "tickerId": f"{ticker_id}",
            "count": -1,
            "direction": "all",
            "expireCycle": [1,
                3,
                2,
                4
            ],
            "type": 0,
            "quoteMultiplier": 100,
            "unSymbol": f"{ticker}"
        }
        data = json.dumps(params)
        url="https://quotes-gw.webullfintech.com/api/quote/option/strategy/list"

        # Headers you may need to include, like authentication tokens, etc.
        headers = trading.headers
        # The body of your POST request as a Python dictionary
        import pandas as pd
        # Make the POST request
        # Make the POST request
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.post(url, data=data) as resp:
                response_json = await resp.json()
          
                # Extract the 'expireDateList' from the response
                expireDateList = response_json.get('expireDateList')

                # Flatten the nested 'data' from each item in 'expireDateList'
            try:
                data_flat = [item for sublist in expireDateList if sublist and sublist.get('data') for item in sublist['data']]



                # Create a DataFrame from the flattened data
                df_cleaned = pd.DataFrame(data_flat)

                # Drop the 'askList' and 'bidList' columns if they exist
                df_cleaned.drop(columns=['askList', 'bidList'], errors='ignore', inplace=True)

                # Convert specified columns to numeric values, coercing errors to NaN
                numeric_cols = ['open', 'high', 'low', 'strikePrice', 'isStdSettle', 'quoteMultiplier', 'quoteLotSize']
                # Iterate through the list of numeric columns and check if they exist in df_cleaned
                existing_numeric_cols = [col for col in numeric_cols if col in df_cleaned.columns]

                # Now apply the conversion only on the columns that exist
                df_cleaned[existing_numeric_cols] = df_cleaned[existing_numeric_cols].apply(pd.to_numeric, errors='coerce')

                print(df_cleaned)
                df_cleaned.to_csv('test.csv', index=False)


                # Load the data from the CSV file
                df = pd.read_csv('test.csv')

                # Extract 'tickerId' column values in batches of 55
                ticker_ids = df['tickerId'].unique()  # Assuming 'tickerId' is a column in your DataFrame
                symbol_list = df['symbol'].unique().tolist()
            # Pair up 'tickerId' and 'symbol'
                pairs = list(zip(ticker_ids, symbol_list))

                
                # Split into batches of 55
                batches = [ticker_ids[i:i + 55] for i in range(0, len(ticker_ids), 55)]

                ticker_id_strings = [','.join(map(str, batch)) for batch in batches]







                for ticker_id_string in ticker_id_strings:
                    ticker_ids = ticker_id_string.split(',')
                    for deriv_id in ticker_ids:
                        all_data = []
                        volume_analysis_url = f"https://quotes-gw.webullfintech.com/api/statistic/option/queryVolumeAnalysis?count=200&tickerId={deriv_id}"
                        async with aiohttp.ClientSession(headers=headers) as session:
                            async with session.get(volume_analysis_url) as resp:
                                data = await resp.json()
                                all_data.append(data)


                   
                        return all_data
                        #df = pd.DataFrame(all_data)
                        #df.to_csv('all_options', index=False)
            except (ContentTypeError, TypeError):
                print(f'Error for {ticker}')

    async def get_option_ids_limited(self, sem, ticker):
        async with sem:
            # This will wait until the semaphore allows entry (i.e., under the limit)
            return await self.get_option_ids(ticker)

    async def harvest_options(self,most_active_tickers):
        # Set the maximum number of concurrent requests
        max_concurrent_requests = 5  # For example, limiting to 10 concurrent requests

        # Create a semaphore with your desired number of concurrent requests
        sem = asyncio.Semaphore(max_concurrent_requests)
        await self.connect()
        # Create tasks using the semaphore
        tasks = [self.get_option_ids_limited(sem, ticker) for ticker in most_active_tickers]

        # Run the tasks concurrently and wait for all to complete
        await asyncio.gather(*tasks)


    async def get_option_data_for_ticker(self, ticker):
        print(f"Starting processing for ticker: {ticker}")
        dataframes = []  # Initialize a list to collect DataFrames

        async for info in self.yield_batch_ids(ticker_symbol=ticker):
            print(f"Processing batch ID: {info} for ticker: {ticker}")
            url = f"https://quotes-gw.webullfintech.com/api/quote/option/quotes/queryBatch?derivativeIds={info}"
            async with aiohttp.ClientSession(headers=trading.headers) as session:
                async with session.get(url) as resp:
                    data = await resp.json()
                    if not data:  # If data is empty or None, break the loop
                        print(f"No more data for ticker: {ticker}. Moving to next.")
                        break
                    wb_data = WebullOptionsData(data)
                    df = wb_data.as_dataframe
                    df['ticker'] = ticker
                    await self.insert_dataframe_in_batches(df, 'options_data')
            
        print(f"Finished processing for ticker: {ticker}")

  



    async def insert_dataframe_in_batches(self, df, table_name, batch_size=55):
        """
        Insert a pandas DataFrame into a SQL table in batches.

        :param df: The pandas DataFrame to insert.
        :param table_name: The name of the target SQL table.
        :param batch_size: The size of the batches to insert.
        """
        # Make sure we have a connection
        
        df['expiry_date'] = df['expiry_date'].apply(convert_to_date)
        df['close'] = df['close'].astype(float)
        df['open'] = df['open'].astype(float)
        df['high'] = df['high'].astype(float)
        df['low'] = df['low'].astype(float)
        df['open_interest'] = df['open_interest'].replace({np.nan: None})
        async with self.pool.acquire() as conn:
            try:
                # Convert DataFrame to list of tuples
                records = df.to_records(index=False)
                columns = df.columns.tolist()
                values = [tuple(x) for x in records]

                # Create a prepared statement
                placeholders = ', '.join(f'${i+1}' for i in range(len(columns)))
                insert_query = f'INSERT INTO {table_name} ({", ".join(columns)}) VALUES ({placeholders})'

                # Insert data in batches
                for i in range(0, len(values), batch_size):
                    batch = values[i:i + batch_size]
                    await conn.executemany(insert_query, batch)

            except asyncpg.exceptions.UniqueViolationError:
                print(f'Duplicate found - skipping.')


    # Initialize an HTTP session
    async def fetch_volume_analysis(self, ticker_id):
        volume_analysis_url = f"https://quotes-gw.webullfintech.com/api/statistic/option/queryVolumeAnalysis?count=500&tickerId={ticker_id}"
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(volume_analysis_url) as resp:
                data = await resp.json()
                if data is not None:
                    return VolumeAnalysis(data)
    async def insert_trades_and_dates(self, data):
        data['date'] = datetime.strptime(data['date'], '%Y-%m-%d')
        # Convert numeric strings to appropriate numeric types
        data['price'] = float(data['price']) if data['price'] else 0.0
        data['buy'] = int(data['buy']) if data['buy'] else 0
        data['sell'] = int(data['sell']) if data['sell'] else 0
        data['volume'] = int(data['volume']) if data['volume'] else 0
        data['strike_price'] = float(data['strike_price']) if data['strike_price'] else 0.0
        # Convert expiry_date string to date object
        data['expiry_date'] = datetime.strptime(data['expiry_date'], '%Y-%m-%d')

                

        # Assuming 'data' is a dictionary containing the keys that match your table's columns.
        async with self.pool.acquire() as connection:
            await connection.execute('''
                INSERT INTO trades_and_dates (
                    date, price, buy, sell, volume, ratio,
                    option_symbol, symbol, strike_price, call_put,
                    expiry_date
                ) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            ''',
            data['date'], data['price'], data['buy'], data['sell'], data['volume'],
            data['ratio'], data['option_symbol'], data['symbol'], data['strike_price'],
            data['call_put'], data['expiry_date'])
    async def insert_volume_analysis_data(self, data: dict):
        # Assuming data contains the fields corresponding to your self.data_dict
        # You would extract these values and insert them into your volume_analysis table
        async with self.pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO volume_analysis (ticker_id, symbol, option_id, total_trades, total_volume, avg_price, buy_volume, sell_volume, neutral_volume, option_symbol, strike_price, call_put, expiry_date)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            """, data.get('ticker_id'), data.get('symbol', None), data.get('option_id', None), data.get('total_trades', None), data.get('total_volume', None), data.get('avg_price',None),data.get('buy_volume',None), data.get('sell_volume', None), data.get('neutral_volume', None), data.get('option_symbol', None), data.get('strike_price') , data.get('call_put'), data.get('expiry_date')        )
        

    async def store_options_data(self):
        await self.connect()
        semaphore = asyncio.Semaphore(5)  # Adjust the number to limit concurrent tasks

        async def limited_get_option_data_for_ticker(ticker):
            async with semaphore:
                return await self.get_option_data_for_ticker(ticker)

        tasks = [limited_get_option_data_for_ticker(i) for i in most_active_tickers]
        await asyncio.gather(*tasks)

    async def close_pool(self):
        await self.pool.close()
