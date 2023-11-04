import os
import sys
import pandas as pd
from pathlib import Path
import aiohttp
from dotenv import load_dotenv
load_dotenv()
# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[1])
if project_dir not in sys.path:
    sys.path.append(project_dir)
from urllib.parse import urlencode
from datetime import datetime, timedelta
from webull.webull_trading import WebullTrading
from .models.option_models.universal_snapshot import UniversalOptionSnapshot
from .polygon_helpers import flatten_nested_dict, flatten_dict

trading = WebullTrading()

class PolygonOptions:
    def __init__(self, connection_string=None):
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
        self.one_year_from_now = (datetime.now() + timedelta(days=365)).strftime('%Y-%m-%d')
        self.one_year_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')



    async def paginate_concurrent(self, url, as_dataframe=False, concurrency=25):
        all_results = []

        
        async with aiohttp.ClientSession() as session:
            pages_to_fetch = [url]
            
            while pages_to_fetch:
                tasks = []
                
                for _ in range(min(concurrency, len(pages_to_fetch))):
                    next_url = pages_to_fetch.pop(0)
                    tasks.append(self.fetch_page(next_url))
                    
                results = await asyncio.gather(*tasks)
                if results is not None:
                    for data in results:
                        if data is not None:
                            if "results" in data:
                                all_results.extend(data["results"])
                                
                            next_url = data.get("next_url")
                            if next_url:
                                next_url += f'&{urlencode({"apiKey": f"{self.api_key}"})}'
                                pages_to_fetch.append(next_url)
                        else:
                            break
        if as_dataframe:
            import pandas as pd
            return pd.DataFrame(all_results)
        else:
            return all_results
        



    async def fetch_page(self,url):
        try:
            async with aiohttp.ClientSession() as session, session.get(url) as response:
                response.raise_for_status()
                return await response.json()
        except TimeoutError:
            print(f"Timeout when accessing {url}")
        except aiohttp.ClientResponseError as http_err:
            print(f"HTTP error occurred: {http_err}")
        except Exception as err:
            print(f"An error occurred: {err}")

    async def fetch_endpoint(self, endpoint, params=None):
        """
        Uses endpoint / parameter combinations to fetch the polygon API

        dynamically.
        
        """
        # Build the query parameters
        filtered_params = {k: v for k, v in params.items() if v is not None} if params else {}
        
        if filtered_params:
            query_string = urlencode(filtered_params)
            if query_string:
                endpoint = f"{endpoint}?{query_string}&{self.api_key}"
                
        async with aiohttp.ClientSession() as session:
            async with session.get(url=endpoint, params=filtered_params) as response:
                response_data = await response.json()
                if 'next_url' in response_data:
                    # If "next_url" exists - the function auto paginates
                    return await self.paginate_concurrent(endpoint,as_dataframe=True,concurrency=40)
                return response_data  # or response.text() based on your needs
                
    async def get_price(self, ticker:str):
        """
        Fetches price from Webull API to use for option queries

        Arguments:

        >>> ticker: required - the ticker to survey
        """

        if ticker.startswith('I:'):
            ticker = ticker.replace('I:', '')
        datas = await trading.stock_quote(ticker)

        price = datas.web_stock_close

        return price

    async def get_option_contracts(self, ticker:str, 
                                   strike_price_greater_than:str=None, 
                                   strike_price_less_than:str=None, 
                                   expiry_date_greater_than:str=None, 
                                   expiry_date_less_than:str=None, limit:str='250'):
        """
        Returns options for a specified ticker.

        Arguments:

        >>> ticker: the ticker to query (required)

        >>> strike_price_greater_than: the minimum strike to be returned (optional)      


        >>> strike_price_less_than: the maximum strike to be returned (optional) 


        >>> expiry_greater_than: the minimum expiry date to be returned (optional)


        >>> expiry_less_than: the maximum expiry date to be returned (optional)

        >>> limit: the amount of contracts to be returned (max 250 per request)

        
        """


        endpoint = f"https://api.polygon.io/v3/snapshot/options/{ticker}"

        
        params = {
            "limit": limit,
            "apiKey": self.api_key
        }

        df = await self.fetch_endpoint(endpoint, params)
      
        # Columns to normalize
        columns_to_normalize = ['day', 'underlying_asset', 'details', 'greeks', 'last_quote', 'last_trade']

        # Apply the helper function to each row and each specified column
        for column in columns_to_normalize:
            df = df.apply(lambda row: flatten_nested_dict(row, column), axis=1)

        return df

    async def get_strike_thresholds(self, ticker:str, price):
        indices_list = ["SPX", "SPXW", "NDX", "VIX", "VVIX"]
        if price is not None and ticker in indices_list:
            lower_strike = round(float(price) * 0.99)
            upper_strike = round(float(price) * 1.01)
            return lower_strike, upper_strike
        else:
            lower_strike = round(float(price) * 0.95)
            upper_strike = round(float(price) * 1.05)
            return lower_strike, upper_strike
    
    async def get_near_the_money_single(self, ticker: str, exp_greater_than:str=None, exp_less_than:str=None):
        """
        Gets options near the money for a ticker
        """

        if exp_greater_than is None:
            exp_greater_than = self.today

        if exp_less_than is None:
            exp_less_than = self.eight_days_from_now



        price = await self.get_price(ticker)
  
        if price is not None:
            upper_strike, lower_strike = await self.get_strike_thresholds(ticker, price)
      
            async with aiohttp.ClientSession() as session:
                url = f"https://api.polygon.io/v3/snapshot/options/{ticker}?strike_price.lte={lower_strike}&strike_price.gte={upper_strike}&expiration_date.gte={exp_greater_than}&expiration_date.lte={exp_less_than}&limit=250&apiKey={self.api_key}"
         
                async with session.get(url) as resp:
                    r = await resp.json()
                    results = r['results'] if 'results' in r else None
                    if results is None:
                        return
                    else:
                        results = UniversalOptionSnapshot(results)
                       
                        tickers = results.ticker
                        if ticker is not None:
                            atm_tickers = ','.join(tickers)
                            return atm_tickers
                        else:
                            return None

    async def get_skew(self, ticker:str):
        """Gets the IV skew across all expiration dates along with the strikes above and below the lowest IV.
        
        Arguments:
        >>> ticker: required - the ticker to survey
        """

        options = await self.get_option_contracts(ticker, expiry_date_less_than='2023-11-03')

        # Sort by 'expiration_date' and 'implied_volatility'
        options_sorted = options.sort_values(by=['expiration_date', 'implied_volatility'])

        results = []

        # For each expiration date, get the lowest IV strike and the ones above and below it
        for expiration, group in options_sorted.groupby('expiration_date'):
            min_iv_idx = group['implied_volatility'].idxmin()
            
            # Get rows for lowest IV and the ones immediately above and below it
            rows = group.loc[[min_iv_idx - 1, min_iv_idx, min_iv_idx + 1], :]
            
            results.append(rows)

        # Concatenate all the results
        result_df = pd.concat(results)

     
        return result_df


    async def get_universal_snapshot(self, ticker, retries=3): #✅
        """Fetches the Polygon.io universal snapshot API endpoint"""
        timeout = aiohttp.ClientTimeout(total=10)  # 10 seconds timeout for the request
        
        for retry in range(retries):
        # async with sema:
            url = f"https://api.polygon.io/v3/snapshot?ticker.any_of={ticker}&apiKey={self.api_key}&limit=250"

            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                try:
                    async with session.get(url) as resp:
                        data = await resp.json()
                        results = data.get('results', None)
        
                        if results is not None:
                            flattened_results = [flatten_dict(result) for result in results]
                            return flattened_results
                            
                except aiohttp.ClientConnectorError:
                    print("ClientConnectorError occurred. Retrying...")
                    continue
                
                except aiohttp.ContentTypeError as e:
                    print(f"ContentTypeError occurred: {e}")  # Consider logging this
                    continue
                
                except Exception as e:
                    print(f"An unexpected error occurred: {e}")  # Consider logging this
                    continue




import asyncio
async def test():
    opts = PolygonOptions()
    while True:
        data = await opts.get_near_the_money_single(ticker='SPY', exp_less_than='2023-11-03')
        

        
        snapshot = await opts.get_universal_snapshot(data)
        df = pd.DataFrame(snapshot)
      
        print()
        # Ensure the columns you want to use are in the DataFrame
        columns_to_use = ['implied_volatility', 'details.strike_price', 'details.expiration_date', 'underlying_asset.price']

        # Check if all desired columns are present in the DataFrame
        missing_columns = [col for col in columns_to_use if col not in df.columns]
        if missing_columns:
            print(f"Warning: Missing columns {missing_columns} in the dataframe. These will be skipped.")
            columns_to_use = [col for col in columns_to_use if col in df.columns]

        # If none of the columns are present, we shouldn't proceed
        if not columns_to_use:
            raise ValueError("None of the specified columns are present in the dataframe.")

        # If there are valid columns to use, proceed with sorting and selecting the top row
        if columns_to_use:
            # Sort by 'implied_volatility' in descending order and select the top row
            top_row = df.sort_values('implied_volatility', ascending=False).iloc[0]

            # Create a list with only the selected columns' values
            selected_values = [top_row[col] for col in columns_to_use]
            df = [selected_values]
            df = df[0]
            print(f"IV: {df[0]} | Skew: {df[1]} | Price: {df[2]}")
