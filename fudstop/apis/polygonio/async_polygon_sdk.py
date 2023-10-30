import os
from typing import List, Dict

from .models.aggregates import AggregatesData
from .models.ticker_news import TickerNews
from .models.company_info import CombinedCompanyResults
from .models.ticker_snapshot import StockSnapshot
from datetime import datetime, timedelta
import aiohttp
import asyncio
import json
import pandas as pd

from urllib.parse import urlencode

class Polygon:
    def __init__(self, connection_string=None):
        self.connection_string = connection_string
        self.api_key = os.environ.get('YOUR_POLYGON_KEY')
        self.today_str = datetime.now().strftime('%Y-%m-%d')


    async def fetch_endpoint(self, endpoint, params=None):
        # Build the query parameters
        if params:
            query_string = urlencode({k: v for k, v in params.items() if v is not None})
            if query_string:
                endpoint = f"{endpoint}?{query_string}&{self.api_key}"
               
        async with aiohttp.ClientSession() as session:
            async with session.get(url=endpoint, params=params) as response:
                return await response.json()  # or response.text() based on your needs
            



    
    async def aggregates(self, ticker, multiplier:str='1', timespan:str='day', date_from=None, date_to=None, limit:str=500, sort:str='desc'):
        """
        Fetches candlestick data for a ticker, option symbol, crypto/forex pair.
        
        Parameters:
        - ticker (str): The ticker symbol for which to fetch data.

        - timespan: The timespan to survey.

        TIMESPAN OPTIONS:

        >>> second
        >>> minute
        >>> hour
        >>> day
        >>> week
        >>> month
        >>> quarter
        >>> year



        >>> Multiplier: the number of timespans to survey.

        - date_from (str, optional): The starting date for the data fetch in yyyy-mm-dd format.
                                     Defaults to 30 days ago if not provided.
        - date_to (str, optional): The ending date for the data fetch in yyyy-mm-dd format.
                                   Defaults to today's date if not provided.

        - limit: the amount of candles to return. Defaults to 500



        Returns:
        - dict: Candlestick data for the given ticker and date range.

        Example:
        >>> await aggregates('AAPL', date_from='2023-09-01', date_to='2023-10-01')
        """
        
        if date_from is None:
            date_from = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        
        if date_to is None:
            date_to = datetime.now().strftime('%Y-%m-%d')
        
  
        params = {
            'adjusted': 'true',
            'sort': sort,
            'limit': limit,
            'apiKey': self.api_key  # API key included here
        }

        endpoint = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{date_from}/{date_to}"
        
        data = await self.fetch_endpoint(endpoint, params=params)
        data = AggregatesData(data)
        
        return data


    async def market_news(self, limit: str = '100'):
        """
        Arguments:

        >>> ticker: the ticker to query (optional)
        >>> limit: the number of news items to return (optional) | Max 1000

        """
        params = {
            'apiKey': self.api_key,
            'limit': limit
        }


        endpoint = "https://api.polygon.io/v2/reference/news"

        data = await self.fetch_endpoint(endpoint, params=params)
        data = TickerNews(data)

        return data
    

    async def company_info(self, ticker) -> CombinedCompanyResults:
        url = f"https://api.polygon.io/v3/reference/tickers/{ticker}?apiKey={self.api_key}"
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                data = await response.json()
                results_data = data['results']
                return CombinedCompanyResults(
                    ticker=results_data.get('ticker'),
                    name=results_data.get('name'),
                    market=results_data.get('market'),
                    locale=results_data.get('locale'),
                    primary_exchange=results_data.get('primary_exchange'),
                    type=results_data.get('type'),
                    active=results_data.get('active'),
                    currency_name=results_data.get('currency_name'),
                    cik=results_data.get('cik'),
                    composite_figi=results_data.get('composite_figi'),
                    share_class_figi=results_data.get('share_class_figi'),
                    market_cap=results_data.get('market_cap'),
                    phone_number=results_data.get('phone_number'),
                    description=results_data.get('description'),
                    sic_code=results_data.get('sic_code'),
                    sic_description=results_data.get('sic_description'),
                    ticker_root=results_data.get('ticker_root'),
                    homepage_url=results_data.get('homepage_url'),
                    total_employees=results_data.get('total_employees'),
                    list_date=results_data.get('list_date'),
                    share_class_shares_outstanding=results_data.get('share_class_shares_outstanding'),
                    weighted_shares_outstanding=results_data.get('weighted_shares_outstanding'),
                    round_lot=results_data.get('round_lot'),
                    address1=results_data.get('address', {}).get('address1'),
                    city=results_data.get('address', {}).get('city'),
                    state=results_data.get('address', {}).get('state'),
                    postal_code=results_data.get('address', {}).get('postal_code'),
                    logo_url=results_data.get('branding', {}).get('logo_url'),
                    icon_url=results_data.get('branding', {}).get('icon_url')
                )

    async def get_all_tickers(self, include_otc=False, save_all_tickers:bool=False) -> List[StockSnapshot]:
        """
        Fetches a list of all stock tickers available on Polygon.io.

        Arguments:
            >>> include_otc: optional - whether to include OTC securities or not

            >>> save_all_tickers: optional - saves all tickers as a list for later processing

        Returns:
            A list of StockSnapshot objects, each containing data for a single stock ticker.

        Usage:
            To fetch a list of all stock tickers available on Polygon.io, you can call:
            ```
            tickers = await sdk.get_all_tickers()
            print(f"Number of tickers found: {len(tickers)}")
            ```
        """
        url = f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?include_otc={include_otc}&apiKey={self.api_key}"
        params = {
            "apiKey": self.api_key,
        }
        print(url)
        async with aiohttp.ClientSession() as session:
            async with session.get(url, params=params) as response:
                response_data = await response.json()



                tickers = response_data['tickers']
                print(tickers)
                ticker_data = [StockSnapshot(ticker) for ticker in response_data['tickers'] if ticker is not None]

                if save_all_tickers:
                    # Extract tickers to a list
                    ticker_list = [ticker['ticker'] for ticker in tickers]
                    
                    # Write the tickers to a file
                    with open('list_sets/saved_tickers.py', 'w') as f:
                        f.write(str(ticker_list))
                return ticker_data

