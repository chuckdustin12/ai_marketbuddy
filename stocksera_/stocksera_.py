import os
import stocksera
import pandas as pd

from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed
from .daily_treasury import DailyTreasuryData
from .sec_filings import SECFilingsData
from .ftds import FailureToDeliverData
from .highest_shorted import HighestShortedData
from .short_volume import ShortVolumeData
from .inflation import InflationData
from .borrowed_shares import BorrowedSharesData
from .jobless_claims import JoblessClaimsData
from .jim_cramer import JimCramerData
from .retail_sales import RetailSalesData
from .reverse_repo import ReverseRepoData


class StockSera:
    def __init__(self):
        self.api_key = os.environ.get('YOUR_STOCKSERA_KEY')

        self.client = stocksera.Client(self.api_key)



    def borrowed_shares(self, ticker_or_tickers, concurrency=None) -> List[BorrowedSharesData]:
        def fetch_data(ticker):
            data = self.client.borrowed_shares(ticker)
            df = pd.DataFrame(data)
            df['ticker'] = df['ticker'].astype(str)
            df['fee'] = df['fee'].astype(float)
            df['available'] = df['available'].astype(int)
            df['date_updated'] = pd.to_datetime(df['date_updated'])
            df.columns = df.columns.str.lower()
            return df

        if concurrency and isinstance(ticker_or_tickers, list):
            data_frames = []
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = {executor.submit(fetch_data, ticker): ticker for ticker in ticker_or_tickers}

                for future in as_completed(futures):
                    ticker = futures[future]
                    try:
                        data = future.result()
                    except Exception as exc:
                        print(f"{ticker} generated an exception: {exc}")
                    else:
                        print(data)
                        data_frames.append(data)

            combined_df = pd.concat(data_frames, ignore_index=True)
            return combined_df

        else:
            data = self.client.borrowed_shares(ticker_or_tickers)
            df = pd.DataFrame(data)
            df['ticker'] = df['ticker'].astype(str)
            df['fee'] = df['fee'].astype(float)
            df['available'] = df['available'].astype(int)
            df['date_updated'] = pd.to_datetime(df['date_updated'])
            df.columns = df.columns.str.lower()
            df.to_csv(f'data/stocksera/borrowed_shares_{ticker_or_tickers}.csv', index=False)  # Corrected the variable here
            return df
        

    def short_volume(self, ticker_or_tickers, date_from='2019-09-17', date_to='2023-10-27', concurrency=None) -> List[ShortVolumeData]:
        """
        REQUIRED:

        >>> ticker



        OPTIONAL: 
        >>> date_from: the date to start surveying
        >>> date_to: the date to stop surveying

        >>> concurrency: Run this command on a list of tickers rather than a single ticker.
        """
        
        def fetch_data(ticker):
            data = self.client.short_volume(ticker, date_from, date_to)
            df = pd.DataFrame(data)
            df.columns = df.columns.str.lower()
            df = df.rename({'short vol': 'short_vol', 'short exempt vol': 'short_exempt_vol', 'total vol': 'total_vol', '% shorted': 'percent_shorted'})
            df['date'] = pd.to_datetime(df['date'])
            df['ticker'] = ticker
            return df
        
        if concurrency and isinstance(ticker_or_tickers, list):
            data_frames = []
            
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = {executor.submit(fetch_data, ticker): ticker for ticker in ticker_or_tickers}
                
                for future in as_completed(futures):
                    ticker = futures[future]
                    try:
                        data = future.result()
                    except Exception as exc:
                        print(f"{ticker} generated an exception: {exc}")
                    else:
                        data_frames.append(data)

            if data_frames:  # Check if list is not empty
                combined_df = pd.concat(data_frames, ignore_index=True)

                combined_df.columns = combined_df.columns.str.lower()
                print(combined_df.columns)
                combined_df = combined_df.rename({'short vol': 'short_vol', 'short exempt vol': 'short_exempt_vol', 'total vol': 'total_vol', '% shorted': 'percent_shorted'})
                combined_df['date'] = pd.to_datetime(combined_df['date'])

                return combined_df
            else:
                print("No data to concatenate.")
                return None
                
        else:
            return fetch_data(ticker_or_tickers)

    def daily_treasury(self, days: str = '100') -> List[DailyTreasuryData]:
        """
        Arguments:

        OPTIONAL:

        >>> days: number of days to survey
        
        """
        data = self.client.daily_treasury(days)
        df = pd.DataFrame(data)

        # Rename columns to standard underscore style
        df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace("%", "percent").str.replace("?", "question")

        all_treasury_data = []
        for index, row in df.iterrows():
            daily_treasury_data = DailyTreasuryData(
                date=row['date'],
                close_balance=row['close_balance'],
                open_balance=row['open_balance'],
                amount_change=row['amount_change'],
                percent_change=row['percent_change'],
                moving_avg=row['moving_avg']
            )

            all_treasury_data.append(daily_treasury_data)

        return all_treasury_data
    


    def failure_to_deliver(self, ticker_or_tickers, date_from='2023-10-28', date_to='2024-01-01', concurrency=None) -> List[FailureToDeliverData]:
        """
        Arguments:

        required:

        >>> ticker or a list of tickers


        OPTIONAL:

        >>> concurrency: add concurrency when passing in a list
        
        """
        def fetch_data(ticker):
            data = self.client.ftd(ticker, date_from, date_to)
            df = pd.DataFrame(data)
            df['ticker'] = ticker  # Assuming the API doesn't return the ticker, add it manually
            df['date'] = pd.to_datetime(df['date'])
            df['ftd'] = df['ftd'].astype(float)
            df['price'] = df['price'].astype(float)
            df['ftd_x_$'] = df['ftd_x_$'].astype(float)
            df['t+35_date'] = pd.to_datetime(df['t+35_date'])
            df.columns = df.columns.str.lower()
            return df

        if concurrency and isinstance(ticker_or_tickers, list):
            data_frames = []
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = {executor.submit(fetch_data, ticker): ticker for ticker in ticker_or_tickers}

                for future in as_completed(futures):
                    ticker = futures[future]
                    try:
                        data = future.result()
                    except Exception as exc:
                        print(f"{ticker} generated an exception: {exc}")
                    else:
                        data_frames.append(data)

            combined_df = pd.concat(data_frames, ignore_index=True)
            return combined_df

        else:
            data = self.client.ftd(ticker_or_tickers, date_from, date_to)
            df = pd.DataFrame(data)
            df['ticker'] = ticker_or_tickers  # Assuming the API doesn't return the ticker, add it manually
            df['date'] = pd.to_datetime(df['date'])
            df['ftd'] = df['ftd'].astype(float)
            df['price'] = df['price'].astype(float)
            df['ftd_x_$'] = df['ftd_x_$'].astype(float)
            df['t+35_date'] = pd.to_datetime(df['t+35_date'])
            df.columns = df.columns.str.lower()
            df.to_csv(f'data/stocksera/failure_to_deliver_{ticker_or_tickers}.csv', index=False)
            return df

    def house_trades(self):
        """
        Arguments:

        >>> ...
        """

        data = self.client.house()
        print(data)
        house = data['house']
     
        districts_available = data['districts_available']
        tickers_available = data['tickers_available']

        districts_df = pd.DataFrame(districts_available)
        tickers_df = pd.DataFrame(tickers_available)



        print(tickers_df.columns)
        print(districts_df.columns)



    def highest_shorted(self) -> List[HighestShortedData]:
        """
        Arguments

        >>> ...
        """

        data = self.client.short_interest()

        df = pd.DataFrame(data)
        df.columns = df.columns.str.lower()
      
        df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace("%", "percent_")
        short_interest = []
        for i, row in df.iterrows():
            short_interest_data = HighestShortedData(rank=row['rank'], 
                                                     ticker=row['ticker'], 
                                                     date=row['date'], 
                                                     short_interest=row['short_interest'], 
                                                     average_volume= row['average_volume'],
                                                     days_to_cover=row['days_to_cover'],
                                                     percent_float_short=row['percent_float_short'])

            short_interest.append(short_interest_data)

        return short_interest
    def inflation(self):
        """
        Arguments:
        >>> ...
        """

        data = self.client.inflation()

        df = pd.DataFrame(data)

        print(df.columns)


    def jobless_claims(self, days:str='100'):
        """
        Arguments:

        >>> ...
        """


        data = self.client.jobless_claims(days)
        df = pd.DataFrame(data)
        print(df.columns)



    
    def insider_trading(self):
        """
        Arguments:

        >>> ...
        """

        data = self.client.insider_trading()

        df = pd.DataFrame(data)
        print(df.columns)


  
    def jim_cramer(self):
        """
        Arguments:
        >>> ...
        """

        data = self.client.jim_cramer()

        df = pd.DataFrame(data)

        print(df.columns)

   


    def low_float(self):
        """
        Arguments:

        >>> ...
        """

        data = self.client.low_float()

        df = pd.DataFrame(data)
        print(df.columns)



    def sec_filings(self, ticker_or_tickers, concurrency=None):
        def fetch_data(ticker):
            data = self.client.sec_filings(ticker)
            df = pd.DataFrame(data)
            df['ticker'] = ticker  # Assuming the API doesn't return the ticker, add it manually
            df['filling'] = df['filling'].astype(str)
            df['description'] = df['description'].astype(str)
            df['filling_date'] = pd.to_datetime(df['filling_date'])
            df.columns = df.columns.str.lower()
            return df

        if concurrency and isinstance(ticker_or_tickers, list):
            data_frames = []
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = {executor.submit(fetch_data, ticker): ticker for ticker in ticker_or_tickers}

                for future in as_completed(futures):
                    ticker = futures[future]
                    try:
                        data = future.result()
                    except Exception as exc:
                        print(f"{ticker} generated an exception: {exc}")
                    else:
                        data_frames.append(data)

            combined_df = pd.concat(data_frames, ignore_index=True)
            return combined_df

        else:
            data = self.client.sec_filings(ticker_or_tickers)
            df = pd.DataFrame(data)
            df['ticker'] = ticker_or_tickers  # Assuming the API doesn't return the ticker, add it manually
            df['filling'] = df['filling'].astype(str)
            df['description'] = df['description'].astype(str)
            df['filling_date'] = pd.to_datetime(df['filling_date'])
            df.columns = df.columns.str.lower()
            df.to_csv(f'data/stocksera/sec_filings_{ticker_or_tickers}.csv', index=False)
            return df



 
    def news_sentiment(self, ticker_or_tickers, concurrency=None):
        def fetch_data(ticker):
            data = self.client.news_sentiment(ticker)
            df = pd.DataFrame(data)
            df['ticker'] = ticker  # Assuming the API doesn't return the ticker, add it manually
            df['date'] = pd.to_datetime(df['date'])
            df['title'] = df['title'].astype(str)
            df['link'] = df['link'].astype(str)
            df['sentiment'] = df['sentiment'].astype(str)
            df.columns = df.columns.str.lower()
            return df

        if concurrency and isinstance(ticker_or_tickers, list):
            data_frames = []
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = {executor.submit(fetch_data, ticker): ticker for ticker in ticker_or_tickers}

                for future in as_completed(futures):
                    ticker = futures[future]
                    try:
                        data = future.result()
                    except Exception as exc:
                        print(f"{ticker} generated an exception: {exc}")
                    else:
                        data_frames.append(data)

            combined_df = pd.concat(data_frames, ignore_index=True)
            return combined_df

        else:
            data = self.client.news_sentiment(ticker_or_tickers)
            df = pd.DataFrame(data)
            df['ticker'] = ticker_or_tickers  # Assuming the API doesn't return the ticker, add it manually
            df['date'] = pd.to_datetime(df['date'])
            df['title'] = df['title'].astype(str)
            df['link'] = df['link'].astype(str)
            df['sentiment'] = df['sentiment'].astype(str)
            df.columns = df.columns.str.lower()
            df.to_csv(f'data/stocksera/news_sentiment_{ticker_or_tickers}.csv', index=False)
            return df
   


    def market_news(self):
        """
        Arguments:

        >>> ...
        
        """

        data = self.client.market_news()


        df = pd.DataFrame(data)

        print(df.columns)


    def retail_sales(self, days:str='100'):

        """
        Arguments:

        >>> ...
        """

        data = self.client.retail_sales(days)

        df = pd.DataFrame(data)

        print(df.columns)



    def reverse_repo(self, days:str='100'):
        """
        Arguments:
        >>> ...
        """

        data = self.client.reverse_repo(days)


        df = pd.DataFrame(data)


        print(df.columns)