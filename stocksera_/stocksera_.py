import os
import stocksera
import pandas as pd

from concurrent.futures import ThreadPoolExecutor, as_completed


class StockSera:
    def __init__(self):
        self.api_key = os.environ.get('YOUR_STOCKSERA_KEY')

        self.client = stocksera.Client(self.api_key)



    def borrowed_shares(self, ticker_or_tickers, concurrency=None):
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