import aiohttp
import asyncio
import pandas as pd
from pytz import timezone
from webull_helpers import parse_most_active, parse_total_top_options, parse_contract_top_options, parse_ticker_values

class Webull:
    def __init__(self):
        self.most_active_types = ['rvol10d', 'turnoverRatio', 'volume', 'range']
        self.top_option_types = ['totalVolume', 'totalPosition', 'volume', 'position', 'impVol', 'turnover']
        self.top_gainer_loser_types = ['afterMarket', 'preMarket', '5min', '1d', '5d', '1m', '3m', '52w']
        self.timeframes = ['m1','m5', 'm10', 'm15', 'm20', 'm30', 'm60', 'm120', 'm240', 'd1']
        self.headers = {
        "App": "global",
        "App-Group": "broker",
        "Appid": "wb_web_app",
        "Device-Type": "Web",
        "Did": "8tb5au1228olpj2jss5vittmtk7pcvf6",
        "Hl": "en",
        "Locale": "eng",
        "Os": "web",
        "Osv": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Ph": "Windows Chrome",
        "Platform": "web",
        "Referer": "https://app.webull.com/",
        "Reqid": "a9d8d422e0e84041a035fb2389f18dae",
        "Sec-Ch-Ua": "\"Chromium\";v=\"118\", \"Google Chrome\";v=\"118\", \"Not=A?Brand\";v=\"99\"",
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": "\"Windows\"",
        "T_time": "1698276695206",
        "Tz": "America/Los_Angeles",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Ver": "3.40.11",
        "X-S": "49ef20ad66d1e24a83ff8b2015bc13c6d133285c5665dbbe4aa6032572749931",
        "X-Sv": "xodp2vg9"
    }

    async def fetch_endpoint(self, endpoint):
        async with aiohttp.ClientSession(headers=self.headers) as session:
            async with session.get(endpoint) as resp:
                return await resp.json()

    async def get_ticker_id(self, symbol):
        """Converts ticker name to ticker ID to be passed to other API endpoints from Webull."""
        endpoint =f"https://quotes-gw.webullfintech.com/api/search/pc/tickers?keyword={symbol}&pageIndex=1&pageSize=1"

        
        data =  await self.fetch_endpoint(endpoint)
        datas = data['data']
        tickerID = datas[0]['tickerId']
        return tickerID
    

    async def get_top_options(self, rank_type:str='volume', as_dataframe:bool = True):
        """Rank Types:
        
        >>> totalVolume (total contract volume for a ticker)
        >>> totalPosition (total open interest for a ticker)
        >>> volume (volume by contract)
        >>> position (open interest by contract)
        >>> posIncrease (open interest increase)
        >>> posDecrease (open interest decrease)
        >>> impVol 
        >>> turnover

        DEFAULT: volume"""
        endpoint = f"https://quotes-gw.webullfintech.com/api/wlas/option/rank/list?regionId=6&rankType={rank_type}&pageIndex=1&pageSize=350"
        datas = await self.fetch_endpoint(endpoint)
        data = datas['data']
        if 'total' in rank_type:
            total_data = await parse_total_top_options(data)
        else:
            total_data = await parse_contract_top_options(data)
        if as_dataframe == False:
            return total_data
        df = pd.DataFrame(total_data)
        df['rank_type'] = rank_type
        df.to_csv(f'data/top_options/top_options_{rank_type}.csv', index=False)
        return df


    async def get_most_active(self, rank_type:str='rvol10d', as_dataframe:bool=True):
        """Rank types: 
        
        >>> volume
        >>> range
        >>> turnoverRatio
        >>> rvol10d
        
        """
        endpoint = f"https://quotes-gw.webullfintech.com/api/wlas/ranking/topActive?regionId=6&rankType={rank_type}&pageIndex=1&pageSize=350"
        datas = await self.fetch_endpoint(endpoint)
        parsed_data = await parse_most_active(datas)
        if as_dataframe == False:
            return parsed_data
        df = pd.DataFrame(parsed_data)
        df['rank_type'] = rank_type
        df.to_csv(f'data/top_active/top_active_{rank_type}.csv', index=False)
        return df

    async def get_bars(self, symbol, timeframe:str='m1'):
        """
        Timeframes:
        
        >>> m1: 1 minute
        >>> m5: 5 minute
        >>> m10: 10 minute
        >>> m15: 15 minute
        >>> m20: 20 minute
        >>> m30: 30 minute
        >>> m60: 1 hour
        >>> m120: 2 hour
        >>> m240: 4 hour
        
        """
        tickerid = await self.get_ticker_id(symbol)
        endpoint = f"https://quotes-gw.webullfintech.com/api/quote/charts/query?tickerIds={tickerid}&type={timeframe}&count=1000"
        datas =  await self.fetch_endpoint(endpoint)
        if datas is not None:
            data = datas[0]['data']
            # Create empty lists for each column
            timestamps = []
            column2 = []
            column3 = []
            column4 = []
            column5 = []
            column6 = []

            # Split each line and append values to respective lists
            for line in data:
                parts = line.split(',')
                timestamps.append(parts[0])
                column2.append(parts[1])
                column3.append(parts[2])
                column4.append(parts[3])
                column5.append(parts[4])
                column6.append(parts[5])

  

            df = pd.DataFrame({
                'Timestamp': timestamps,
                'Open': column2,
                'Low': column3,
                'High': column4,
                'Close': column5,
                'Vwap': column6
            })

            # Convert the 'Timestamp' column to integers before converting to datetime
            df['Timestamp'] = df['Timestamp'].astype(int)

            # Then convert to datetime
            df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s')


            # Convert to Eastern Time
            eastern = timezone('US/Eastern')
            df['Timestamp'] = df['Timestamp'].dt.tz_localize('UTC').dt.tz_convert(eastern)

            # Remove the timezone information
            df['Timestamp'] = df['Timestamp'].dt.tz_localize(None)
            df['Timeframe'] = timeframe
            df['Ticker'] = symbol
            return df
        
    async def get_all_rank_types(self,types):
        """
        types:

        >>> wb.most_active_types
        >>> wb.top_option_types
        """
        if types == wb.top_option_types:
            tasks = [wb.get_top_options(type) for type in types]
            results = await asyncio.gather(*tasks)

            return results
        elif types == wb.most_active_types:
            tasks = [wb.get_most_active(type) for type in types]
            results = await asyncio.gather(*tasks)
            return results           


    async def earnings(self, start_date:str, pageSize: str='100', as_dataframe:str=True):
        """
        Pulls a list of earnings.

        >>> Start Date: enter a start date in YYYY-MM-DD format.

        >>> pageSize: enter the amount to be returned. default = 100

        >>> as_dataframe: default returns as a pandas dataframe.
        
        """
        endpoint = f"https://quotes-gw.webullfintech.com/api/bgw/explore/calendar/earnings?regionId=6&pageIndex=1&pageSize={pageSize}&startDate={start_date}"
        datas = await self.fetch_endpoint(endpoint)
        parsed_data = await parse_ticker_values(datas)
        if as_dataframe == False:
            return parsed_data
       
        df = pd.DataFrame(parsed_data)
        df.to_csv('data/earnings/earnings_upcoming.csv', index=False)
        return df


    async def get_top_gainers(self, rank_type:str='1d', pageSize: str='100', as_dataframe:bool=True):
        """
        Rank Types:

        >>> afterMarket
        >>> preMarket
        >>> 5min
        >>> 1d (daily)
        >>> 5d (5day)
        >>> 1m (1month)
        >>> 3m (3month)
        >>> 52w (52 week)  

        DEFAULT: 1d (daily) 


        >>> PAGE SIZE:
            Number of results to return. Default = 100     
        """
        endpoint = f"https://quotes-gw.webullfintech.com/api/bgw/market/topGainers?regionId=6&rankType={rank_type}&pageIndex=1&pageSize={pageSize}"
        datas = await self.fetch_endpoint(endpoint)
        parsed_data = await parse_ticker_values(datas)
        if as_dataframe == False:
            return parsed_data
        df = pd.DataFrame(parsed_data)
        df['rank_type'] = rank_type
        df['gainer_type'] = 'topGainers'

        df.to_csv(f'data/top_gainers/top_gainers_{rank_type}.csv', index=False)

        return df
    

    async def get_top_losers(self, rank_type:str='1d', pageSize: str='100', as_dataframe:bool=True):
        """
        Rank Types:

        >>> afterMarket
        >>> preMarket
        >>> 5min
        >>> 1d (daily)
        >>> 5d (5day)
        >>> 1m (1month)
        >>> 3m (3month)
        >>> 52w (52 week)  

        DEFAULT: 1d (daily) 


        >>> PAGE SIZE:
            Number of results to return. Default = 100     
        """
        endpoint = f"https://quotes-gw.webullfintech.com/api/bgw/market/dropGainers?regionId=6&rankType={rank_type}&pageIndex=1&pageSize={pageSize}"
        datas = await self.fetch_endpoint(endpoint)
        parsed_data = await parse_ticker_values(datas)
        if as_dataframe == False:
            return parsed_data
        df = pd.DataFrame(parsed_data)
        df['rank_type'] = rank_type
        df['gainer_type'] = 'topLosers'

        df.to_csv(f'data/top_losers/top_losers{rank_type}.csv', index=False)

        return df
    
    async def get_all_gainers_losers(self, type:str='gainers'):
        """TYPE OPTIONS:
        >>> gainers - all gainers across all rank_types
        >>> losers - all losers across all rank_types
        
        """
        types = wb.top_gainer_loser_types
        if type == 'gainers':
            tasks = [self.get_top_gainers(type) for type in types]
            results = await asyncio.gather(*tasks)
            return results
        

        elif type == 'losers':
            tasks =[self.get_top_losers(type) for type in types]
            results = await asyncio.gather(*tasks)
            return results



wb = Webull()
async def main():

    top_active = await wb.get_all_gainers_losers('losers')
    print(top_active)
asyncio.run(main())