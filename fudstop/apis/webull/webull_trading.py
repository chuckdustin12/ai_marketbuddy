import aiohttp
import pandas as pd
from pytz import timezone

from .trade_models.capital_flow import CapitalFlow
from .trade_models.cost_distribution import CostDistribution
from .trade_models.etf_holdings import ETFHoldings
from .trade_models.institutional_holdings import InstitutionHolding, InstitutionStat
from .trade_models.financials import BalanceSheet, FinancialStatement, CashFlow
from .trade_models.news import NewsItem
from .trade_models.forecast_evaluator import ForecastEvaluator
from .trade_models.short_interest import ShortInterest
from .trade_models.volume_analysis import WebullVolAnalysis
from .trade_models.ticker_query import WebullStockData
from .trade_models.analyst_ratings import Analysis


class WebullTrading:
    def __init__(self):
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
        


    async def get_stock_quote(self, ticker:str):
        ticker_id = await self.get_ticker_id(ticker)

        endpoint = f"https://quotes-gw.webullfintech.com/api/stock/tickerRealTime/getQuote?tickerId={ticker_id}&includeSecu=1&includeQuote=1&more=1"
        datas = await self.fetch_endpoint(endpoint)

        data = WebullStockData(datas)
        return data


    async def get_analyst_ratings(self, ticker:str):
        ticker_id = await self.get_ticker_id(ticker)
        endpoint=f"https://quotes-gw.webullfintech.com/api/information/securities/analysis?tickerId={ticker_id}"
        datas = await self.fetch_endpoint(endpoint)
        data = Analysis(datas)
        return data
    

    async def get_short_interest(self, ticker:str):
        ticker_id = await self.get_ticker_id(ticker)
        endpoint = f"https://quotes-gw.webullfintech.com/api/information/brief/shortInterest?tickerId={ticker_id}"
        datas = await self.fetch_endpoint(endpoint)
        data = ShortInterest(datas)
        return data
    
    async def institutional_holding(self, ticker:str):
        ticker_id = await self.get_ticker_id(ticker)
        endpoint = f"https://quotes-gw.webullfintech.com/api/information/stock/getInstitutionalHolding?tickerId={ticker_id}"
        datas = await self.fetch_endpoint(endpoint)
        data = InstitutionStat(datas)

        return data
    

    async def volume_analysis(self, ticker:str):
        ticker_id = await self.get_ticker_id(ticker)
        endpoint = f"https://quotes-gw.webullfintech.com/api/stock/capitalflow/stat?count=10&tickerId={ticker_id}&type=0"
        datas = await self.fetch_endpoint(endpoint)
        data = WebullVolAnalysis(datas)
        return data
    

    async def cost_distribution(self, ticker:str, start_date:str='2023-10-01', end_date:str='2023-10-27'):
        ticker_id = await self.get_ticker_id(ticker)
        endpoint = f"https://quotes-gw.webullfintech.com/api/quotes/chip/query?tickerId={ticker_id}&startDate={start_date}&endDate={end_date}"
        datas = await self.fetch_endpoint(endpoint)
        data = CostDistribution(datas)
        return data
    

    async def stock_quote(self, ticker:str):
        ticker_id = await self.get_ticker_id(ticker)
        endpoint = f"https://quotes-gw.webullfintech.com/api/bgw/quote/realtime?ids={ticker_id}&includeSecu=1&delay=0&more=1"
        datas = await self.fetch_endpoint(endpoint)
        data = WebullStockData(datas)
        return data
    

    async def news(self, ticker:str, pageSize:str='100'):
        ticker_id = await self.get_ticker_id(ticker)
        endpoint = f"https://nacomm.webullfintech.com/api/information/news/tickerNews?tickerId={ticker_id}&currentNewsId=0&pageSize={pageSize}"
        datas = await self.fetch_endpoint(endpoint)
        data = NewsItem(datas)
        return data
    

    async def balance_sheet(self, ticker:str, limit:str='11'):
        ticker_id = await self.get_ticker_id(ticker)
        endpoint = f"https://quotes-gw.webullfintech.com/api/information/financial/balancesheet?tickerId={ticker_id}&type=101&fiscalPeriod=0&limit={limit}"
        datas = await self.fetch_endpoint(endpoint)
        data = BalanceSheet(datas)
        return data
    
    async def cash_flow(self, ticker:str, limit:str='12'):
        ticker_id = await self.get_ticker_id(ticker)
        endpoint = f"https://quotes-gw.webullfintech.com/api/information/financial/cashflow?tickerId={ticker_id}&type=102&fiscalPeriod=1,2,3,4&limit={limit}"
        datas = await self.fetch_endpoint(endpoint)
        data = CashFlow(datas)
        return data
    
    async def income_statement(self, ticker:str, limit:str='12'):
        ticker_id = await self.get_ticker_id(ticker)
        endpoint = f"https://quotes-gw.webullfintech.com/api/information/financial/incomestatement?tickerId={ticker_id}&type=102&fiscalPeriod=1,2,3,4&limit={limit}"
        datas = await self.fetch_endpoint(endpoint)
        data = FinancialStatement(datas)
        return data
    



    async def capital_flow(self, ticker:str):
        ticker_id = await self.get_ticker_id(ticker)
        endpoint = f"https://quotes-gw.webullfintech.com/api/stock/capitalflow/ticker?tickerId={ticker_id}&showHis=true"
        datas = await self.fetch_endpoint(endpoint)
        data = CapitalFlow(datas)
        return data
    

    async def etf_holdings(self, ticker:str, pageSize:str='200'):
        ticker_id = await self.get_ticker_id(ticker)
        endpoint = f"https://quotes-gw.webullfintech.com/api/information/company/queryEtfList?tickerId={ticker_id}&pageIndex=1&pageSize={pageSize}"
        datas = await self.fetch_endpoint(endpoint)
        data = ETFHoldings(datas)
        return data
    

    async def forecast(self):
       # ticker_id = await self.get_ticker_id(ticker)

       endpoint = f""