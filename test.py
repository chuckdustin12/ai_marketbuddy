from fudstop.apis.webull.webull_trading import WebullTrading
from fudstop.apis.polygonio.async_polygon_sdk import Polygon
from fudstop.list_sets.ticker_lists import most_active_tickers
from fudstop.apis.stocksera_.stocksera_ import StockSera



trading = WebullTrading()
polygon = Polygon()
ss = StockSera()

import asyncio



async def technicals(ticker):

    rsi = await polygon.rsi(ticker, 'minute', limit=100)
    print(rsi.as_dataframe)


    ema = await polygon.ema(ticker, 'day', limit=100)
    print(ema.as_dataframe)


    sma = await polygon.sma(ticker, 'day', limit=100)
    print(sma.as_dataframe)


asyncio.run(technicals('AAPL'))



sec_filings = ss.sec_filings('AAPL')

print(sec_filings)