import os
from dotenv import load_dotenv
load_dotenv()
from fudstop.list_sets.ticker_lists import most_active_tickers

from fudstop.apis.webull.webull_options import WebullOptions


from fudstop.apis.webull.webull_trading import WebullTrading


trading = WebullTrading()


import asyncio


async def main():
    x = await trading.stock_quote('GME')
    print(x.avg_10d_vol)


    print(x)

asyncio.run(main())