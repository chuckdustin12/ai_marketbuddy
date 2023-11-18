import os
from dotenv import load_dotenv

load_dotenv()

import asyncpg
import asyncio
from fudstop.apis.polygonio.async_polygon_sdk import Polygon
from fudstop.apis.polygonio.polygon_options import PolygonOptions
from fudstop.list_sets.ticker_lists import all_tickers, most_active_tickers
import pandas as pd

opts = PolygonOptions()
poly = Polygon()
from asyncio import Semaphore
sema = Semaphore(30)
async def get_all_options(ticker):
    async with sema:
        options = await opts.get_option_chain_all(ticker)
        df = pd.DataFrame(options.data_dict)
        print(type(options.data_dict))
        
        await opts.batch_insert_dataframe(df, 'options_data', unique_columns='option_symbol')
  
    

async def run_all_options():
    await opts.connect()
    try:
        tasks = [get_all_options(i) for i in most_active_tickers]

        await asyncio.gather(*tasks)
    except Exception as e:
        print(f'Error - {e}')


asyncio.run(run_all_options())
