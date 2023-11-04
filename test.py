import os
from dotenv import load_dotenv

load_dotenv()
GEX_KEY = os.environ.get('GEXBOT')

print(GEX_KEY)


from fudstop.apis.gexbot.gexbot import GEXBot
from fudstop.list_sets.ticker_lists import most_active_tickers
gexbot = GEXBot()
import requests
from gex import Gex, GexMajorLevels, MaxGex
import aiohttp
import asyncio
import pandas as pd 

async def run_concurrent():
    
    await gexbot.run_all_gex()


asyncio.run(run_concurrent())