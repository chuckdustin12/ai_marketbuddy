import feedparser
from fudstop.apis.polygonio.async_polygon_sdk import Polygon
import asyncio
# URL of the SEC 10-K RSS feed
from fudstop.apis.rss.rss_sdk import RSSSDK
from fudstop.apis.rss.rss_models import Feed, Entries
import aiohttp

polygon = Polygon()
rss = RSSSDK()


headers = rss.headers

from xml.etree import ElementTree as ET
from fudstop.list_sets.ticker_lists import most_active_tickers
import requests

from asyncio import Semaphore
sem = Semaphore(10)
async def main(ticker, session, sem):
    async with sem:
        # Assuming rss.get_filing_urls is an asynchronous function that returns a dictionary
        await rss.get_filing_urls(ticker)
    
async def run_main():
    sem = Semaphore(10)  # Set the semaphore value to 10
    async with aiohttp.ClientSession() as session:  # Using aiohttp for HTTP requests
        tasks = [main(ticker, session, sem) for ticker in most_active_tickers]
        await asyncio.gather(*tasks)

# asyncio.run(run_main())



async def get_filing_by_type():
    for i in most_active_tickers:
        x = await rss.get_filing_urls(i)


asyncio.run(get_filing_by_type())
   
