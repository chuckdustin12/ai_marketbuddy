import asyncio

from webull.webull_markets import WebullMarkets
import os

connection_string = os.environ.get('CONNECTION_STRING')

markets = WebullMarkets(connection_string)


async def all_market_functions():


    etfs = await markets.get_all_etfs(markets.etf_types)

    await markets.get_top_gainers('1d', as_dataframe=True)
    await markets.get_all_etfs(types=markets.etf_types)

    losers = await markets.get_all_gainers_losers('losers')


    await markets.get_top_losers(rank_type='52w')

    await markets.get_top_options(rank_type='posIncrease')

    earnings = await markets.earnings(start_date='2023-10-27')

    forex = await markets.get_forex()


asyncio.run(all_market_functions())
