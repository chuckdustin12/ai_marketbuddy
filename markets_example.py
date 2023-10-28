import asyncio

from webull.webull_markets import WebullMarkets
import os

connection_string = os.environ.get('CONNECTION_STRING')

markets = WebullMarkets(connection_string)


async def all_market_functions():


    etfs = await markets.get_all_etfs(markets.etf_types)

    gainers = await markets.get_all_gainers_losers('gainers')

    losers = await markets.get_all_gainers_losers('losers')


    most_active = await markets.get_all_rank_types(types=markets.most_active_types)

    top_options = await markets.get_all_rank_types(types=markets.top_option_types)

    earnings = await markets.earnings(start_date='2023-10-27')

    forex = await markets.get_forex()


asyncio.run(all_market_functions())
