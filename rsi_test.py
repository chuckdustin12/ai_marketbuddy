import asyncio
import aiohttp
import os


from fudstop.list_sets.ticker_lists import most_active_tickers
from aiohttp.client_exceptions import ClientOSError

YOUR_POLYGON_KEY = os.environ.get('YOUR_POLYGON_KEY')




async def get_rsi(ticker:str, timespan:str='day', window=14, limit:str=1):
    try:
        endpoint = f"https://api.polygon.io/v1/indicators/rsi/{ticker}?timespan={timespan}&adjusted=true&window={window}&series_type=close&order=desc&limit=1&apiKey={YOUR_POLYGON_KEY}"

        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint) as resp:
                data = await resp.json()


                results = data['results'] if 'results' in data else None

                if results is not None:
                    values = results.get('values', 'NA')

                    if values is not 'NA':

                        rsi_values = [i.get('value', 'NA') for i in values]

                        ##################################################


                        #this is where you can THINK and create custom scans

                        ############# YOU START CODING HERE############

                        for rsi in rsi_values:
                            if rsi <= 30:
                                status = f'{ticker} is OVERSOLD - {timespan}'

                                print(status)

                            elif rsi >= 70:
                                status = f'{ticker} is OVERBOUGHT - {timespan}'
                                print(status)







    except ClientOSError:
        print(f'OS Error: {ticker}')

    #autism

async def get_all_rsi():

    timespan= 'day'
    tasks = [get_rsi(ticker, timespan) for ticker in most_active_tickers]


    await asyncio.gather(*tasks)



asyncio.run(get_all_rsi())