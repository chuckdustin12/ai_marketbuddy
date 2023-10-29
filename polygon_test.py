from apis.polygonio.async_polygon_sdk import Polygon
import os

import asyncio

poly = Polygon()


# Creating a database configuration dictionary using os.environ.get for environment variables
db_config = {
    "DB_HOST": os.environ.get('DB_HOST', 'localhost'),
    "DB_PASSWORD": os.environ.get('DB_PASSWORD'),
    "DB_NAME": os.environ.get('COMPANY_INFO_STRING'),
    "DB_PORT": os.environ.get('DB_PORT', 5432),
    "DB_USER": os.environ.get('DB_USER')
}



async def ticker_snapshot():

    ticker_data = await poly.get_all_tickers(save_all_tickers=True)


    print(f"Number of tickers found: {len(ticker_data)}")

    print(ticker_data)

asyncio.run(ticker_snapshot())