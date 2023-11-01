from fudstop.apis.fed_print.fedprint_sdk import FedPrint
import asyncio

import os
fed = FedPrint()
import requests

async def main():

    ids = await fed.get_series_id()
    for id in ids:

        series_info = await fed.get_series(id)

        print(series_info.as_dataframe)



asyncio.run(main())



