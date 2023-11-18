import os
from dotenv import load_dotenv

load_dotenv()



import asyncio


from fudstop.apis.federal_register.fed_register_sync import FedRegisterSDK


rss =FedRegisterSDK()



def main():
    urls = rss.query_document('options')

    print(urls)


main()