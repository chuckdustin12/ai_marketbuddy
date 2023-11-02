from fudstop.apis.webull.webull_trading import WebullTrading
from fudstop.apis.polygonio.async_polygon_sdk import Polygon
from fudstop.list_sets.ticker_lists import most_active_tickers
from fudstop.apis.stocksera_.stocksera_ import StockSera
from fudstop.apis.cfr.cfr_sdk import CFRManager


import asyncio

trading = WebullTrading()
polygon = Polygon()
ss = StockSera()
cfr = CFRManager()

import xml.etree.ElementTree as ET

async def get_documents(url, filename):
    
    cfr.get_xml_document(url, filename)



    # Load the XML file
    tree = ET.parse(filename)

    # Get the root element of the XML document
    root = tree.getroot()

    # Now you can work with the XML data. For example, to print all tags and text content:
    for elem in root.iter():
        print(f'{elem.tag}: {elem.text}')

    # Or, to find and work with specific elements:
    for elem in root.findall('.//YourElementTag'):
        print(f'{elem.tag}: {elem.text}')



    # Test the function with the uploaded XML file
    parsed_data = cfr.parse_title_12_xml('title-11.xml')
    parsed_data.keys(), len(parsed_data['titles']), len(parsed_data['chapters']), len(parsed_data['parts'])
    await cfr.connect()

    # Create tables (if they don't exist)
    await cfr.create_title_table()
    await cfr.create_chapter_table()
    await cfr.create_part_table()
    await cfr.create_authority_table()
    await cfr.create_source_table()

    # Insert parsed data into the database
    await cfr.insert_parsed_data(parsed_data)



asyncio.run(get_documents(url="https://www.ecfr.gov/api/versioner/v1/full/2023-09-27/title-11.xml", filename='title-11.xml'))