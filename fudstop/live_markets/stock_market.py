import os
from dotenv import load_dotenv

load_dotenv()

from fudstop.apis.helpers import map_stock_conditions, STOCK_EXCHANGES, stock_condition_dict, TAPES
from fudstop.apis.helpers import format_large_number, convert_to_ns_datetime
from fudstop.list_sets.ticker_lists import most_active_tickers
from polygon.websocket import WebSocketClient, EquityTrade  
from polygon.websocket.models import WebSocketMessage
from typing import List
from discord_webhook import DiscordEmbed, DiscordWebhook
# type: ignore
import orjson
import asyncio
import asyncpg



class StockMarketLive:
    def __init__(self):
        self.pool = None
        self.connection_string = os.environ.get('STOCK_MARKET')  # Use one environment variable for connection string

        self.c = WebSocketClient(subscriptions=["T.*"], custom_json=orjson, api_key=os.environ.get('YOUR_POLYGON_KEY'))

    async def connect(self):
        self.pool = await asyncpg.create_pool(dsn=self.connection_string, min_size=1, max_size=10)
        return self.pool
    async def fetch(self, query):
        async with self.pool.acquire() as conn:
            records = await conn.fetch(query)
            return records
        


    async def fetch_latest_trade(self, ticker):
        async with self.pool.acquire() as connection:
            # The query must have a placeholder for the ticker parameter
            query = "SELECT symbol,size,price,timestamp FROM trades WHERE symbol = $1 ORDER BY timestamp DESC LIMIT 1;"
            latest_trade = await connection.fetchrow(query, ticker)
            return latest_trade
    async def insert_trade(self, trade_data):
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                # The SQL INSERT statement with placeholders for the actual trade data
                insert_query = """
                INSERT INTO trades (
                    event_type, symbol, exchange, external_id, tape, price, size,
                    conditions, timestamp, sequence_number, trf_id, trf_timestamp
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
                )
                """
                await connection.execute(
                    insert_query,
                    trade_data['event_type'],
                    trade_data['symbol'],
                    trade_data['exchange'],
                    trade_data['id'],  # Assuming this is the 'external_id'
                    trade_data['tape'],
                    trade_data['price'],
                    trade_data['size'],
                    trade_data['conditions'],  # Ensure this is passed as an array of strings
                    trade_data['timestamp'],
                    trade_data['sequence_number'],
                    trade_data['trf_id'],
                    trade_data['trf_timestamp']
                )

        return self.pool

    async def handle_msg(self, msgs: WebSocketMessage):
        
        
        for message in msgs:

            if hasattr(message, 'conditions') and message.symbol in set(most_active_tickers):
                conditions = [stock_condition_dict.get(i) for i in message.conditions] if message.conditions is not None else []

                exchange = STOCK_EXCHANGES.get(message.exchange)    
                timestamp = convert_to_ns_datetime(message.timestamp)
                trf_timestamp = convert_to_ns_datetime(message.timestamp)

                tape = TAPES.get(message.tape)
                print(timestamp)


                dollar_cost = message.price * message.size


                trade_data = { 

                    'symbol': message.symbol,
                    
                    
                    'event_type': message.event_type,
                    'exchange': exchange,
                    'id': message.id,
                    'tape': tape,
                    'price': message.price,
                    'size': message.size,
                    'conditions': conditions,
                    'timestamp': timestamp,
                    
                    'sequence_number': message.sequence_number,
                    'trf_timestamp': trf_timestamp,
                    'trf_id': message.trf_id,
                    'dollar_cost': dollar_cost,
                    
                    
                }




                await self.insert_trade(trade_data)



market = StockMarketLive()

async def main():
    await market.connect()
    await asyncio.gather(market.c.connect(market.handle_msg))
    

if __name__ == '__main__':
    asyncio.run(main())
