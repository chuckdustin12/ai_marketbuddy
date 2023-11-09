import os
from dotenv import load_dotenv

import aiohttp
import json
import asyncio

async def send_discord_webhook(webhook_url, message, color):
    async with aiohttp.ClientSession() as session:
        webhook_data = {
            "embeds": [{
                "title": "RSI Alert",
                "description": message,
                "color": color
            }]
        }
        async with session.post(webhook_url, json=webhook_data) as response:
            return await response.text()

async def process_ticker(i, timespan, webhook_url):
    rsi = i.rsi_value[0]
    status = ''
    if rsi <= 30 and timespan == 'hour':
        status = 'is oversold'
    elif rsi >= 70 and timespan == 'hour':
        status = 'is overbought'

    if status:
        color = 0x00ff00 if status == 'is oversold' else 0xff0000
        message = f"{i.ticker} {status} on the {timespan} with an RSI of: {rsi}"
        await send_discord_webhook(webhook_url, message, color)

async def gather_rsi_results(timespan, webhook_url):
    rsi = await polygon.gather_rsi_for_all_tickers(tickers=most_active_tickers)
    tasks = []
    for i in rsi:
        if i and i.rsi_value and len(i.rsi_value) > 0:
            task = asyncio.create_task(process_ticker(i, timespan, webhook_url))
            tasks.append(task)
    await asyncio.gather(*tasks)

async def main():
    hour_hook_url = os.environ.get('hour_osob')
    minute_hook_url = os.environ.get('minute_osob')
    day_hook_url = os.environ.get('day_osob')
    
    await asyncio.gather(
        gather_rsi_results('hour', hour_hook_url),
        gather_rsi_results('minute', minute_hook_url),
        gather_rsi_results('day', day_hook_url)
    )

asyncio.run(main())