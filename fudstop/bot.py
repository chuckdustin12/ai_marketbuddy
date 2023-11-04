import os

import disnake
from tabulate import tabulate
from testicals import update_plot
from disnake.ext import commands

import matplotlib.pyplot as plt
import io
import pandas as pd

from apis.polygonio.polygon_options import PolygonOptions
from apis.gexbot.gexbot import GEXBot
opts = PolygonOptions()
gexbot = GEXBot()
bot = commands.Bot(command_prefix="!", intents=disnake.Intents.all())




@bot.slash_command()
async def skew(inter:disnake.AppCmdInter, ticker):
    """Track 0dte skew"""
    opts = PolygonOptions()
    await inter.response.defer()
    while True:
        data = await opts.get_near_the_money_single(ticker=ticker,exp_less_than=opts.today)
        

        
        snapshot = await opts.get_universal_snapshot(data)
        df = pd.DataFrame(snapshot)
    
        print()
        # Ensure the columns you want to use are in the DataFrame
        columns_to_use = ['implied_volatility', 'details.strike_price', 'details.contract_type', 'underlying_asset.price']

        # Check if all desired columns are present in the DataFrame
        missing_columns = [col for col in columns_to_use if col not in df.columns]
        if missing_columns:
            print(f"Warning: Missing columns {missing_columns} in the dataframe. These will be skipped.")
            columns_to_use = [col for col in columns_to_use if col in df.columns]

        # If none of the columns are present, we shouldn't proceed
        if not columns_to_use:
            raise ValueError("None of the specified columns are present in the dataframe.")

        # If there are valid columns to use, proceed with sorting and selecting the top row
        if columns_to_use:
            # Sort by 'implied_volatility' in descending order and select the top row
            top_row = df.sort_values('implied_volatility', ascending=False).iloc[0]

            # Create a list with only the selected columns' values
            selected_values = [top_row[col] for col in columns_to_use]
            df = [selected_values]
            df = df[0]
            print(f"IV: {df[0]} | Skew: {df[1]} | Price: {df[2]}")
            if df[2] == 'put':
                em = "🔴 "
            else:
                em = "🟢"
            await inter.edit_original_message(f"> {em} IV: {df[0]} | Skew: {df[1]} | Type: {df[2]} | Price: {df[3]}")

import asyncio
import aiohttp
from gex import Gex,GexMajorLevels



GEX_KEY = os.environ.get('GEXBOT')

            
from list_sets.ticker_lists import most_active_tickers


@bot.slash_command()
async def gex(inter: disnake.AppCmdInter, ticker:str):
    """Gets GEX - Work in progress"""
    ticker = ticker.upper()
    await inter.response.defer()
    while True:

        gex = await gexbot.get_gex(ticker)


        await inter.edit_original_message(f"> **{gex}**")

        await asyncio.sleep(5)

@bot.slash_command()
async def all_gex(inter: disnake.AppCmdInter):
    """Gets GEX in real-time and displays a chart."""
    await inter.response.defer()
    counter = 0
    message = None
    
    while True:
        counter += 1
        df = await gexbot.run_all_gex()
        df = df[['ticker', 'spot', 'zero_gamma', 'sum_gex_vol', 'sum_gex_oi']]
        print(df)
        df.to_csv('dataframe_test.csv', index=False)
        # Generate the plot
        plt.figure(figsize=(10, 5))
        for ticker in df['ticker'].unique():
            plt.plot(df[df['ticker'] == ticker]['spot'], label=ticker)
        plt.legend()
        plt.title('Spot Price by Ticker')
        plt.xlabel('Time')
        plt.ylabel('Spot Price')
        plt.grid(True)
        
        # Save the plot to a BytesIO object
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png')
        plt.close()  # Close the plot to release memory
        buffer.seek(0)
        
        file = disnake.File(buffer, filename='chart.png')
        embed = disnake.Embed()
        embed.set_image(url="attachment://chart.png")
        
        try:
            # If it's the first message, send it. Otherwise, edit the existing message.
            if message is None:
                message = await inter.edit_original_message(file=file, embed=embed)

        except Exception as e:
            print(f"An error occurred: {e}")
        finally:
            buffer.close()  # Close the buffer after editing the message
            plt.close()
        
        if counter == 250:
            await inter.send('Stream ended.')
            break
        # Sleep to avoid rate limiting (adjust as needed)
        await asyncio.sleep(1)




async def run_gex():
    tasks = [gex(i) for i in most_active_tickers]
    await asyncio.gather(*tasks)


@bot.slash_command()
async def gex_major_levels(inter:disnake.AppCmdInter, ticker:str):
    ticker = ticker.upper()
    await inter.response.defer()
    counter = 0
    while True:
        counter = counter + 1
        data = await gexbot.major_levels(ticker)
        embed = disnake.Embed(title=f"Gex Major Levels - {ticker}", description=f"```py\n{data.as_dataframe}```", color=disnake.Colour.dark_gold())
      
        embed.set_footer(text='Provided by KRAKENSLAYER')


        await inter.edit_original_message(embed=embed)

        if counter == 250:
            await inter.edit_original_message(f"> Stream ended. Use /gex_major_levels to run again.")



@bot.slash_command()
async def gex_spy_spx(inter:disnake.AppCmdInter):
  
    await inter.response.defer()
    counter = 0
    while True:
        counter = counter + 1
        data = await gexbot.major_levels('SPX')
        data2 = await gexbot.major_levels('SPY')
        embed = disnake.Embed(title=f"Gex Major Levels - SPY/SPX", color=disnake.Colour.dark_gold())
        
        embed.add_field(name=f"Zero Gamma:", value=f"> SPY: **{round(float(data2.zero_gamma),4)}**\n> SPX: **{round(float(data.zero_gamma),2)}**")
        embed.add_field(name=f"Major OI:", value=f"> SPY POS: **{data2.mpos_oi}**\n> SPY NEG: **{data2.mneg_oi}**\n\n> SPX POS: **{data.mpos_oi}**\n> SPX NEG: **{data.mneg_oi}**")
        embed.add_field(name=f"Major Vol:", value=f"> SPY POS: **{data2.mpos_vol}**\n> SPY NEG: **{data2.mneg_vol}**\n\n> SPX POS: **{data.mpos_vol}**\n> SPX NEG: **{data.mneg_vol}**")
        embed.add_field(name=f"Prices:", value=f"> SPY: **${data2.spot}**\n> SPX: **${data.spot}**")
        embed.set_footer(text='Provided by KRAKENSLAYER')


        await inter.edit_original_message(embed=embed)

        if counter == 250:
            await inter.edit_original_message(f"> Stream ended. Use /gex_spy_spx to run again.")





bot.load_extensions('fudstop/cogs')
bot.run(os.environ.get('BOT'))