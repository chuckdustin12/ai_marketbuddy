import os

import disnake
import base64
from disnake.ext import commands
from list_sets.ticker_lists import most_active_tickers
most_active_tickers = set(most_active_tickers)
import asyncio
from bot_menus.pagination import AlertMenus
import aiohttp
import matplotlib.pyplot as plt
import io
import pandas as pd
from cogs.database import MyModal
from apis.polygonio.polygon_options import PolygonOptions
from apis.webull.opt_modal import OptionModal, SQLQueryModal
from apis.gexbot.gexbot import GEXBot
from dalle_modal import DalleModal
opts = PolygonOptions()
gexbot = GEXBot()
bot = commands.Bot(command_prefix="!", intents=disnake.Intents.all())
from td9_test import scan_bars, scan_all_bars
from cogs.database import QueryView
from list_sets.ticker_lists import gex_tickers
from typing import List
import disnake
from disnake.ext import commands
import openai

import os
from dotenv import load_dotenv
load_dotenv()
# Initialize the OpenAI client with your API key
openai.api_key = os.getenv("YOUR_OPENAI_KEY")

from openai import OpenAI

@bot.event
async def on_message(message: disnake.Message):
    """Use  GPT4 Vision to listen for image URLs"""

    if message.channel.id == 896207280117264434:

        if message.author.id == 1152436404522066002:
            await message.delete()
        print(message.content)
        if message.content.endswith('.png'):

            response = client.chat.completions.create(
                model="gpt-4-vision-preview",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": f"Explain the options chain to the user. You're an option trader expert. Call open interest = resistance. Put open interest = support. Look for call skews - or lower iV out of the money. Take note of volume values, open interest values - reiterate them back to the user."},
                            {
                                "type": "image_url",
                                "image_url": message.content,
                            },
                        ],
                    }
                ],
                max_tokens=2000,
            )

            await message.channel.send(response.choices[0].message.content[:2000])
 
        if message.content.endswith('.jpg'):

            response = client.chat.completions.create(
                model="gpt-4-vision-preview",
                messages=[
                    {
                        "role": "user",
                        "content": [
                            {"type": "text", "text": "What’s in this image?"},
                            {
                                "type": "image_url",
                                "image_url": message.content,
                            },
                        ],
                    }
                ],
                max_tokens=300,
            )

            await message.channel.send(response.choices[0].message)

    await bot.process_commands(message)
# This dictionary will hold the conversation state for each user
conversations = {}
client = OpenAI(api_key=os.environ.get('YOUR_OPENAI_KEY'))
@bot.slash_command()
async def option_data(inter: disnake.ApplicationCommandInteraction):
    modal = OptionModal()
    await inter.response.send_modal(modal)






@bot.slash_command()
async def options_database(inter:disnake.ApplicationCommandInteraction):
    """Use a Modal to query the database for options."""
    modal = SQLQueryModal()
    await inter.response.send_modal(modal)
@bot.command()
async def slave(ctx):
    # Start a new conversation with the user
    conversations[ctx.author.id] = []
    
    # Send an initial message to the user
    await ctx.send("> # Slave Bot\n> Online. \n\n> Your work is my...work.. Type to chat... or.. type stop to quit.")

    while True:
        # Wait for a message from the same user
        message = await bot.wait_for(
            "message",
            check=lambda m: m.author == ctx.author and m.channel == ctx.channel
        )

        # Check if the user wants to stop the conversation
        if message.content.lower() == "stop":
            await ctx.send("Goodbye! If you need help again, just call me.")
            del conversations[ctx.author.id]  # Clean up the conversation
            break

        # Append the user's message to the conversation
        conversations[ctx.author.id].append({"role": "user", "content": message.content})

        # Send the conversation to OpenAI
        response = client.chat.completions.create(
            model="gpt-4-1106-preview",
            messages=conversations[ctx.author.id],
            temperature=0.33,
            max_tokens=4000        )

        # Get the content from the OpenAI response
        ai_response = response.choices[0].message.content

        # Append the AI's response to the conversation
        conversations[ctx.author.id].append({"role": "assistant", "content": ai_response})
        embeds = []
        chunks = [ai_response[i:i + 3860] for i in range(0, len(ai_response), 3860)]
        for chunk in chunks:
            embed = disnake.Embed(title=f"GPT4-Turbo", description=f"```py\n{chunk}```", color=disnake.Colour.dark_orange())
            embed.add_field(name=f"Your prompt:", value=f"> **{message.content[:400]}**", inline=False)
            embeds.append(embed)

        await ctx.send(embed=embeds[0], view=AlertMenus(embeds))

@bot.slash_command()
async def dalle3(inter: disnake.AppCmdInter):
    """Generates an image using openai's Dalle3"""
    await inter.response.send_modal(modal=DalleModal(bot))


    




class WebullModal(disnake.ui.Modal):
    def __init__(self):
        self.tickers = most_active_tickers
        self.valid_tickers = most_active_tickers
        

        
        components=[disnake.ui.TextInput(
            label="Enter Ticker(s)",
            placeholder="e.g., AAPL or AAPL,MSFT,GOOGL",
            custom_id="tickers_input",
            style=disnake.TextInputStyle.short,
            max_length=100  # Adjust the max length as needed
        ),

        disnake.ui.TextInput(
            label="Timeframe",
            placeholder="e.g., 1min, 5min, 10min, 15min, 1hour, daily, weekly",
            custom_id="timeframe_input",
            style=disnake.TextInputStyle.short,
            max_length=5  # Adjust the max length as needed
        )]

        super().__init__(title="TD9 Analysis Configuration", components=components)
    async def callback(self, interaction: disnake.ModalInteraction):
        await interaction.response.defer(ephemeral=True)
        # Retrieve values from the modal
        user_input = interaction.text_values
       
    
        tickers = user_input.get('tickers_input')


        results = await scan_all_bars(tickers)


        await interaction.edit_original_message(f"# > **{results}**")



    


        # # Validate tickers
        # valid_tickers = [ticker for ticker in tickers if ticker in self.valid_tickers]
        # invalid_tickers = list(set(tickers) - set(valid_tickers))

        # if not valid_tickers:
        #     # No valid tickers were entered
        #     await interaction.response.send_message(
        #         "None of the entered tickers were recognized. Please try again with valid ticker symbols.",
        #         ephemeral=True
        #     )
        #     return

        # # If there are invalid tickers, inform the user but proceed with valid ones
        # if invalid_tickers:
        #     await interaction.followup.send(
        #         f"The following tickers were not recognized and will be ignored: {', '.join(invalid_tickers)}",
        #         ephemeral=True
        #     )

        # # Proceed with scanning the valid tickers
        # results = await scan_bars(valid_tickers, timeframe)
        # await interaction.followup.send(results)

from live_markets.stock_market import StockMarketLive




stock_market = StockMarketLive()
@bot.slash_command()
async def stream(inter: disnake.AppCmdInter, ticker:str):
    """Stream live trades for a ticker."""
    await inter.response.defer()
    
    await stock_market.connect()
    counter = 0
    while True:
        counter = counter + 1
        data = await stock_market.fetch_latest_trade(ticker)
        if data:
            # Format the message with the trade data
            message = f"> # Latest trade for {ticker} | Price: ${data['price']} | Size: {data['size']} | Time: {data['timestamp']}"
        else:
            # No trade data found
            message = f"> No recent trades found for {ticker}."
        await inter.edit_original_message(f"> # {message}")

        if counter == 250:
            await inter.send(f'> # Stream ended.')
            break
    





timeframe_choices = [
    disnake.OptionChoice(name="1 Minute", value="m1"),
    disnake.OptionChoice(name="5 Minutes", value="m5"),
    disnake.OptionChoice(name="10 Minutes", value="m10"),
    disnake.OptionChoice(name="15 Minutes", value="m15"),
    disnake.OptionChoice(name="30 Minutes", value="m30"),
    disnake.OptionChoice(name="1 Hour", value="m60"),
    disnake.OptionChoice(name="2 Hours", value="m120"),
    disnake.OptionChoice(name="4 Hours", value="m240"),
    disnake.OptionChoice(name="Daily", value="d1"),
    disnake.OptionChoice(name="Weekly", value="w")
]



@bot.command()
async def query(inter:disnake.AppCmdInter, ticker:str):
    await inter.send(view=QueryView(ticker))


@bot.command()
async def test(ctx, ticker):
    await ctx.send(view=QueryView(ticker))





bot.load_extensions('fudstop/cogs')
bot.run(os.environ.get('BOT'))