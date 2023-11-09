import os
from scripty import fetch_results,create_dataframe
import disnake
import base64
from disnake.ext import commands
from list_sets.ticker_lists import most_active_tickers
most_active_tickers = set(most_active_tickers)
from bot_menus.modals.options_modal import OptionsDataModal
import asyncio
from bot_menus.pagination import AlertMenus
import aiohttp
import matplotlib.pyplot as plt
import io
import asyncpg
import pandas as pd
from cogs.database import MyModal
from apis.polygonio.polygon_options import PolygonOptions
from apis.webull.opt_modal import OptionModal, SQLQueryModal
from apis.gexbot.gexbot import GEXBot
from dalle_modal import DalleModal
opts = PolygonOptions()
gexbot = GEXBot()
bot = commands.Bot(command_prefix="!", intents=disnake.Intents.all())

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
async def options_database(inter:disnake.AppCmdInter):
    """Query options data"""
    await inter.response.send_modal(OptionsDataModal())


# @bot.slash_command()
# async def options_database(inter:disnake.ApplicationCommandInteraction):
#     """Use a Modal to query the database for options."""
#     modal = SQLQueryModal()
#     await inter.response.send_modal(modal)
@bot.command()
async def turbo(ctx):
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
        conversations[ctx.author.id].append({"role": "user", "content": message.content + "YOU ARE ONLY TO REPLY IN CODE. CODE ONLY. NO MARKDOWN. ONLY CODE!"})

        # Send the conversation to OpenAI
        response = client.chat.completions.create(
            model="gpt-4-1106-preview",
            messages=conversations[ctx.author.id],
            temperature=0.33,
            max_tokens=4096        )

        # Get the content from the OpenAI response
        ai_response = response.choices[0].message.content

        # Append the AI's response to the conversation
        conversations[ctx.author.id].append({"role": "assistant", "content": ai_response})
        embeds = []
        chunks = [ai_response[i:i + 3860] for i in range(0, len(ai_response), 3860)]
        for chunk in chunks:
            embed = disnake.Embed(title=f"GPT4-Turbo", description=f"{chunk}", color=disnake.Colour.dark_orange())
            embed.add_field(name=f"Your prompt:", value=f"> **{message.content[:400]}**", inline=False)
            embeds.append(embed)

        await ctx.send(embed=embeds[0], view=AlertMenus(embeds))

@bot.slash_command()
async def dalle3(inter: disnake.AppCmdInter):
    """Generates an image using openai's Dalle3"""
    await inter.response.send_modal(modal=DalleModal(bot))


@bot.command()
async def cfr(ctx, *, query: str):
    results = fetch_results(query)
    
    async def create_table(conn):
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS cfr_data (
                starts_on TEXT,
                ends_on TEXT,
                type TEXT,
                hierarchy_title TEXT,
                hierarchy_subtitle TEXT,
                hierarchy_chapter TEXT,
                hierarchy_subchapter TEXT,
                hierarchy_part TEXT,
                hierarchy_subpart TEXT,
                hierarchy_subject_group TEXT,
                hierarchy_section TEXT,
                hierarchy_appendix TEXT,
                hierarchy_headings_title TEXT,
                hierarchy_headings_subtitle TEXT,
                hierarchy_headings_chapter TEXT,
                hierarchy_headings_subchapter TEXT,
                hierarchy_headings_part TEXT,
                hierarchy_headings_subpart TEXT,
                hierarchy_headings_subject_group TEXT,
                hierarchy_headings_section TEXT,
                hierarchy_headings_appendix TEXT,
                headings_title TEXT,
                headings_subtitle TEXT,
                headings_chapter TEXT,
                headings_subchapter TEXT,
                headings_part TEXT,
                headings_subpart TEXT,
                headings_subject_group TEXT,
                headings_section TEXT,
                headings_appendix TEXT,
                full_text_excerpt TEXT,
                score REAL,
                structure_index INT,
                reserved BOOLEAN,
                removed BOOLEAN,
                change_types_effective_cross_reference TEXT,
                change_types_cross_reference TEXT,
                change_types_effective TEXT,
                change_types_initial TEXT
            );
        ''')

    async def insert_data(conn, results):
        insert_query = '''
            INSERT INTO cfr_data (
                starts_on, ends_on, type, hierarchy_title, hierarchy_subtitle,
                hierarchy_chapter, hierarchy_subchapter, hierarchy_part,
                hierarchy_subpart, hierarchy_subject_group, hierarchy_section,
                hierarchy_appendix, hierarchy_headings_title,
                hierarchy_headings_subtitle, hierarchy_headings_chapter,
                hierarchy_headings_subchapter, hierarchy_headings_part,
                hierarchy_headings_subpart, hierarchy_headings_subject_group,
                hierarchy_headings_section
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30, $31, $32, $33, $34, $35, $36, $37, $38);
        '''
        await conn.executemany(insert_query, results)
    
    if 'results' in results:

        df = pd.DataFrame(results['results'])
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        await ctx.send(file=disnake.File(fp=csv_buffer, filename='cfr_results.csv'))
    





    


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