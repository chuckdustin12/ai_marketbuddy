import os

import disnake

from disnake.ext import commands



bot = commands.Bot(command_prefix="!", intents=disnake.Intents.all())




@bot.event
async def on_ready():
    print(f'Bot loggged in')





bot.load_extensions('fudstop/cogs')
bot.run(os.environ.get('BOT'))