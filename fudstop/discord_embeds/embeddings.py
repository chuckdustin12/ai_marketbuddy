import os

from dotenv import load_dotenv

load_dotenv()

from discord_webhook import DiscordEmbed, AsyncDiscordWebhook



from datetime import datetime, timedelta

class Embeddings:
    def __init__(self, ticker):

        self.ticker=ticker



        

        self.today = datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        self.tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
        self.thirty_days_ago = (datetime.now() - timedelta(days=30)).strftime('%Y-%m-%d')
        self.thirty_days_from_now = (datetime.now() + timedelta(days=30)).strftime('%Y-%m-%d')
        self.fifteen_days_ago = (datetime.now() - timedelta(days=15)).strftime('%Y-%m-%d')
        self.fifteen_days_from_now = (datetime.now() + timedelta(days=15)).strftime('%Y-%m-%d')
        self.eight_days_from_now = (datetime.now() + timedelta(days=8)).strftime('%Y-%m-%d')
        self.eight_days_ago = (datetime.now() - timedelta(days=8)).strftime('%Y-%m-%d')

        #conditions

        self.accumulation = os.environ.get('accumulation')
        self.fire_sale = os.environ.get('fire_sale')
        self.neutral_zone = os.environ.get('neutral_zone')



    async def send_webhook(self, webhook_url, embed=None):
        """
        Sends webhook to discord when conditions are met.

        Arguments:

        >>> webhook_url: REQUIRED - your discord webhook URL

        >>> embed: OPTIONAL - the embed to go with the feed


        
        """


        webhook = AsyncDiscordWebhook(webhook_url)


        if embed is not None:
            webhook.add_embed(embed)


        await webhook.execute()

    async def volume_analysis_embed(self, condition, webhook_url, data_dict:dict):
        """Sends conditional embeds based on volume analysis
        
        >>> fire_sale, accumulation, neutral_zone
        
        """
        print(data_dict)
        ticker = data_dict.get('ticker')

        embed = DiscordEmbed(title=f"|| {condition} || - {ticker}")

        await self.send_webhook(webhook_url, embed)

        
