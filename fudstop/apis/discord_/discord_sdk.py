import os



class DiscordSDK:
    def __init__(self):
        """
        A toolkit for working with the discord API.

        Your token is retrieved from the developer tools window. 

        >>> Network tab:

        Type a message in a channel while the dev. tools window is open with the 
        "Network" tab selected and filtered by "FETCH/XHR". 


        >>> URL:

        After typing the message - you'll see a URL pop up. Click on it - and scroll 
        down until you see the "Request Headers" section.
        
        Look for "authorization" and copy the token beside it, and paste it into an .env file as YOUR_DISCORD_HTTP_TOKEN=.
        """


        self.token = os.environ.get('YOUR_DISCORD_HTTP_TOKEN')