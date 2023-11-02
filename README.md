# NOTE: ALL OF YOUR KEYS / WEBHOOK URLS / DATABASE CONFIGURATION WILL GO IN THE .env FILE AT THE ROOT. 

Add/adjust/remove as needed.




#### to get your free stocksera key:
https://stocksera.pythonanywhere.com/accounts/signup/


After signing up, click the developers tab on the top of the screen and then click "Generate"

#USAGE:

#### Clone repo


## To use the stocksera functions:

Make a .env file in the root of the folder.


add: YOUR_STOCKSERA_KEY=your key


You can now import and use the stocksera functions.


## To use the nasdaq datalink functions:

Go to:

https://data.nasdaq.com/sign-up

Make an account. After doing so - log in and go to your account settings. Request a new key.

Copy/paste the key into the .env file as YOUR_NASDAQ_KEY=key.


You can now import and use the nasdaq datalink functions.


## To use the discord SDK functions:


        A toolkit for working with the discord API.

        Your token is retrieved from the developer tools window. 

        >>> Network tab:

        Type a message in a channel while the dev. tools window is open with the 
        "Network" tab selected and filtered by "FETCH/XHR". 


        >>> URL:

        After typing the message - you'll see a URL pop up. Click on it - and scroll 
        down until you see the "Request Headers" section.
        
        Look for "authorization" and copy the token beside it, and paste it into an .env file as 
        
       
        YOUR_DISCORD_HTTP_TOKEN=your_token
      


## To use the polygon.io functions and features:


1. head over to https://www.polygon.io

2. Sign up. Polygon offers a free tier for tinkering, as well as subscriptions for real-time data APIs and websocket clusters.`

        When checking out - use code FUDSTOP at checkout to recieve a 10% discount on your order!


3. After signing up - get your API key - post it in an .env file as: YOUR_POLYGON_KEY=key



## To use the weather functions for the discord bot:

1. Head over to https://openweathermap.org/api and sign up for an account. They offer a free API that should satisfy the majority of needs.

2. Generate your API key and confirm email. It will work in about 1-2 hours after signing up.






### ENV FILE SKELETON ###

You can now use the robust polygon.io suite of tools, as well as the SDK kit for real-time market data analysis.
