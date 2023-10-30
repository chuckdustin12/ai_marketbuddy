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
      