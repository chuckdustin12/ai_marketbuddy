from stocksera_.stocksera_ import StockSera
from list_sets.ticker_lists import most_active_tickers

ss = StockSera()



x = ss.borrowed_shares(ticker_or_tickers=most_active_tickers, concurrency=30)

print(x)