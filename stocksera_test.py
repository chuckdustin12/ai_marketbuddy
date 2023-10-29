from stocksera_.stocksera_ import StockSera
from list_sets.ticker_lists import most_active_tickers

ss = StockSera()



treas = ss.daily_treasury()
for i in treas:
    print(i.amount_change)
    print(i.date)

    dict = { 
        'date': i.date,
        'amt_change': i.amount_change,


    }
    print(dict)


short_vol = ss.short_volume(['AAPL', 'SPY', 'MSFT'], date_from='2023-10-01', date_to='2023-10-27',concurrency=10)

print(short_vol)




#good for settlement window
highest_shorted = ss.highest_shorted()

print(highest_shorted)

#by attribute:

for item in highest_shorted:
    print(item.date)
    print(item.average_volume)
    print(item.short_interest)
    # .....



inflation = ss.inflation('1970')
print(inflation)


#optional as_dataframe = False - dot notation
jobless_claims = ss.jobless_claims(days='200', as_dataframe=False)
for item in jobless_claims:
    print(f"Number:", item.number)
    print(f"Date:", item.date)
    ...



retail_sales = ss.retail_sales(days='100')
print(retail_sales)


insider_trades = ss.insider_trading()
print(insider_trades)



sec_filings = ss.sec_filings(['AAPL', 'MSFT', 'GME'], concurrency=10)

print(sec_filings)


reverse_repo = ss.reverse_repo(days='100')
print(reverse_repo)


low_float = ss.low_float()
print(low_float)

#with as_dataframe set as false - dot notation
jim_cramer = ss.jim_cramer(as_dataframe=False)
for attribute in jim_cramer:
    print(f"Date: {attribute.date} | Call: {attribute.call} | Ticker: {attribute.ticker}")



market_news = ss.market_news(as_dataframe=False)

for attribute in market_news:
    print(attribute.title)



news_sentiment = ss.news_sentiment(['AMZN', 'MSFT', 'AAPL', 'CVX'], concurrency=10)

print(news_sentiment)
