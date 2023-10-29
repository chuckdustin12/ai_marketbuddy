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


#latest
print(highest_shorted[0])