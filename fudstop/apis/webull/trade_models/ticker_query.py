import pandas as pd
class WebullStockData:
    """A class representing stock data obtained from Webull.

    Attributes:
        web_name (str): The name of the stock.
        web_symb (str): The stock's symbol.
        web_exchange (str): The exchange code where the stock is traded.
        web_stock_close (float): The stock's closing price.
        last_earnings (str): The date of the stock's latest earnings report.
        web_stock_vol (int): The stock's trading volume.
        web_change_ratio (float): The stock's price change ratio.
        web_stock_open (float): The stock's opening price.
        web_stock_high (float): The stock's highest price.
        web_stock_low (float): The stock's lowest price.
        fifty_high (float): The stock's 52-week high price.
        avg_vol3m (float): The stock's average trading volume over the past 3 months.
        fifty_low (float): The stock's 52-week low price.
        avg_10d_vol (float): The stock's average trading volume over the past 10 days.
        outstanding_shares (int): The number of outstanding shares of the stock.
        total_shares (int): The total number of shares of the stock.
        estimated_earnings (str): The estimated date of the stock's next earnings report.
        web_vibrate_ratio (float): The stock's price fluctuation ratio.
    """
    def __init__(self, r):
        self.r = r
        for i in self.r:
            self.web_name = i.get("name", None)
            self.web_symb = i.get("symbol", None)
            self.web_exchange = i.get("disExchangeCode", None)
            self.web_stock_close =i.get("close", None)
            self.last_earnings = i.get('latestEarningsDate',None)
            self.web_stock_vol =i.get("volume",None)
            self.web_change_ratio = i.get("changeRatio", None)
            self.web_stock_open =i.get("open",None)
            self.web_stock_high =i.get("high", None)
            self.web_stock_low =i.get("low", None)
            self.fifty_high = i.get("fiftyTwoWkHigh", None)
            self.avg_vol3m = i.get('avgVol3M')
            self.fifty_low = i.get("fiftyTwoWkLow", None)
            self.avg_10d_vol = i.get("avgVol10D", None)
            self.outstanding_shares = i.get('outstandingShares', None)
            self.total_shares = i.get('totalShares', None)

            try:
                self.estimated_earnings = i.get("nextEarningDay", None)
                self.web_vibrate_ratio = i.get('vibrateRatio', None)
            except KeyError:
                self.estimated_earnings = None
                self.web_vibrate_ratio = None


            #self.data_dict = {
            #     'Company Name': i.get("name", None),
            #     'Symbol': i.get("symbol", None),
            #     'Exchange': i.get("disExchangeCode", None),
            #     'Close Price': i.get("close", None),
            #     'Latest Earnings': i.get('latestEarningsDate', None),
            #     'Volume': i.get("volume", None),
            #     'Change Ratio': i.get("changeRatio", None),
            #     'Open Price': i.get("open", None),
            #     'High Price': i.get("high", None),
            #     'Low Price': i.get("low", None),
            #     '52week High': i.get("fiftyTwoWkHigh", None),
            #     'Avg 3month Volume': i.get('avgVol3M', None),
            #     '52week Low': i.get("fiftyTwoWkLow", None),
            #     'Avg 10day Volume': self.r.get("avgVol10D", None),
            #     'Outstanding Shares': self.r.get('outstandingShares', None),
            #     'Total Shares': self.r.get('totalShares', None)
            # }

            # self.df = pd.DataFrame(self.data_dict, index=[0]).transpose()




        