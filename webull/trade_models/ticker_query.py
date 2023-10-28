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
        self.r=r

        self.web_name = self.r.get("name", None)
        self.web_symb = self.r.get("symbol", None)
        self.web_exchange = self.r.get("disExchangeCode", None)
        self.web_stock_close =self.r.get("close", None)
        self.last_earnings = self.r.get('latestEarningsDate',None)
        self.web_stock_vol =self.r.get("volume",None)
        self.web_change_ratio = self.r.get("changeRatio", None)
        self.web_stock_open =self.r.get("open",None)
        self.web_stock_high =self.r.get("high", None)
        self.web_stock_low =self.r.get("low", None)
        self.fifty_high = self.r.get("fiftyTwoWkHigh", None)
        self.avg_vol3m = self.r.get('avgVol3M')
        self.fifty_low = self.r.get("fiftyTwoWkLow", None)
        self.avg_10d_vol = self.r.get("avgVol10D", None)
        self.outstanding_shares = self.r.get('outstandingShares', None)
        self.total_shares = self.r.get('totalShares', None)

        try:
            self.estimated_earnings = self.r.get("nextEarningDay", None)
            self.web_vibrate_ratio = self.r.get('vibrateRatio', None)
        except KeyError:
            self.estimated_earnings = None
            self.web_vibrate_ratio = None


        self.data_dict = {
            'Company Name': self.r.get("name", None),
            'Symbol': self.r.get("symbol", None),
            'Exchange': self.r.get("disExchangeCode", None),
            'Close Price': self.r.get("close", None),
            'Latest Earnings': self.r.get('latestEarningsDate', None),
            'Volume': self.r.get("volume", None),
            'Change Ratio': self.r.get("changeRatio", None),
            'Open Price': self.r.get("open", None),
            'High Price': self.r.get("high", None),
            'Low Price': self.r.get("low", None),
            '52week High': self.r.get("fiftyTwoWkHigh", None),
            'Avg 3month Volume': self.r.get('avgVol3M', None),
            '52week Low': self.r.get("fiftyTwoWkLow", None),
            'Avg 10day Volume': self.r.get("avgVol10D", None),
            'Outstanding Shares': self.r.get('outstandingShares', None),
            'Total Shares': self.r.get('totalShares', None)
        }

        self.df = pd.DataFrame(self.data_dict, index=[0]).transpose()




        