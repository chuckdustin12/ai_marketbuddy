from dataclasses import dataclass
from datetime import datetime

@dataclass
class FailureToDeliverData:
    date: datetime
    ticker: str
    ftd: int
    price: float
    ftd_x_dollar: float
    t_35_date: datetime

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        insert_query = """INSERT INTO failure_to_deliver (date, ticker, ftd, price, ftd_x_dollar, t_35_date) VALUES (%s, %s, %s, %s, %s, %s);"""
        cursor.execute(insert_query, (self.date, self.ticker, self.ftd, self.price, self.ftd_x_dollar, self.t_35_date))
        conn.commit()
