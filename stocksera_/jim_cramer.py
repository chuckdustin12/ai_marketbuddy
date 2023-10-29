from dataclasses import dataclass
from datetime import datetime

@dataclass
class JimCramerData:
    ticker: str
    date: datetime
    segment: str
    call: str
    price: float

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        insert_query = """INSERT INTO jim_cramer (ticker, date, segment, call, price) VALUES (%s, %s, %s, %s, %s);"""
        cursor.execute(insert_query, (self.ticker, self.date, self.segment, self.call, self.price))
        conn.commit()
