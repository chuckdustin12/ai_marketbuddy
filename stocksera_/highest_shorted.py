from dataclasses import dataclass
from datetime import datetime

@dataclass
class HighestShortedData:
    rank: int
    ticker: str
    date: datetime
    short_interest: int
    average_volume: int
    days_to_cover: float
    percent_float_short: float

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        insert_query = """INSERT INTO highest_shorted (rank, ticker, date, short_interest, average_volume, days_to_cover, percent_float_short) VALUES (%s, %s, %s, %s, %s, %s, %s);"""
        cursor.execute(insert_query, (self.rank, self.ticker, self.date, self.short_interest, self.average_volume, self.days_to_cover, self.percent_float_short))
        conn.commit()
