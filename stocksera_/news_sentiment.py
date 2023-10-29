from dataclasses import dataclass
from datetime import datetime

@dataclass
class NewsSentimentData:
    ticker: str
    date: datetime
    title: str
    link: str
    sentiment: str

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        insert_query = """INSERT INTO news_sentiment (ticker, date, title, link, sentiment) VALUES (%s, %s, %s, %s, %s);"""
        cursor.execute(insert_query, (self.ticker, self.date, self.title, self.link, self.sentiment))
        conn.commit()
