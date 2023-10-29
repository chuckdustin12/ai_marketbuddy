from dataclasses import dataclass
from datetime import datetime

@dataclass
class MarketNewsData:
    date: datetime
    title: str
    source: str
    url: str
    section: str

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        insert_query = """INSERT INTO market_news (date, title, source, url, section) VALUES (%s, %s, %s, %s, %s);"""
        cursor.execute(insert_query, (self.date, self.title, self.source, self.url, self.section))
        conn.commit()
