from dataclasses import dataclass
from datetime import datetime

@dataclass
class SECFilingsData:
    ticker: str
    filling: str
    description: str
    filling_date: datetime
    report_url: str
    filing_url: str

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        insert_query = """INSERT INTO sec_filings (ticker, filling, description, filling_date, report_url, filing_url) VALUES (%s, %s, %s, %s, %s, %s);"""
        cursor.execute(insert_query, (self.ticker, self.filling, self.description, self.filling_date, self.report_url, self.filing_url))
        conn.commit()
