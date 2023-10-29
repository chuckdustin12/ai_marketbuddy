from dataclasses import dataclass
from datetime import datetime

@dataclass
class JoblessClaimsData:
    date: datetime
    number: int
    percent_change: float

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        insert_query = """INSERT INTO jobless_claims (date, number, percent_change) VALUES (%s, %s, %s);"""
        cursor.execute(insert_query, (self.date, self.number, self.percent_change))
        conn.commit()
