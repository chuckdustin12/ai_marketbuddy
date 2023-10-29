from dataclasses import dataclass
from datetime import datetime

@dataclass
class ReverseRepoData:
    date: datetime
    amount: float
    num_parties: int
    average: float
    moving_avg: float

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        insert_query = """INSERT INTO reverse_repo (date, amount, num_parties, average, moving_avg) VALUES (%s, %s, %s, %s, %s);"""
        cursor.execute(insert_query, (self.date, self.amount, self.num_parties, self.average, self.moving_avg))
        conn.commit()
