from dataclasses import dataclass
from datetime import datetime

@dataclass
class RetailSalesData:
    date: datetime
    amount: float
    percent_change: float
    monthly_avg_cases: int

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        insert_query = """INSERT INTO retail_sales (date, amount, percent_change, monthly_avg_cases) VALUES (%s, %s, %s, %s);"""
        cursor.execute(insert_query, (self.date, self.amount, self.percent_change, self.monthly_avg_cases))
        conn.commit()
