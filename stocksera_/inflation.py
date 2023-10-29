from dataclasses import dataclass, field
from typing import Dict

@dataclass
class InflationData:
    years_data: Dict[int, float]  # Assuming inflation data is float; adjust as needed

    def insert_into_pg(self, conn):
        cursor = conn.cursor()
        for year, value in self.years_data.items():
            insert_query = """INSERT INTO inflation (year, value) VALUES (%s, %s);"""
            cursor.execute(insert_query, (year, value))
        conn.commit()
