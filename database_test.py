import os
import asyncio
from webull.database_manager import DatabaseManager

connection_string = os.environ.get('CONNECTION_STRING')

db = DatabaseManager(connection_string)

async def main():

    await db.connect()

    print(f"Connected to DB!")



asyncio.run(main())