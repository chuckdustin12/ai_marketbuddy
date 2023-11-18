import os
from dotenv import load_dotenv
import pandas as pd
import asyncpg
load_dotenv()

from fudstop.examples.polygonio.get_all_options import run_all_options
import asyncio


asyncio.run(run_all_options())