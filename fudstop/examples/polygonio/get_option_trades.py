
import sys
from pathlib import Path

# Add the project directory to the sys.path
project_dir = str(Path(__file__).resolve().parents[2])
if project_dir not in sys.path:
    sys.path.append(project_dir)



from apis.helpers import get_human_readable_string, map_conditions




import asyncio
import pandas as pd
from apis.polygonio.polygon_options import PolygonOptions


opts = PolygonOptions()


ticker="O:ENPH231215C00075000"
async def get_trades(ticker):
    trades = await opts.option_trades(ticker)

    print(trades)
        
    # Assuming trades is a DataFrame and sip_timestamp is a column with Unix timestamps
    # First, convert the Unix timestamps to datetime objects, normalize to UTC, and then convert to US/Eastern time
    # Convert the Unix timestamps to datetime objects (without timezone)
    trades['sip_timestamp'] = pd.to_datetime(trades['sip_timestamp'], unit='ns')

    # Apply timezone localization and conversion for each timestamp individually
    trades['sip_timestamp'] = trades['sip_timestamp'].apply(lambda x: x.tz_localize('UTC').tz_convert('US/Eastern'))

    # Format these datetime objects to a string format
    trades['sip_timestamp'] = trades['sip_timestamp'].dt.strftime('%Y-%m-%d %H:%M:%S')
asyncio.run(get_trades(ticker))
    