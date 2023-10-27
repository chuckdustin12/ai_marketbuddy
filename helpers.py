from datetime import datetime, timedelta

# Function to convert Unix timestamps in seconds to Eastern Time in milliseconds
def convert_seconds_to_ms_eastern_time(seconds_timestamp):
    et_offset = -5 * 3600  # Eastern Standard Time (EST) offset in seconds
    utc_time = datetime.utcfromtimestamp(int(seconds_timestamp))
    eastern_time = utc_time + timedelta(seconds=et_offset)
    eastern_time_ms = int(eastern_time.timestamp() * 1000)  # Convert to milliseconds
    return eastern_time_ms


def convert_unix_to_eastern(unix_timestamp):
    eastern_time = datetime.fromtimestamp(unix_timestamp).strftime('%Y-%m-%d %H:%M:%S')
    return eastern_time