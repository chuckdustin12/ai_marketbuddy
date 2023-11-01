import requests
from fudstop.apis.misc.weather import get_coordinates

import os


key = os.environ.get('YOUR_WEATHER_KEY')
key2 = os.environ.get('YOUR_GEOCODE_KEY')
print(key)
city='dallas'
state_code='tx'
country_code='us'
lat, lon = get_coordinates(city,state_code,country_code)
print(lat,lon)
cnt=3
url = f"https://api.openweathermap.org/data/2.5/forecast/daily?lat={lat}&lon={lon}&cnt={cnt}&appid={key2}"
        
r = requests.get(url).text
print(r)
