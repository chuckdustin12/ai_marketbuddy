from dotenv import load_dotenv
load_dotenv()

import requests

import os

API_KEY = os.environ.get('YOUR_WEATHER_KEY')



def get_coordinates(city_name, state, country_code, limit:str='2'):
    url = f"http://api.openweathermap.org/geo/1.0/direct?q={city_name},{state},{country_code}&limit={limit}&appid={API_KEY}"
    response = requests.get(url)
    data = response.json()
    if data:
        return data[0]['lat'], data[0]['lon']
    else:
        return None, None
    



