import requests



r = requests.get("https://fudstop.io/api/top-options-total-volume").json()
print(r)