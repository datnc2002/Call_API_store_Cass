import requests

url = "http://api.openweathermap.org/data/2.5/weather"
params = {
    "q": "Hanoi",
    "appid": "1af9056958b2d0d54180b6a72ed0729f"
}
res = requests.get(url, params=params)
res = res.json()

print(res)